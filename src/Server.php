<?php

namespace Eyika\Atom\Reverb;

use Eyika\Atom\Reverb\Auth\Signature;
use Eyika\Atom\Reverb\Backplane\Backplane;
use Eyika\Atom\Reverb\Backplane\LocalBackplane;
use Eyika\Atom\Reverb\Protocol\Frame;
use Eyika\Atom\Reverb\Protocol\Handshake;
use RuntimeException;
use Throwable;

/**
 * Production WebSocket broadcast server (Pusher-protocol compatible) on stream_socket_server
 * + stream_select. Hardened for real deployments:
 *
 *   - non-blocking writes with per-connection back-pressure (write buffers drained on
 *     write-readiness), so a slow client can't block the loop;
 *   - ping/pong heartbeat + idle timeout to reap dead connections;
 *   - fragmented-message reassembly and control-frame handling;
 *   - private-/presence- channel authorisation (HMAC) + presence membership + member events;
 *   - authenticated broadcast ingest (HMAC-signed);
 *   - a pluggable backplane (Redis) so many nodes fan out behind a load balancer;
 *   - opt-in native TLS (wss://) — a reverse proxy remains the recommended default.
 */
class Server
{
    protected ChannelManager $channels;
    protected Backplane $backplane;
    /** @var array<string,mixed> */
    protected array $options;
    /** @var callable|null */
    protected $onLog;

    /** @var array<int, Connection> keyed by (int) socket */
    protected array $conns = [];
    protected float $lastHeartbeat = 0.0;
    protected int $socketSeq = 0;

    public function __construct(
        ?ChannelManager $channels = null,
        array $options = [],
        ?Backplane $backplane = null,
        ?callable $onLog = null
    ) {
        $this->channels = $channels ?? new ChannelManager();
        $this->backplane = $backplane ?? new LocalBackplane();
        $this->onLog = $onLog;
        $this->options = $options + [
            'host'               => '127.0.0.1',
            'ws_port'            => 8091,
            'ingest_port'        => 8092,
            'app_key'            => 'atom',
            'app_secret'         => '',            // '' disables auth (dev only)
            'max_connections'    => 10000,
            'heartbeat_interval' => 30,            // seconds between server→client pings
            'idle_timeout'       => 120,           // close a connection idle this long
            'activity_timeout'   => 120,           // advertised to clients
            'tls'                => ['enabled' => false],
        ];
    }

    public function channels(): ChannelManager
    {
        return $this->channels;
    }

    public function start(): void
    {
        $host = $this->options['host'];
        $ws = $this->makeServerSocket((int) $this->options['ws_port'], $this->options['tls']);
        $ingest = $this->makeServerSocket((int) $this->options['ingest_port'], ['enabled' => false]);

        $scheme = ($this->options['tls']['enabled'] ?? false) ? 'wss' : 'ws';
        $this->log(sprintf(
            'Atom Reverb: %s on %s://%s:%d, ingest on http://%s:%d',
            get_class($this->backplane) === LocalBackplane::class ? 'single-node' : 'clustered',
            $scheme,
            $host,
            $this->options['ws_port'],
            $host,
            $this->options['ingest_port']
        ));

        while (true) {
            $read = [$ws, $ingest];
            foreach ($this->conns as $c) {
                $read[] = $c->socket;
            }
            foreach ($this->backplane->readStreams() as $s) {
                $read[] = $s;
            }

            $write = [];
            foreach ($this->conns as $c) {
                if ($c->hasPendingWrites()) {
                    $write[] = $c->socket;
                }
            }

            $except = null;
            if (@stream_select($read, $write, $except, 1) === false) {
                continue;
            }

            foreach ($read as $sock) {
                if ($sock === $ws) {
                    $this->acceptClient($ws);
                } elseif ($sock === $ingest) {
                    $this->acceptIngest($ingest);
                } elseif (isset($this->conns[(int) $sock])) {
                    $this->onClientData($this->conns[(int) $sock]);
                }
            }

            foreach ($write as $sock) {
                if (isset($this->conns[(int) $sock]) && !$this->conns[(int) $sock]->flush()) {
                    // still pending or errored; a hard error is caught on the next read
                }
            }

            $this->backplane->poll(function (array $message) {
                $this->fanOut(
                    (string) ($message['channel'] ?? ''),
                    ['event' => $message['event'] ?? '', 'channel' => $message['channel'] ?? '', 'data' => $message['data'] ?? []]
                );
            });

            $this->tick();
        }
    }

    // --- Connection lifecycle ----------------------------------------------------------

    protected function acceptClient($ws): void
    {
        $sock = @stream_socket_accept($ws, 0);
        if ($sock === false) {
            return;
        }
        if (count($this->conns) >= (int) $this->options['max_connections']) {
            @fclose($sock);
            return;
        }
        stream_set_blocking($sock, false);
        $id = (int) $sock;
        $this->conns[$id] = new Connection($id, $sock, $this->now());
    }

    protected function onClientData(Connection $conn): void
    {
        $chunk = @fread($conn->socket, 65535);
        if ($chunk === '' || $chunk === false) {
            $this->disconnect($conn);
            return;
        }
        $conn->readBuffer .= $chunk;
        $conn->touch($this->now());

        if (!$conn->handshook) {
            if (strpos($conn->readBuffer, "\r\n\r\n") === false) {
                return;
            }
            $request = $conn->readBuffer;
            $conn->readBuffer = '';
            $this->doHandshake($conn, $request);
            return;
        }

        while (($frame = Frame::decode($conn->readBuffer)) !== null) {
            $conn->readBuffer = substr($conn->readBuffer, $frame['consumed']);
            $this->handleFrame($conn, $frame);
        }
    }

    protected function doHandshake(Connection $conn, string $request): void
    {
        $key = Handshake::keyFrom($request);
        if (!Handshake::isUpgrade($request) || $key === null) {
            $this->disconnect($conn);
            return;
        }

        @fwrite($conn->socket, Handshake::response($key));
        $conn->handshook = true;
        $conn->socketId = $this->newSocketId();

        $this->send($conn, [
            'event' => 'pusher:connection_established',
            'data'  => json_encode([
                'socket_id'        => $conn->socketId,
                'activity_timeout' => (int) $this->options['activity_timeout'],
            ]),
        ]);
    }

    // --- Frame handling (with reassembly) ----------------------------------------------

    protected function handleFrame(Connection $conn, array $frame): void
    {
        $opcode = $frame['opcode'];

        if ($opcode === Frame::OP_CLOSE) {
            $conn->queue(Frame::encode('', Frame::OP_CLOSE));
            $conn->flush();
            $this->disconnect($conn);
            return;
        }
        if ($opcode === Frame::OP_PING) {
            $conn->queue(Frame::encode($frame['payload'], Frame::OP_PONG));
            return;
        }
        if ($opcode === Frame::OP_PONG) {
            $conn->awaitingPong = false;
            return;
        }

        // Reassemble fragmented data messages.
        if ($opcode === Frame::OP_CONTINUATION) {
            $conn->fragment .= $frame['payload'];
        } elseif ($opcode === Frame::OP_TEXT || $opcode === Frame::OP_BINARY) {
            $conn->fragment = $frame['payload'];
            $conn->fragmentOpcode = $opcode;
        }

        if (!$frame['fin']) {
            return; // wait for the rest
        }

        $message = $conn->fragment;
        $conn->fragment = '';
        $conn->fragmentOpcode = null;

        $this->handleClientMessage($conn, $message);
    }

    /** Handle one complete client text message (a Pusher-style JSON event). */
    public function handleClientMessage(Connection $conn, string $json): void
    {
        $msg = json_decode($json, true);
        if (!is_array($msg)) {
            return;
        }
        $event = $msg['event'] ?? '';
        $data = $msg['data'] ?? [];
        if (is_string($data)) {
            $decoded = json_decode($data, true);
            $data = is_array($decoded) ? $decoded : [];
        }

        switch ($event) {
            case 'pusher:ping':
                $this->send($conn, ['event' => 'pusher:pong', 'data' => (object) []]);
                return;
            case 'pusher:subscribe':
            case 'subscribe':
                $this->subscribe(
                    $conn,
                    (string) ($data['channel'] ?? ''),
                    (string) ($data['auth'] ?? ''),
                    isset($data['channel_data']) ? (string) $data['channel_data'] : null
                );
                return;
            case 'pusher:unsubscribe':
            case 'unsubscribe':
                $this->unsubscribe($conn, (string) ($data['channel'] ?? ''));
                return;
        }
    }

    // --- Subscriptions + presence ------------------------------------------------------

    protected function subscribe(Connection $conn, string $channel, string $auth, ?string $channelData): void
    {
        if ($channel === '') {
            return;
        }

        // Authorise private/presence channels.
        if (Signature::isPrivate($channel) && ($secret = (string) $this->options['app_secret']) !== '') {
            $ok = Signature::verifyChannel(
                (string) $this->options['app_key'],
                $secret,
                $conn->socketId,
                $channel,
                $auth,
                Signature::isPresence($channel) ? $channelData : null
            );
            if (!$ok) {
                $this->send($conn, [
                    'event'   => 'pusher:error',
                    'channel' => $channel,
                    'data'    => ['message' => 'Subscription auth failed for ' . $channel, 'code' => 4009],
                ]);
                return;
            }
        }

        $member = null;
        if (Signature::isPresence($channel)) {
            $parsed = json_decode((string) $channelData, true);
            $member = is_array($parsed) ? $parsed : ['user_id' => $conn->socketId];
        }

        $this->channels->subscribe($conn->id, $channel, $member);
        $conn->channels[$channel] = $member;

        if (Signature::isPresence($channel)) {
            $this->sendPresenceSucceeded($conn, $channel);
            $this->fanOut($channel, [
                'event'   => 'pusher_internal:member_added',
                'channel' => $channel,
                'data'    => $member,
            ], $conn->id);
        } else {
            $this->send($conn, [
                'event'   => 'pusher_internal:subscription_succeeded',
                'channel' => $channel,
                'data'    => (object) [],
            ]);
        }
    }

    protected function unsubscribe(Connection $conn, string $channel): void
    {
        if (!array_key_exists($channel, $conn->channels)) {
            return;
        }
        $member = $conn->channels[$channel];
        unset($conn->channels[$channel]);
        $this->channels->unsubscribe($conn->id, $channel);

        if (Signature::isPresence($channel) && is_array($member)) {
            $this->fanOut($channel, [
                'event'   => 'pusher_internal:member_removed',
                'channel' => $channel,
                'data'    => ['user_id' => $member['user_id'] ?? null],
            ]);
        }
    }

    protected function sendPresenceSucceeded(Connection $conn, string $channel): void
    {
        $members = $this->channels->members($channel);
        $ids = [];
        $hash = [];
        foreach ($members as $m) {
            $uid = $m['user_id'] ?? null;
            if ($uid === null) {
                continue;
            }
            $ids[] = $uid;
            $hash[$uid] = $m['user_info'] ?? [];
        }

        $this->send($conn, [
            'event'   => 'pusher_internal:subscription_succeeded',
            'channel' => $channel,
            'data'    => json_encode(['presence' => ['ids' => $ids, 'hash' => $hash, 'count' => count($ids)]]),
        ]);
    }

    // --- Broadcasting ------------------------------------------------------------------

    /** Broadcast to a channel: fan out to local subscribers + publish to peer nodes. */
    public function broadcast(string $channel, string $event, mixed $data, bool $fromPeer = false): int
    {
        $delivered = $this->fanOut($channel, ['event' => $event, 'channel' => $channel, 'data' => $data]);

        if (!$fromPeer) {
            $this->backplane->publish(['channel' => $channel, 'event' => $event, 'data' => $data]);
        }

        return $delivered;
    }

    /** Send a message to every local subscriber of a channel (optionally excluding one). */
    protected function fanOut(string $channel, array $message, ?int $exceptId = null): int
    {
        $frame = Frame::encode(json_encode($message));
        $delivered = 0;

        foreach ($this->channels->subscribers($channel) as $id) {
            if ($id === $exceptId || !isset($this->conns[$id])) {
                continue;
            }
            $conn = $this->conns[$id];
            $conn->queue($frame);
            $conn->flush(); // best-effort now; leftover stays buffered for the write loop
            $delivered++;
        }

        return $delivered;
    }

    protected function send(Connection $conn, array $message): void
    {
        $conn->queue(Frame::encode(json_encode($message)));
        $conn->flush();
    }

    // --- Broadcast ingest (app → server) -----------------------------------------------

    protected function acceptIngest($ingest): void
    {
        $sock = @stream_socket_accept($ingest, 0);
        if ($sock === false) {
            return;
        }
        stream_set_timeout($sock, 2);

        $raw = '';
        while (strpos($raw, "\r\n\r\n") === false) {
            $chunk = @fread($sock, 8192);
            if ($chunk === '' || $chunk === false) {
                break;
            }
            $raw .= $chunk;
        }
        if (preg_match('/Content-Length:\s*(\d+)/i', $raw, $m)) {
            $need = (int) $m[1];
            $bodyLen = strlen((string) substr($raw, (int) strpos($raw, "\r\n\r\n") + 4));
            while ($bodyLen < $need) {
                $chunk = @fread($sock, 8192);
                if ($chunk === '' || $chunk === false) {
                    break;
                }
                $raw .= $chunk;
                $bodyLen += strlen($chunk);
            }
        }

        $ok = false;
        $delivered = 0;
        $payload = self::parseIngest($raw);
        if ($payload !== null && $this->ingestAuthorised($raw, $payload['body'])) {
            $ok = true;
            $delivered = $this->broadcast($payload['channel'], $payload['event'], $payload['data']);
        }

        $status = $payload === null ? '400 Bad Request' : ($ok ? '200 OK' : '401 Unauthorized');
        $body = json_encode(['ok' => $ok, 'delivered' => $delivered]);
        @fwrite($sock, "HTTP/1.1 {$status}\r\nContent-Type: application/json\r\nContent-Length: "
            . strlen($body) . "\r\nConnection: close\r\n\r\n" . $body);
        @fclose($sock);
    }

    protected function ingestAuthorised(string $rawRequest, string $body): bool
    {
        $secret = (string) $this->options['app_secret'];
        if ($secret === '') {
            return true; // auth disabled (dev)
        }
        if (!preg_match('/X-Reverb-Signature:\s*(\S+)/i', $rawRequest, $m)) {
            return false;
        }
        return Signature::verifyIngest($secret, $body, trim($m[1]));
    }

    /** Parse a broadcast ingest request into ['channel','event','data','body']. Pure. */
    public static function parseIngest(string $rawHttp): ?array
    {
        $pos = strpos($rawHttp, "\r\n\r\n");
        $body = $pos === false ? $rawHttp : substr($rawHttp, $pos + 4);
        $json = json_decode(trim($body), true);
        if (!is_array($json) || !isset($json['channel'], $json['event'])) {
            return null;
        }
        return [
            'channel' => (string) $json['channel'],
            'event'   => (string) $json['event'],
            'data'    => $json['data'] ?? [],
            'body'    => trim($body),
        ];
    }

    // --- Heartbeat + idle sweep --------------------------------------------------------

    protected function tick(): void
    {
        $now = $this->now();
        if ($now - $this->lastHeartbeat < (int) $this->options['heartbeat_interval']) {
            return;
        }
        $this->lastHeartbeat = $now;

        $idle = (int) $this->options['idle_timeout'];
        foreach ($this->conns as $conn) {
            if (!$conn->handshook) {
                continue;
            }
            $silent = $now - $conn->lastActivity;
            if ($silent > $idle) {
                $this->disconnect($conn); // dead — no pong within the window
                continue;
            }
            if ($silent > (int) $this->options['heartbeat_interval']) {
                $conn->awaitingPong = true;
                $conn->queue(Frame::encode('', Frame::OP_PING));
                $conn->flush();
            }
        }
    }

    protected function disconnect(Connection $conn): void
    {
        // Fire member_removed for any presence channels before dropping the connection.
        foreach ($conn->channels as $channel => $member) {
            if (Signature::isPresence($channel) && is_array($member)) {
                $this->channels->unsubscribe($conn->id, $channel);
                $this->fanOut($channel, [
                    'event'   => 'pusher_internal:member_removed',
                    'channel' => $channel,
                    'data'    => ['user_id' => $member['user_id'] ?? null],
                ]);
            }
        }
        $this->channels->forget($conn->id);
        @fclose($conn->socket);
        unset($this->conns[$conn->id]);
    }

    // --- Socket + helpers --------------------------------------------------------------

    /** @return resource */
    protected function makeServerSocket(int $port, array $tls)
    {
        $host = $this->options['host'];
        $scheme = 'tcp';
        $ctx = stream_context_create(['socket' => ['so_reuseport' => true, 'backlog' => 511]]);

        if ($tls['enabled'] ?? false) {
            $scheme = 'ssl';
            stream_context_set_option($ctx, 'ssl', 'local_cert', $tls['cert'] ?? '');
            stream_context_set_option($ctx, 'ssl', 'local_pk', $tls['key'] ?? '');
            stream_context_set_option($ctx, 'ssl', 'allow_self_signed', (bool) ($tls['allow_self_signed'] ?? false));
            stream_context_set_option($ctx, 'ssl', 'verify_peer', false);
        }

        $socket = @stream_socket_server(
            "{$scheme}://{$host}:{$port}",
            $errno,
            $errstr,
            STREAM_SERVER_BIND | STREAM_SERVER_LISTEN,
            $ctx
        );
        if ($socket === false) {
            throw new RuntimeException("Cannot bind {$scheme}://{$host}:{$port} — {$errstr} ({$errno})");
        }
        return $socket;
    }

    protected function newSocketId(): string
    {
        // Pusher socket-id shape: "<int>.<int>".
        $this->socketSeq++;
        try {
            return $this->socketSeq . '.' . random_int(100000, 999999999);
        } catch (Throwable $e) {
            return $this->socketSeq . '.' . crc32(uniqid('', true));
        }
    }

    protected function now(): float
    {
        return (float) hrtime(true) / 1e9;
    }

    protected function log(string $message): void
    {
        if ($this->onLog !== null) {
            ($this->onLog)($message);
        }
    }
}
