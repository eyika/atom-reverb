<?php

namespace Eyika\Atom\Reverb;

use Eyika\Atom\Reverb\Protocol\Frame;
use Eyika\Atom\Reverb\Protocol\Handshake;
use RuntimeException;
use Throwable;

/**
 * A minimal, dependency-free WebSocket broadcast server (Laravel-Reverb-style) built on
 * PHP's own stream_socket_server + stream_select — no ratchet/swoole/react. It listens on
 * two ports:
 *
 *   - the WS port: browsers/clients connect, subscribe to channels, receive broadcasts.
 *   - the ingest port: the application (PHP-FPM) POSTs events here; the server fans each
 *     one out to that channel's subscribers as WebSocket text frames.
 *
 * This split is what lets a short-lived FPM request "broadcast" to long-lived socket
 * clients without a shared Redis — the app just makes a localhost HTTP call (see
 * Broadcasting\BroadcastManager).
 *
 * The client protocol is a simplified Pusher-style JSON envelope:
 *   client → server : {"event":"subscribe",  "data":{"channel":"orders"}}
 *                     {"event":"unsubscribe","data":{"channel":"orders"}}
 *   server → client : {"event":"OrderShipped","channel":"orders","data":{...}}
 */
class Server
{
    protected ChannelManager $channels;
    /** @var callable|null */
    protected $onLog;

    /** @var array<int, array{sock: resource, buffer: string, handshook: bool}> keyed by (int)$sock */
    protected array $conns = [];

    public function __construct(?ChannelManager $channels = null, ?callable $onLog = null)
    {
        $this->channels = $channels ?? new ChannelManager();
        $this->onLog = $onLog;
    }

    public function channels(): ChannelManager
    {
        return $this->channels;
    }

    /** Boot both listeners and run the select loop forever. Blocks. */
    public function start(string $host = '127.0.0.1', int $wsPort = 8091, int $ingestPort = 8092): void
    {
        $ws = @stream_socket_server("tcp://{$host}:{$wsPort}", $e1, $s1);
        if ($ws === false) {
            throw new RuntimeException("Cannot bind WS {$host}:{$wsPort} — {$s1} ({$e1})");
        }
        $ingest = @stream_socket_server("tcp://{$host}:{$ingestPort}", $e2, $s2);
        if ($ingest === false) {
            throw new RuntimeException("Cannot bind ingest {$host}:{$ingestPort} — {$s2} ({$e2})");
        }

        $this->log("Atom Reverb: WS on ws://{$host}:{$wsPort}, ingest on http://{$host}:{$ingestPort}");

        while (true) {
            $read = [$ws, $ingest];
            foreach ($this->conns as $c) {
                $read[] = $c['sock'];
            }
            $write = $except = null;

            if (@stream_select($read, $write, $except, null) === false) {
                continue;
            }

            foreach ($read as $sock) {
                if ($sock === $ws) {
                    $this->acceptClient($ws);
                } elseif ($sock === $ingest) {
                    $this->acceptIngest($ingest);
                } else {
                    $this->onClientData($sock);
                }
            }
        }
    }

    protected function acceptClient($ws): void
    {
        $sock = @stream_socket_accept($ws, 0);
        if ($sock === false) {
            return;
        }
        stream_set_blocking($sock, false);
        $this->conns[(int) $sock] = ['sock' => $sock, 'buffer' => '', 'handshook' => false];
    }

    protected function onClientData($sock): void
    {
        $id = (int) $sock;
        $chunk = @fread($sock, 65535);

        if ($chunk === '' || $chunk === false) {
            $this->disconnect($id);
            return;
        }

        $this->conns[$id]['buffer'] .= $chunk;

        if (!$this->conns[$id]['handshook']) {
            if (strpos($this->conns[$id]['buffer'], "\r\n\r\n") === false) {
                return; // wait for the full upgrade request
            }
            $request = $this->conns[$id]['buffer'];
            $this->conns[$id]['buffer'] = '';

            $key = Handshake::keyFrom($request);
            if (!Handshake::isUpgrade($request) || $key === null) {
                $this->disconnect($id);
                return;
            }
            @fwrite($sock, Handshake::response($key));
            $this->conns[$id]['handshook'] = true;
            return;
        }

        // Drain as many complete frames as the buffer holds.
        while (($frame = Frame::decode($this->conns[$id]['buffer'])) !== null) {
            $this->conns[$id]['buffer'] = substr($this->conns[$id]['buffer'], $frame['consumed']);

            if ($frame['opcode'] === Frame::OP_CLOSE) {
                $this->disconnect($id);
                return;
            }
            if ($frame['opcode'] === Frame::OP_PING) {
                @fwrite($sock, Frame::encode($frame['payload'], Frame::OP_PONG));
                continue;
            }
            if ($frame['opcode'] === Frame::OP_TEXT) {
                $this->handleClientText($id, $frame['payload']);
            }
        }
    }

    /**
     * Handle one decoded client text message (subscribe/unsubscribe). Pure w.r.t. sockets
     * — it only mutates the ChannelManager — so it's directly unit-testable.
     */
    public function handleClientText(int $connectionId, string $json): void
    {
        $msg = json_decode($json, true);
        if (!is_array($msg)) {
            return;
        }

        $event = $msg['event'] ?? '';
        $channel = $msg['data']['channel'] ?? ($msg['channel'] ?? null);
        if (!is_string($channel) || $channel === '') {
            return;
        }

        if ($event === 'subscribe') {
            $this->channels->subscribe($connectionId, $channel);
        } elseif ($event === 'unsubscribe') {
            $this->channels->unsubscribe($connectionId, $channel);
        }
    }

    /** Accept + service one ingest (broadcast) HTTP POST, then close it. */
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
        // read any Content-Length body that trails the headers
        if (preg_match('/Content-Length:\s*(\d+)/i', $raw, $m)) {
            $need = (int) $m[1];
            $bodySoFar = strlen(substr($raw, strpos($raw, "\r\n\r\n") + 4));
            while ($bodySoFar < $need) {
                $chunk = @fread($sock, 8192);
                if ($chunk === '' || $chunk === false) {
                    break;
                }
                $raw .= $chunk;
                $bodySoFar += strlen($chunk);
            }
        }

        $sent = 0;
        $payload = self::parseIngest($raw);
        if ($payload !== null) {
            $sent = $this->broadcast($payload['channel'], $payload['event'], $payload['data']);
        }

        $body = json_encode(['ok' => $payload !== null, 'delivered' => $sent]);
        @fwrite($sock, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
            . strlen($body) . "\r\nConnection: close\r\n\r\n" . $body);
        @fclose($sock);
    }

    /**
     * Parse a broadcast ingest request body into ['channel','event','data']. Pure +
     * testable. Accepts {"channel":..,"event":..,"data":{..}}.
     */
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
        ];
    }

    /** Fan an event out to every subscriber of a channel. Returns the delivery count. */
    public function broadcast(string $channel, string $event, mixed $data): int
    {
        $message = Frame::encode(json_encode([
            'event'   => $event,
            'channel' => $channel,
            'data'    => $data,
        ]));

        $delivered = 0;
        foreach ($this->channels->subscribers($channel) as $id) {
            if (isset($this->conns[$id])) {
                if (@fwrite($this->conns[$id]['sock'], $message) !== false) {
                    $delivered++;
                }
            }
        }
        return $delivered;
    }

    protected function disconnect(int $id): void
    {
        if (!isset($this->conns[$id])) {
            return;
        }
        $this->channels->forget($id);
        try {
            @fclose($this->conns[$id]['sock']);
        } catch (Throwable $e) {
            // already closed
        }
        unset($this->conns[$id]);
    }

    protected function log(string $message): void
    {
        if ($this->onLog !== null) {
            ($this->onLog)($message);
        }
    }
}
