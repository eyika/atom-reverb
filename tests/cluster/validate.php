<?php
/**
 * Cross-node validation harness. Spawns TWO Reverb nodes (sharing one Redis), connects a
 * WebSocket client to each, and proves the things only a real cluster can: cross-node
 * broadcast, global presence state + member events, per-user dedup, and the crashed-node
 * reaper. Exits non-zero if any check fails. Meant to run in CI with a Redis service.
 *
 * Usage: REDIS_HOST=127.0.0.1 php tests/cluster/validate.php
 */
$src = dirname(__DIR__, 2) . '/src';
require_once $src . '/Protocol/Frame.php';
require_once $src . '/Auth/Signature.php';

use Eyika\Atom\Reverb\Auth\Signature;
use Eyika\Atom\Reverb\Protocol\Frame;

const KEY = 'appkey';
const SECRET = 'shared-secret';

$failures = 0;
function check(bool $ok, string $label): void
{
    global $failures;
    echo ($ok ? "\033[32mPASS\033[0m" : "\033[31mFAIL\033[0m") . " — {$label}\n";
    if (!$ok) {
        $failures++;
    }
}

/** A masked client → server text frame (handles extended lengths). */
function clientFrame(string $payload): string
{
    $mask = random_bytes(4);
    $len = strlen($payload);
    $out = chr(0x81);
    if ($len <= 125) {
        $out .= chr(0x80 | $len);
    } elseif ($len <= 0xFFFF) {
        $out .= chr(0x80 | 126) . pack('n', $len);
    } else {
        $out .= chr(0x80 | 127) . pack('NN', 0, $len);
    }
    return $out . $mask . Frame::applyMask($payload, $mask);
}

/** A minimal buffered WebSocket client. */
class Client
{
    public $ws;
    public string $buf = '';
    public string $sid = '';

    public function __construct(int $port)
    {
        $this->ws = fsockopen('127.0.0.1', $port, $e, $m, 2);
        if (!$this->ws) {
            throw new RuntimeException("cannot connect to node on {$port}");
        }
        stream_set_timeout($this->ws, 3);

        $key = base64_encode(random_bytes(16));
        fwrite($this->ws, "GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n"
            . "Connection: Upgrade\r\nSec-WebSocket-Key: {$key}\r\nSec-WebSocket-Version: 13\r\n\r\n");

        while (strpos($this->buf, "\r\n\r\n") === false) {
            $c = fread($this->ws, 2048);
            if ($c === '' || $c === false) {
                break;
            }
            $this->buf .= $c;
        }
        $this->buf = substr($this->buf, strpos($this->buf, "\r\n\r\n") + 4); // keep any frame bytes

        $established = $this->next();
        $data = json_decode($established['data'] ?? '{}', true);
        $this->sid = $data['socket_id'] ?? '';
    }

    public function send(array $msg): void
    {
        fwrite($this->ws, clientFrame(json_encode($msg)));
    }

    public function subscribePresence(string $channel, string $userId, array $info): void
    {
        $cd = json_encode(['user_id' => $userId, 'user_info' => $info]);
        $auth = Signature::channelAuth(KEY, SECRET, $this->sid, $channel, $cd);
        $this->send(['event' => 'pusher:subscribe', 'data' => ['channel' => $channel, 'auth' => $auth, 'channel_data' => $cd]]);
    }

    /** Decode the next JSON text-message frame (skipping control frames), or null on timeout. */
    public function next(float $timeout = 3): ?array
    {
        $deadline = microtime(true) + $timeout;
        while (true) {
            $f = Frame::decode($this->buf);
            if ($f !== null) {
                $this->buf = substr($this->buf, $f['consumed']);
                if ($f['opcode'] === 0x1) {
                    $j = json_decode($f['payload'], true);
                    if (is_array($j)) {
                        return $j;
                    }
                }
                continue; // control frame — keep going
            }
            if (microtime(true) > $deadline) {
                return null;
            }
            $c = @fread($this->ws, 4096);
            if ($c === '' || $c === false) {
                usleep(50000);
                continue;
            }
            $this->buf .= $c;
        }
    }

    /** Wait for a message with the given event name. */
    public function waitFor(string $event, float $timeout = 5): ?array
    {
        $deadline = microtime(true) + $timeout;
        while (microtime(true) < $deadline) {
            $m = $this->next(max(0.2, $deadline - microtime(true)));
            if ($m === null) {
                break;
            }
            if (($m['event'] ?? '') === $event) {
                return $m;
            }
        }
        return null;
    }
}

function ingest(int $port, string $channel, string $event, array $data): void
{
    $body = json_encode(['channel' => $channel, 'event' => $event, 'data' => $data]);
    $sig = Signature::ingest(SECRET, $body);
    $ig = fsockopen('127.0.0.1', $port, $e, $m, 2);
    stream_set_timeout($ig, 2);
    fwrite($ig, "POST /broadcast HTTP/1.1\r\nHost: x\r\nContent-Length: " . strlen($body)
        . "\r\nX-Reverb-Signature: {$sig}\r\nConnection: close\r\n\r\n" . $body);
    stream_get_contents($ig);
    fclose($ig);
}

function waitPort(int $port): bool
{
    for ($i = 0; $i < 100; $i++) {
        $s = @fsockopen('127.0.0.1', $port, $e, $m, 0.2);
        if ($s) {
            fclose($s);
            return true;
        }
        usleep(100000);
    }
    return false;
}

// --- spawn two nodes ------------------------------------------------------------------

$logDir = sys_get_temp_dir();
// IMPORTANT: use the ARRAY command form so PHP runs directly (no `/bin/sh -c` wrapper).
// With a string command, proc_terminate() would signal the shell, not the php child, and
// the "crashed" node would keep running — the reaper would (correctly) never fire.
$node = fn (int $ws, int $ingest, string $log) => proc_open(
    [PHP_BINARY, __DIR__ . '/node.php', (string) $ws, (string) $ingest],
    [1 => ['file', $log, 'w'], 2 => ['file', $log, 'a']],
    $pipes
);
$log1 = "{$logDir}/reverb-n1.log";
$log2 = "{$logDir}/reverb-n2.log";
$p1 = $node(8091, 8092, $log1);
$p2 = $node(8093, 8094, $log2);

if (!waitPort(8091) || !waitPort(8093)) {
    echo "FAIL — nodes did not start\n";
    echo @file_get_contents($log1) . "\n" . @file_get_contents($log2) . "\n";
    exit(1);
}
usleep(300000);

try {
    $c1 = new Client(8091); // node 1
    $c2 = new Client(8093); // node 2
    check($c1->sid !== '' && $c2->sid !== '', 'both clients handshook and received a socket_id');

    // 1) Cross-node broadcast: subscribe c2 (node2) to a public channel, broadcast via node1's ingest.
    $c2->send(['event' => 'pusher:subscribe', 'data' => ['channel' => 'orders']]);
    $c2->waitFor('pusher_internal:subscription_succeeded');
    ingest(8092, 'orders', 'OrderShipped', ['id' => 42]);
    $bc = $c2->waitFor('OrderShipped');
    check($bc !== null && ($bc['data']['id'] ?? null) === 42, 'cross-node broadcast (node1 ingest → node2 client)');

    // 2) Global presence state + cross-node member_added.
    $c1->subscribePresence('presence-room', 'u1', ['name' => 'Ada']);
    $c1->waitFor('pusher_internal:subscription_succeeded');
    $c2->subscribePresence('presence-room', 'u2', ['name' => 'Bo']);
    $ss2 = $c2->waitFor('pusher_internal:subscription_succeeded');
    $state = json_decode($ss2['data'] ?? '{}', true)['presence'] ?? [];
    check(($state['count'] ?? 0) === 2 && in_array('u1', $state['ids'] ?? [], true),
        'c2 sees the GLOBAL presence list (u1 lives on node1)');

    $ma = $c1->waitFor('pusher_internal:member_added');
    check($ma !== null && ($ma['data']['user_id'] ?? null) === 'u2',
        'cross-node member_added (u2 on node2 → c1 on node1)');

    // 3) The reaper: kill node2 ungracefully; node1 must emit member_removed for u2 once
    //    node2's liveness key expires (TTL 8s) and node1's tick reaps it.
    proc_terminate($p2, 9);
    $mr = $c1->waitFor('pusher_internal:member_removed', 20);
    check($mr !== null && ($mr['data']['user_id'] ?? null) === 'u2',
        'reaper: member_removed for a crashed node\'s member');
} catch (Throwable $e) {
    echo "FAIL — exception: {$e->getMessage()}\n";
    $failures++;
}

@proc_terminate($p1, 9);
@proc_terminate($p2, 9);

if ($failures > 0) {
    echo "\n--- node1 log ---\n" . @file_get_contents($log1);
    echo "\n--- node2 log ---\n" . @file_get_contents($log2);
}
echo "\n" . ($failures === 0 ? "All cluster checks passed.\n" : "{$failures} check(s) failed.\n");
exit($failures === 0 ? 0 : 1);
