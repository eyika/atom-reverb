<?php

namespace Eyika\Atom\Reverb\Backplane;

use RuntimeException;

/**
 * Redis pub/sub backplane using a minimal RESP client over raw sockets — no ext-redis /
 * predis, and the subscribe socket plugs straight into the server's stream_select loop.
 * Each node publishes broadcasts tagged with its own id and skips its own on receipt, so a
 * message fans out exactly once per node.
 */
class RedisBackplane implements Backplane
{
    /** @var resource */
    protected $pub;
    /** @var resource */
    protected $sub;
    protected string $buffer = '';
    protected string $nodeId;
    /** @var string[] */
    protected array $pending = [];

    public function __construct(
        string $host = '127.0.0.1',
        int $port = 6379,
        protected string $channel = 'atom-reverb',
        ?string $password = null,
        float $timeout = 2.0
    ) {
        $this->nodeId = bin2hex(random_bytes(8));

        $this->pub = $this->connect($host, $port, $password, $timeout);
        $this->sub = $this->connect($host, $port, $password, $timeout);

        // Enter subscribe mode on the sub connection; make it non-blocking for the loop.
        $this->command($this->sub, ['SUBSCRIBE', $this->channel]);
        stream_set_blocking($this->sub, false);
    }

    public function publish(array $message): void
    {
        $envelope = json_encode(['node' => $this->nodeId, 'payload' => $message]);
        $this->command($this->pub, ['PUBLISH', $this->channel, $envelope]);
    }

    public function poll(callable $handler): void
    {
        $chunk = @fread($this->sub, 65535);
        if ($chunk !== '' && $chunk !== false) {
            $this->buffer .= $chunk;
            $this->extract();
        }

        while ($this->pending) {
            $raw = array_shift($this->pending);
            $envelope = json_decode($raw, true);
            if (!is_array($envelope) || ($envelope['node'] ?? null) === $this->nodeId) {
                continue; // ignore malformed + our own messages (we already fanned out locally)
            }
            if (isset($envelope['payload']) && is_array($envelope['payload'])) {
                $handler($envelope['payload']);
            }
        }
    }

    public function readStreams(): array
    {
        return [$this->sub];
    }

    // --- RESP plumbing ----------------------------------------------------------------

    /** @return resource */
    protected function connect(string $host, int $port, ?string $password, float $timeout)
    {
        $socket = @stream_socket_client("tcp://{$host}:{$port}", $errno, $errstr, $timeout);
        if ($socket === false) {
            throw new RuntimeException("Cannot connect to Redis at {$host}:{$port} — {$errstr} ({$errno})");
        }
        if ($password !== null && $password !== '') {
            $this->command($socket, ['AUTH', $password]);
        }
        return $socket;
    }

    /** Write a RESP command (array of bulk strings). */
    protected function command($socket, array $args): void
    {
        $out = '*' . count($args) . "\r\n";
        foreach ($args as $arg) {
            $out .= '$' . strlen((string) $arg) . "\r\n" . $arg . "\r\n";
        }
        @fwrite($socket, $out);
    }

    /** Pull complete pub/sub "message" replies out of the accumulated buffer. */
    protected function extract(): void
    {
        while (($reply = self::parseReply($this->buffer, 0)) !== null) {
            [$value, $consumed] = $reply;
            $this->buffer = substr($this->buffer, $consumed);

            if (is_array($value) && ($value[0] ?? '') === 'message' && isset($value[2])) {
                $this->pending[] = (string) $value[2];
            }
        }
    }

    /**
     * Parse one RESP reply from $buf at $off. Returns [value, newOffset] or null if the
     * buffer doesn't yet hold a complete reply.
     */
    public static function parseReply(string $buf, int $off): ?array
    {
        if (!isset($buf[$off])) {
            return null;
        }
        $type = $buf[$off];
        $lineEnd = strpos($buf, "\r\n", $off);
        if ($lineEnd === false) {
            return null;
        }
        $line = substr($buf, $off + 1, $lineEnd - $off - 1);
        $next = $lineEnd + 2;

        switch ($type) {
            case '+':
            case '-':
                return [$line, $next];
            case ':':
                return [(int) $line, $next];
            case '$':
                $len = (int) $line;
                if ($len === -1) {
                    return [null, $next];
                }
                if (strlen($buf) < $next + $len + 2) {
                    return null;
                }
                return [substr($buf, $next, $len), $next + $len + 2];
            case '*':
                $count = (int) $line;
                $items = [];
                $cur = $next;
                for ($i = 0; $i < $count; $i++) {
                    $item = self::parseReply($buf, $cur);
                    if ($item === null) {
                        return null;
                    }
                    [$val, $cur] = $item;
                    $items[] = $val;
                }
                return [$items, $cur];
        }

        return null;
    }
}
