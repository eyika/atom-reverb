<?php

namespace Eyika\Atom\Reverb\Redis;

use Eyika\Atom\Reverb\Backplane\RedisBackplane;
use RuntimeException;

/**
 * A minimal blocking request/reply Redis client over a raw socket — enough for the presence
 * store's data ops (HSET/HDEL/HGETALL/HLEN/EVAL/SADD/SMEMBERS/EXPIRE…). No ext-redis/predis.
 * Distinct from the backplane's pub/sub sockets (a subscribe connection can't run commands).
 */
class RedisClient
{
    /** @var resource */
    protected $socket;
    protected string $buffer = '';

    public function __construct(string $host = '127.0.0.1', int $port = 6379, ?string $password = null, float $timeout = 2.0)
    {
        $socket = @stream_socket_client("tcp://{$host}:{$port}", $errno, $errstr, $timeout);
        if ($socket === false) {
            throw new RuntimeException("Cannot connect to Redis at {$host}:{$port} — {$errstr} ({$errno})");
        }
        $this->socket = $socket;
        stream_set_timeout($this->socket, (int) ceil($timeout));

        if ($password !== null && $password !== '') {
            $this->command(['AUTH', $password]);
        }
    }

    /** Run a command (array of arguments) and return the parsed reply. */
    public function command(array $args): mixed
    {
        @fwrite($this->socket, self::encode($args));
        return $this->readReply();
    }

    /** Run a Lua script via EVAL. */
    public function eval(string $script, array $keys, array $args): mixed
    {
        $cmd = ['EVAL', $script, (string) count($keys)];
        foreach ($keys as $k) {
            $cmd[] = $k;
        }
        foreach ($args as $a) {
            $cmd[] = $a;
        }
        return $this->command($cmd);
    }

    /** Encode a RESP command (array of bulk strings). */
    public static function encode(array $args): string
    {
        $out = '*' . count($args) . "\r\n";
        foreach ($args as $arg) {
            $arg = (string) $arg;
            $out .= '$' . strlen($arg) . "\r\n" . $arg . "\r\n";
        }
        return $out;
    }

    protected function readReply(): mixed
    {
        while (true) {
            $parsed = RedisBackplane::parseReply($this->buffer, 0);
            if ($parsed !== null) {
                [$value, $consumed] = $parsed;
                $this->buffer = substr($this->buffer, $consumed);
                return $value;
            }
            $chunk = @fread($this->socket, 65535);
            if ($chunk === '' || $chunk === false) {
                return null; // connection closed
            }
            $this->buffer .= $chunk;
        }
    }
}
