<?php

namespace Eyika\Atom\Reverb\Presence;

use Eyika\Atom\Reverb\Redis\RedisClient;

/**
 * Cluster-wide presence backed by Redis, so members()/count() aggregate across every node.
 * Per channel it keeps three hashes — connKey→userId, userId→refcount, userId→userInfo — and
 * mutates them with Lua scripts so join/leave dedup by user is atomic under concurrency.
 *
 * Crashed nodes are handled by a liveness heartbeat + a reaper: each node records the members
 * it owns and refreshes a TTL key; a surviving node claims an expired node and removes its
 * members (emitting member_removed for the users whose last connection vanished).
 *
 * Requires a running Redis; the pure key/Lua layout is covered by tests, but end-to-end
 * behaviour needs a real cluster to validate.
 */
class RedisPresenceStore implements PresenceStore
{
    /** HSET conn; HSET info; refcount++ → return the new refcount (1 = first connection). */
    private const JOIN = <<<'LUA'
    redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
    redis.call('HSET', KEYS[3], ARGV[2], ARGV[3])
    return redis.call('HINCRBY', KEYS[2], ARGV[2], 1)
    LUA;

    /** HDEL conn; refcount-- ; when it hits zero drop the user → return 1 (last connection). */
    private const LEAVE = <<<'LUA'
    redis.call('HDEL', KEYS[1], ARGV[1])
    local n = redis.call('HINCRBY', KEYS[2], ARGV[2], -1)
    if n <= 0 then
        redis.call('HDEL', KEYS[2], ARGV[2])
        redis.call('HDEL', KEYS[3], ARGV[2])
        return 1
    end
    return 0
    LUA;

    public function __construct(
        protected RedisClient $redis,
        protected string $nodeId,
        protected int $ttl = 60
    ) {
    }

    /**
     * The three per-channel keys. The channel is wrapped in a Redis Cluster hash tag
     * ({channel}) so all three land in the same hash slot — required for the multi-key Lua
     * scripts to run on a sharded Redis Cluster (harmless on a single instance).
     *
     * @return array{0:string,1:string,2:string} [conn→user, user→refcount, user→info]
     */
    private function keys(string $channel): array
    {
        $tag = '{' . $channel . '}';
        return ["presence:{$tag}", "presence:{$tag}:u", "presence:{$tag}:i"];
    }

    public function join(string $channel, string $connKey, string $userId, array $userInfo): bool
    {
        $refcount = (int) $this->redis->eval(
            self::JOIN,
            $this->keys($channel),
            [$connKey, $userId, json_encode($userInfo)]
        );

        // Record ownership so this member can be reaped if the node dies.
        $this->redis->command(['SADD', "node:{$this->nodeId}:members", "{$channel}\x1f{$connKey}\x1f{$userId}"]);

        return $refcount === 1;
    }

    public function leave(string $channel, string $connKey, string $userId): bool
    {
        $last = (int) $this->redis->eval(
            self::LEAVE,
            $this->keys($channel),
            [$connKey, $userId]
        );

        $this->redis->command(['SREM', "node:{$this->nodeId}:members", "{$channel}\x1f{$connKey}\x1f{$userId}"]);

        return $last === 1;
    }

    public function members(string $channel): array
    {
        $flat = $this->redis->command(['HGETALL', $this->keys($channel)[2]]);
        if (!is_array($flat)) {
            return [];
        }

        $members = [];
        for ($i = 0; $i + 1 < count($flat); $i += 2) {
            $decoded = json_decode((string) $flat[$i + 1], true);
            $members[(string) $flat[$i]] = is_array($decoded) ? $decoded : [];
        }
        return $members;
    }

    public function count(string $channel): int
    {
        return (int) $this->redis->command(['HLEN', $this->keys($channel)[1]]);
    }

    public function heartbeat(): void
    {
        $this->redis->command(['SET', "node:{$this->nodeId}:alive", '1', 'EX', (string) $this->ttl]);
        $this->redis->command(['SADD', 'presence:nodes', $this->nodeId]);
    }

    public function reap(callable $onRemoved): void
    {
        $nodes = $this->redis->command(['SMEMBERS', 'presence:nodes']);
        if (!is_array($nodes)) {
            return;
        }

        foreach ($nodes as $node) {
            if ($node === $this->nodeId || $this->redis->command(['EXISTS', "node:{$node}:alive"]) == 1) {
                continue; // ourselves, or still alive
            }
            // Claim the dead node (only the winner cleans it up → no duplicate removals).
            if ((int) $this->redis->command(['SREM', 'presence:nodes', $node]) !== 1) {
                continue;
            }

            $members = $this->redis->command(['SMEMBERS', "node:{$node}:members"]);
            foreach ((array) $members as $entry) {
                [$channel, $connKey, $userId] = array_pad(explode("\x1f", (string) $entry), 3, '');
                $last = (int) $this->redis->eval(
                    self::LEAVE,
                    $this->keys($channel),
                    [$connKey, $userId]
                );
                if ($last === 1) {
                    $onRemoved($channel, $userId);
                }
            }
            $this->redis->command(['DEL', "node:{$node}:members"]);
        }
    }
}
