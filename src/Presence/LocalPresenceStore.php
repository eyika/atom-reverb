<?php

namespace Eyika\Atom\Reverb\Presence;

/**
 * In-memory presence for a single node. Reference-counts users so a user with several
 * connections is one member — the correct Pusher semantics even on one node.
 */
class LocalPresenceStore implements PresenceStore
{
    /** @var array<string, array<string, string>> channel => connKey => userId */
    protected array $conns = [];
    /** @var array<string, array<string, array>> channel => userId => userInfo */
    protected array $info = [];
    /** @var array<string, array<string, int>> channel => userId => refcount */
    protected array $refs = [];

    public function join(string $channel, string $connKey, string $userId, array $userInfo): bool
    {
        $this->conns[$channel][$connKey] = $userId;
        $this->info[$channel][$userId] = $userInfo;

        $count = ($this->refs[$channel][$userId] ?? 0) + 1;
        $this->refs[$channel][$userId] = $count;

        return $count === 1;
    }

    public function leave(string $channel, string $connKey, string $userId): bool
    {
        unset($this->conns[$channel][$connKey]);

        $count = ($this->refs[$channel][$userId] ?? 1) - 1;
        if ($count <= 0) {
            unset($this->refs[$channel][$userId], $this->info[$channel][$userId]);
            $this->pruneChannel($channel);
            return true;
        }

        $this->refs[$channel][$userId] = $count;
        return false;
    }

    public function members(string $channel): array
    {
        return $this->info[$channel] ?? [];
    }

    public function count(string $channel): int
    {
        return count($this->refs[$channel] ?? []);
    }

    public function reap(callable $onRemoved): void
    {
        // single node — nothing to reap
    }

    public function heartbeat(): void
    {
        // single node — no peers
    }

    protected function pruneChannel(string $channel): void
    {
        if (empty($this->refs[$channel])) {
            unset($this->conns[$channel], $this->info[$channel], $this->refs[$channel]);
        }
    }
}
