<?php

namespace Eyika\Atom\Reverb\Presence;

/**
 * Tracks presence-channel membership with Pusher semantics: a user may hold several
 * connections, but counts as one member — `member_added` fires on the first connection,
 * `member_removed` on the last. Implementations are single-node (in-memory) or cluster-wide
 * (Redis-backed, so `members()`/`count()` aggregate across nodes).
 */
interface PresenceStore
{
    /**
     * Register a connection's membership.
     *
     * @return bool true if this is the user's FIRST connection on the channel (emit member_added).
     */
    public function join(string $channel, string $connKey, string $userId, array $userInfo): bool;

    /**
     * Remove a connection's membership.
     *
     * @return bool true if this was the user's LAST connection (emit member_removed).
     */
    public function leave(string $channel, string $connKey, string $userId): bool;

    /**
     * The deduplicated members of a channel.
     *
     * @return array<string, array> user_id => user_info
     */
    public function members(string $channel): array;

    /** Number of distinct users present on a channel. */
    public function count(string $channel): int;

    /**
     * Reap members that belonged to crashed nodes, invoking $onRemoved($channel, $userId)
     * for each user whose last connection vanished. No-op for single-node stores.
     */
    public function reap(callable $onRemoved): void;

    /** Refresh this node's liveness so peers don't reap its members. No-op for single-node. */
    public function heartbeat(): void;
}
