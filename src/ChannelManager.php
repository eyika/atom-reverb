<?php

namespace Eyika\Atom\Reverb;

/**
 * In-memory channel bookkeeping: which connections are subscribed to which channels, and —
 * for presence channels — the member payload each connection carries. Pure (no sockets) so
 * the pub/sub + presence logic is unit-testable on its own.
 */
class ChannelManager
{
    /**
     * channel name => (connection id => member data | true). A value of `true` is a plain
     * subscriber; an array is a presence member ({user_id, user_info}).
     *
     * @var array<string, array<int, array|true>>
     */
    protected array $channels = [];

    /** Subscribe a connection; pass $member (array) for a presence channel. */
    public function subscribe(int $connectionId, string $channel, ?array $member = null): void
    {
        $this->channels[$channel][$connectionId] = $member ?? true;
    }

    public function unsubscribe(int $connectionId, string $channel): void
    {
        unset($this->channels[$channel][$connectionId]);
        if (empty($this->channels[$channel])) {
            unset($this->channels[$channel]);
        }
    }

    /** Drop a connection from every channel (call on disconnect). */
    public function forget(int $connectionId): void
    {
        foreach ($this->channels as $channel => $members) {
            unset($this->channels[$channel][$connectionId]);
            if (empty($this->channels[$channel])) {
                unset($this->channels[$channel]);
            }
        }
    }

    public function isSubscribed(int $connectionId, string $channel): bool
    {
        return isset($this->channels[$channel][$connectionId]);
    }

    /** @return int[] connection ids subscribed to the channel */
    public function subscribers(string $channel): array
    {
        return array_keys($this->channels[$channel] ?? []);
    }

    /**
     * Presence members of a channel: connection id => member payload.
     *
     * @return array<int, array>
     */
    public function members(string $channel): array
    {
        return array_filter(
            $this->channels[$channel] ?? [],
            fn ($member) => is_array($member)
        );
    }

    /** The presence member payload a connection carries on a channel (null if none). */
    public function memberFor(int $connectionId, string $channel): ?array
    {
        $member = $this->channels[$channel][$connectionId] ?? null;
        return is_array($member) ? $member : null;
    }

    /** @return string[] channels a connection is subscribed to */
    public function channelsFor(int $connectionId): array
    {
        $out = [];
        foreach ($this->channels as $channel => $members) {
            if (isset($members[$connectionId])) {
                $out[] = $channel;
            }
        }
        return $out;
    }

    /** @return string[] all channels with at least one subscriber */
    public function channels(): array
    {
        return array_keys($this->channels);
    }

    public function count(string $channel): int
    {
        return count($this->channels[$channel] ?? []);
    }
}
