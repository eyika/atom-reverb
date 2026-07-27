<?php

namespace Eyika\Atom\Reverb;

/**
 * In-memory channel bookkeeping: which connections are subscribed to which channels. Pure
 * (no sockets) so the pub/sub logic is unit-testable on its own — the Server layers socket
 * I/O on top and asks this class who should receive a broadcast.
 */
class ChannelManager
{
    /** @var array<string, array<int, true>> channel name => set of connection ids */
    protected array $channels = [];

    public function subscribe(int $connectionId, string $channel): void
    {
        $this->channels[$channel][$connectionId] = true;
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

    /** @return int[] connection ids subscribed to the channel */
    public function subscribers(string $channel): array
    {
        return array_keys($this->channels[$channel] ?? []);
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
