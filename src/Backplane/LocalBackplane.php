<?php

namespace Eyika\Atom\Reverb\Backplane;

/**
 * Single-node backplane: there are no peers, so publish/poll are no-ops. The server still
 * fans out to its own connections directly. This is the default when Redis isn't configured.
 */
class LocalBackplane implements Backplane
{
    public function publish(array $message): void
    {
        // no peers
    }

    public function poll(callable $handler): void
    {
        // no peers
    }

    public function readStreams(): array
    {
        return [];
    }
}
