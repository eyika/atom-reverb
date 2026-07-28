<?php

namespace Eyika\Atom\Reverb\Backplane;

/**
 * A backplane lets multiple Reverb nodes behind a load balancer share broadcasts: a node
 * fans a message out to its OWN connections directly and `publish()`es it so peer nodes fan
 * it out to theirs. `poll()` delivers messages that arrived from peers (never the node's own).
 */
interface Backplane
{
    /** Publish a broadcast to peer nodes. */
    public function publish(array $message): void;

    /** Deliver any messages received from peers, invoking $handler(array $message) for each. */
    public function poll(callable $handler): void;

    /**
     * Stream resources to include in the server's select read-set so peer messages wake the
     * loop promptly (empty for backplanes that don't use a socket).
     *
     * @return array<int,resource>
     */
    public function readStreams(): array;
}
