<?php

namespace Eyika\Atom\Reverb\Contracts;

/**
 * Marker for an event that should be pushed to WebSocket subscribers when dispatched.
 * Dispatch such an event through the framework's Event dispatcher and the
 * ReverbServiceProvider's wildcard listener forwards it to the Reverb server (which fans
 * it out to the channel's subscribers). Or broadcast it explicitly via Broadcast::event().
 */
interface ShouldBroadcast
{
    /** The channel name to broadcast on. */
    public function broadcastOn(): string;

    /** The event name clients receive (defaults are typically the class basename). */
    public function broadcastAs(): string;

    /** The JSON-serialisable payload sent to subscribers. */
    public function broadcastWith(): array;
}
