<?php

if (!function_exists('broadcast')) {
    /**
     * Broadcast an event to a channel's WebSocket subscribers via the Reverb server.
     * Returns true when the server acknowledges the ingest.
     */
    function broadcast(string $channel, string $event, mixed $data = []): bool
    {
        return app()->make('reverb.broadcast')->send($channel, $event, $data);
    }
}
