<?php

namespace Eyika\Atom\Reverb;

/**
 * Per-connection state for the WebSocket server: read/write buffers (the write buffer gives
 * back-pressure — data that can't be flushed now is drained when the socket is writable),
 * fragmentation reassembly, heartbeat bookkeeping, the assigned socket id, and the channels
 * this connection is subscribed to (with presence member data where applicable).
 */
class Connection
{
    /** @var resource */
    public $socket;
    public int $id;

    public string $readBuffer = '';
    public string $writeBuffer = '';
    public bool $handshook = false;
    public string $socketId = '';

    public float $lastActivity;
    public bool $awaitingPong = false;

    /** Reassembly of a fragmented message. */
    public string $fragment = '';
    public ?int $fragmentOpcode = null;

    /** @var array<string, array|null> channel name => presence member data (null = non-presence) */
    public array $channels = [];

    public function __construct(int $id, $socket, float $now)
    {
        $this->id = $id;
        $this->socket = $socket;
        $this->lastActivity = $now;
    }

    /** Queue bytes to send; they're flushed by the server's write loop (non-blocking). */
    public function queue(string $data): void
    {
        $this->writeBuffer .= $data;
    }

    public function hasPendingWrites(): bool
    {
        return $this->writeBuffer !== '';
    }

    /**
     * Attempt to flush the write buffer without blocking. Returns:
     *   true  — buffer fully drained,
     *   false — bytes remain (register for write-readiness) or the socket errored.
     */
    public function flush(): bool
    {
        if ($this->writeBuffer === '') {
            return true;
        }
        $written = @fwrite($this->socket, $this->writeBuffer);
        if ($written === false) {
            return false;
        }
        if ($written > 0) {
            $this->writeBuffer = substr($this->writeBuffer, $written);
        }
        return $this->writeBuffer === '';
    }

    /** Mark inbound activity (resets the idle clock + pending-pong flag). */
    public function touch(float $now): void
    {
        $this->lastActivity = $now;
        $this->awaitingPong = false;
    }
}
