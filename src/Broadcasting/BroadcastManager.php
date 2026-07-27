<?php

namespace Eyika\Atom\Reverb\Broadcasting;

use Eyika\Atom\Reverb\Contracts\ShouldBroadcast;

/**
 * The application side of broadcasting. A short-lived PHP-FPM request can't reach the
 * long-lived WebSocket clients directly, so it POSTs the event to the Reverb server's
 * ingest port over localhost; the server fans it out. No Redis / queue required.
 */
class BroadcastManager
{
    protected string $host;
    protected int $port;
    protected float $timeout;

    public function __construct(?string $host = null, ?int $port = null, float $timeout = 1.0)
    {
        $env = function_exists('env');
        $this->host = $host ?? ($env ? (string) env('REVERB_INGEST_HOST', '127.0.0.1') : '127.0.0.1');
        $this->port = $port ?? ($env ? (int) env('REVERB_INGEST_PORT', 8092) : 8092);
        $this->timeout = $timeout;
    }

    /** Broadcast a raw channel/event/data tuple. Returns true when the server acks 200. */
    public function send(string $channel, string $event, mixed $data = []): bool
    {
        return $this->post(['channel' => $channel, 'event' => $event, 'data' => $data]);
    }

    /** Broadcast a ShouldBroadcast event object. */
    public function event(ShouldBroadcast $event): bool
    {
        return $this->send($event->broadcastOn(), $event->broadcastAs(), $event->broadcastWith());
    }

    /** Serialize the payload and POST it to the Reverb ingest endpoint. */
    protected function post(array $payload): bool
    {
        $body = json_encode($payload);

        $fp = @fsockopen($this->host, $this->port, $errno, $errstr, $this->timeout);
        if ($fp === false) {
            return false;
        }

        $request = "POST /broadcast HTTP/1.1\r\n"
            . "Host: {$this->host}\r\n"
            . "Content-Type: application/json\r\n"
            . "Content-Length: " . strlen($body) . "\r\n"
            . "Connection: close\r\n\r\n"
            . $body;

        @fwrite($fp, $request);

        stream_set_timeout($fp, (int) ceil($this->timeout));
        $response = '';
        while (!feof($fp)) {
            $chunk = @fread($fp, 4096);
            if ($chunk === '' || $chunk === false) {
                break;
            }
            $response .= $chunk;
        }
        @fclose($fp);

        return str_contains($response, ' 200 ');
    }
}
