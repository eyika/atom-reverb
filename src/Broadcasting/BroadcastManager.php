<?php

namespace Eyika\Atom\Reverb\Broadcasting;

use Eyika\Atom\Reverb\Auth\Signature;
use Eyika\Atom\Reverb\Contracts\ShouldBroadcast;

/**
 * The application side of broadcasting. A short-lived PHP-FPM request can't reach the
 * long-lived WebSocket clients directly, so it POSTs the event (HMAC-signed) to the Reverb
 * server's ingest port; the server verifies + fans it out. Also produces the auth payload
 * your broadcasting-auth endpoint returns for private/presence subscriptions.
 */
class BroadcastManager
{
    protected string $host;
    protected int $port;
    protected float $timeout;
    protected string $appKey;
    protected string $appSecret;

    public function __construct(
        ?string $host = null,
        ?int $port = null,
        float $timeout = 1.0,
        ?string $appKey = null,
        ?string $appSecret = null
    ) {
        $env = function_exists('env');
        $this->host = $host ?? ($env ? (string) env('REVERB_INGEST_HOST', '127.0.0.1') : '127.0.0.1');
        $this->port = $port ?? ($env ? (int) env('REVERB_INGEST_PORT', 8092) : 8092);
        $this->timeout = $timeout;
        $this->appKey = $appKey ?? ($env ? (string) env('REVERB_APP_KEY', 'atom') : 'atom');
        $this->appSecret = $appSecret ?? ($env ? (string) env('REVERB_APP_SECRET', '') : '');
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

    /**
     * Build the auth payload for a private/presence subscription. Call this from your
     * broadcasting-auth endpoint AFTER checking the authenticated user may access $channel.
     * Returns ['auth' => 'appKey:hmac'] plus 'channel_data' for presence channels.
     *
     * @param  array|null  $presenceData  {user_id, user_info} for presence channels
     * @return array{auth:string, channel_data?:string}
     */
    public function channelAuth(string $socketId, string $channel, ?array $presenceData = null): array
    {
        $channelData = null;
        $out = [];

        if (Signature::isPresence($channel)) {
            $channelData = json_encode($presenceData ?? ['user_id' => $socketId]);
            $out['channel_data'] = $channelData;
        }

        $out['auth'] = Signature::channelAuth($this->appKey, $this->appSecret, $socketId, $channel, $channelData);

        return $out;
    }

    /** Serialize the payload and POST it (signed) to the Reverb ingest endpoint. */
    protected function post(array $payload): bool
    {
        $body = json_encode($payload);

        $fp = @fsockopen($this->host, $this->port, $errno, $errstr, $this->timeout);
        if ($fp === false) {
            return false;
        }

        $headers = "POST /broadcast HTTP/1.1\r\n"
            . "Host: {$this->host}\r\n"
            . "Content-Type: application/json\r\n"
            . 'Content-Length: ' . strlen($body) . "\r\n";
        if ($this->appSecret !== '') {
            $headers .= 'X-Reverb-Signature: ' . Signature::ingest($this->appSecret, $body) . "\r\n";
        }
        $headers .= "Connection: close\r\n\r\n";

        @fwrite($fp, $headers . $body);

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
