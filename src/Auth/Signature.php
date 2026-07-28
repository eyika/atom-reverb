<?php

namespace Eyika\Atom\Reverb\Auth;

/**
 * HMAC-SHA256 signatures, Pusher-compatible:
 *
 *  - Channel auth: a client subscribing to a `private-*` / `presence-*` channel must present
 *    an `auth` string that only the application (holding the secret) can produce, binding the
 *    subscription to the connection's socket id (and, for presence, the member payload).
 *  - Ingest auth: the application signs the broadcast body it POSTs to the server so the
 *    server only fans out messages from a trusted source.
 *
 * All comparisons are constant-time.
 */
class Signature
{
    /** The `auth` string a client must present to subscribe: "appKey:hmac". */
    public static function channelAuth(
        string $appKey,
        string $secret,
        string $socketId,
        string $channel,
        ?string $channelData = null
    ): string {
        $payload = $socketId . ':' . $channel . ($channelData !== null ? ':' . $channelData : '');

        return $appKey . ':' . hash_hmac('sha256', $payload, $secret);
    }

    public static function verifyChannel(
        string $appKey,
        string $secret,
        string $socketId,
        string $channel,
        string $auth,
        ?string $channelData = null
    ): bool {
        $expected = self::channelAuth($appKey, $secret, $socketId, $channel, $channelData);

        return hash_equals($expected, $auth);
    }

    /** Signature for a broadcast ingest body (app → server). */
    public static function ingest(string $secret, string $body): string
    {
        return hash_hmac('sha256', $body, $secret);
    }

    public static function verifyIngest(string $secret, string $body, string $signature): bool
    {
        return hash_equals(self::ingest($secret, $body), $signature);
    }

    public static function isPrivate(string $channel): bool
    {
        return str_starts_with($channel, 'private-') || self::isPresence($channel);
    }

    public static function isPresence(string $channel): bool
    {
        return str_starts_with($channel, 'presence-');
    }
}
