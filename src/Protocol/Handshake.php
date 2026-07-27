<?php

namespace Eyika\Atom\Reverb\Protocol;

/**
 * The RFC 6455 opening handshake. A WebSocket client sends an HTTP Upgrade request with a
 * Sec-WebSocket-Key; the server proves it speaks the protocol by echoing a derived
 * Sec-WebSocket-Accept. Pure + testable — no sockets here.
 */
class Handshake
{
    /** The magic GUID every RFC 6455 server concatenates before hashing (§4.2.2). */
    public const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

    /** Derive the Sec-WebSocket-Accept value from the client's Sec-WebSocket-Key. */
    public static function acceptKey(string $secWebSocketKey): string
    {
        return base64_encode(sha1($secWebSocketKey . self::GUID, true));
    }

    /** Extract the Sec-WebSocket-Key header value from a raw HTTP upgrade request. */
    public static function keyFrom(string $rawRequest): ?string
    {
        if (preg_match('/Sec-WebSocket-Key:\s*(.+)\r\n/i', $rawRequest, $m)) {
            return trim($m[1]);
        }
        return null;
    }

    /** True when the raw request is a WebSocket upgrade. */
    public static function isUpgrade(string $rawRequest): bool
    {
        return (bool) preg_match('/Upgrade:\s*websocket/i', $rawRequest);
    }

    /** Build the 101 Switching Protocols response for a given client key. */
    public static function response(string $secWebSocketKey): string
    {
        $accept = self::acceptKey($secWebSocketKey);

        return "HTTP/1.1 101 Switching Protocols\r\n"
            . "Upgrade: websocket\r\n"
            . "Connection: Upgrade\r\n"
            . "Sec-WebSocket-Accept: {$accept}\r\n\r\n";
    }
}
