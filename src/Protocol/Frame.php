<?php

namespace Eyika\Atom\Reverb\Protocol;

/**
 * RFC 6455 data framing. Client → server frames are always masked; server → client frames
 * are never masked. This encodes/decodes single (unfragmented) text frames — enough for a
 * JSON pub/sub protocol — plus the close/ping/pong control opcodes. Pure + testable.
 */
class Frame
{
    public const OP_CONTINUATION = 0x0;
    public const OP_TEXT         = 0x1;
    public const OP_BINARY       = 0x2;
    public const OP_CLOSE        = 0x8;
    public const OP_PING         = 0x9;
    public const OP_PONG         = 0xA;

    /** Encode a server → client frame (unmasked, FIN set). */
    public static function encode(string $payload, int $opcode = self::OP_TEXT): string
    {
        $length = strlen($payload);
        $frame = chr(0x80 | $opcode); // FIN + opcode

        if ($length <= 125) {
            $frame .= chr($length);
        } elseif ($length <= 0xFFFF) {
            $frame .= chr(126) . pack('n', $length);
        } else {
            // 64-bit length: high dword 0 (payloads never exceed 32-bit here), then low.
            $frame .= chr(127) . pack('NN', 0, $length);
        }

        return $frame . $payload;
    }

    /**
     * Decode one client → server frame from the head of $buffer. Returns
     * ['opcode' => int, 'payload' => string, 'consumed' => int] or null if the buffer
     * doesn't yet hold a full frame (caller should read more and retry).
     */
    public static function decode(string $buffer): ?array
    {
        $len = strlen($buffer);
        if ($len < 2) {
            return null;
        }

        $b0 = ord($buffer[0]);
        $b1 = ord($buffer[1]);

        $opcode = $b0 & 0x0F;
        $masked = ($b1 & 0x80) !== 0;
        $payloadLen = $b1 & 0x7F;
        $offset = 2;

        if ($payloadLen === 126) {
            if ($len < $offset + 2) {
                return null;
            }
            $payloadLen = unpack('n', substr($buffer, $offset, 2))[1];
            $offset += 2;
        } elseif ($payloadLen === 127) {
            if ($len < $offset + 8) {
                return null;
            }
            $parts = unpack('N2', substr($buffer, $offset, 8));
            $payloadLen = ($parts[1] << 32) | $parts[2];
            $offset += 8;
        }

        $maskKey = '';
        if ($masked) {
            if ($len < $offset + 4) {
                return null;
            }
            $maskKey = substr($buffer, $offset, 4);
            $offset += 4;
        }

        if ($len < $offset + $payloadLen) {
            return null; // full payload not yet buffered
        }

        $payload = substr($buffer, $offset, $payloadLen);
        if ($masked) {
            $payload = self::applyMask($payload, $maskKey);
        }

        return [
            'opcode'   => $opcode,
            'payload'  => $payload,
            'consumed' => $offset + $payloadLen,
        ];
    }

    /** XOR a payload with a 4-byte masking key (symmetric — mask == unmask). */
    public static function applyMask(string $payload, string $maskKey): string
    {
        $out = '';
        for ($i = 0, $n = strlen($payload); $i < $n; $i++) {
            $out .= $payload[$i] ^ $maskKey[$i % 4];
        }
        return $out;
    }
}
