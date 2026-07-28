<?php
/**
 * A standalone Reverb node for cluster validation — no framework/composer, just the src.
 * Usage: php node.php <wsPort> <ingestPort>   (Redis from REDIS_HOST/REDIS_PORT env).
 */
$src = dirname(__DIR__, 2) . '/src';
foreach ([
    '/Protocol/Handshake.php', '/Protocol/Frame.php', '/ChannelManager.php', '/Connection.php',
    '/Auth/Signature.php', '/Backplane/Backplane.php', '/Backplane/LocalBackplane.php',
    '/Backplane/RedisBackplane.php', '/Presence/PresenceStore.php', '/Presence/LocalPresenceStore.php',
    '/Redis/RedisClient.php', '/Presence/RedisPresenceStore.php', '/Server.php',
] as $f) {
    require_once $src . $f;
}

use Eyika\Atom\Reverb\Backplane\RedisBackplane;
use Eyika\Atom\Reverb\Presence\RedisPresenceStore;
use Eyika\Atom\Reverb\Redis\RedisClient;
use Eyika\Atom\Reverb\Server;

$wsPort = (int) ($argv[1] ?? 8091);
$ingestPort = (int) ($argv[2] ?? 8092);
$redisHost = getenv('REDIS_HOST') ?: '127.0.0.1';
$redisPort = (int) (getenv('REDIS_PORT') ?: 6379);
$nodeId = bin2hex(random_bytes(6));

$backplane = new RedisBackplane($redisHost, $redisPort, 'atom-reverb');
$presence = new RedisPresenceStore(new RedisClient($redisHost, $redisPort), $nodeId, 8); // short TTL so the reaper fires fast

$server = new Server(
    null,
    [
        'host' => '127.0.0.1', 'ws_port' => $wsPort, 'ingest_port' => $ingestPort,
        'app_key' => 'appkey', 'app_secret' => 'shared-secret',
        'heartbeat_interval' => 2, 'idle_timeout' => 300, 'node_id' => $nodeId,
    ],
    $backplane,
    fn ($m) => fwrite(STDERR, "[node:{$wsPort}] {$m}\n"),
    $presence
);

$server->start();
