<?php

namespace Eyika\Atom\Reverb\Console;

use Eyika\Atom\Framework\Foundation\Console\Command;
use Eyika\Atom\Reverb\Backplane\LocalBackplane;
use Eyika\Atom\Reverb\Backplane\RedisBackplane;
use Eyika\Atom\Reverb\Server;
use Throwable;

/**
 * Start the WebSocket broadcast server:
 *
 *   php artisan reverb:start
 *   php artisan reverb:start --host=0.0.0.0 --ws-port=8091 --ingest-port=8092
 *
 * Reads config/reverb.php for credentials, TLS, connection limits, and the Redis backplane.
 */
class StartCommand extends Command
{
    public string $signature = 'reverb:start';
    public string $description = 'Start the Reverb-style WebSocket broadcast server';

    public function handle(): bool
    {
        $cfg = fn (string $key, $default) => function_exists('config') ? config("reverb.{$key}", $default) : $default;

        $options = [
            'host'               => (string) ($this->option('host') ?: $cfg('host', '127.0.0.1')),
            'ws_port'            => (int) ($this->option('ws-port') ?: $cfg('ws_port', 8091)),
            'ingest_port'        => (int) ($this->option('ingest-port') ?: $cfg('ingest_port', 8092)),
            'app_key'            => (string) $cfg('app_key', 'atom'),
            'app_secret'         => (string) $cfg('app_secret', ''),
            'max_connections'    => (int) $cfg('max_connections', 10000),
            'heartbeat_interval' => (int) $cfg('heartbeat_interval', 30),
            'idle_timeout'       => (int) $cfg('idle_timeout', 120),
            'tls'                => (array) $cfg('tls', ['enabled' => false]),
        ];

        $backplane = $cfg('redis.enabled', false)
            ? new RedisBackplane(
                (string) $cfg('redis.host', '127.0.0.1'),
                (int) $cfg('redis.port', 6379),
                (string) $cfg('redis.channel', 'atom-reverb'),
                $cfg('redis.password', null) ?: null
            )
            : new LocalBackplane();

        if ($options['app_secret'] === '') {
            $this->warn('REVERB_APP_SECRET is empty — channel + ingest auth are DISABLED (development only).');
        }

        $server = new Server(null, $options, $backplane, fn (string $msg) => $this->info($msg));

        try {
            $server->start();
        } catch (Throwable $e) {
            $this->error('reverb:start failed: ' . $e->getMessage());
            return false;
        }

        return true;
    }
}
