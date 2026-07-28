<?php

namespace Eyika\Atom\Reverb;

use Eyika\Atom\Framework\Foundation\ServiceProvider;
use Eyika\Atom\Reverb\Broadcasting\BroadcastManager;
use Eyika\Atom\Reverb\Console\StartCommand;
use Eyika\Atom\Reverb\Contracts\ShouldBroadcast;

/**
 * Auto-discovered via composer.json extra.atom.providers (PKG-02). Wires:
 *   - config/reverb.php (merged; publishable with --tag=reverb-config),
 *   - the BroadcastManager (bound as 'reverb.broadcast', used by the Broadcast facade +
 *     the broadcast() helper + your broadcasting-auth endpoint),
 *   - the `reverb:start` command,
 *   - a wildcard Event listener that auto-forwards any dispatched ShouldBroadcast event.
 */
class ReverbServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/../config/reverb.php', 'reverb');

        $this->app->bind('reverb.broadcast', function () {
            return new BroadcastManager(
                host: config('reverb.host', '127.0.0.1'),
                port: (int) config('reverb.ingest_port', 8092),
                appKey: (string) config('reverb.app_key', 'atom'),
                appSecret: (string) config('reverb.app_secret', '')
            );
        });
        $this->app->bind(BroadcastManager::class, fn ($app) => $app->make('reverb.broadcast'));
    }

    public function boot(): void
    {
        $this->commands([
            StartCommand::class,
        ]);

        $this->publishes([
            __DIR__ . '/../config/reverb.php' => base_path('config/reverb.php'),
        ], 'reverb-config');

        if ($this->app->bound('events')) {
            $this->app->make('events')->listen('*', function ($event = null) {
                if ($event instanceof ShouldBroadcast) {
                    $this->app->make('reverb.broadcast')->event($event);
                }
            });
        }
    }
}
