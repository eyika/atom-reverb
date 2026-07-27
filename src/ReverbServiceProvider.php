<?php

namespace Eyika\Atom\Reverb;

use Eyika\Atom\Framework\Foundation\ServiceProvider;
use Eyika\Atom\Reverb\Broadcasting\BroadcastManager;
use Eyika\Atom\Reverb\Console\StartCommand;
use Eyika\Atom\Reverb\Contracts\ShouldBroadcast;

/**
 * Auto-discovered via composer.json extra.atom.providers (PKG-02). Wires:
 *   - the BroadcastManager (bound as 'reverb.broadcast', used by the Broadcast facade),
 *   - the `reverb:start` command,
 *   - a wildcard Event listener that auto-forwards any dispatched ShouldBroadcast event
 *     to the Reverb server — so `event(new OrderShipped(...))` reaches the browser.
 */
class ReverbServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->app->bind('reverb.broadcast', fn () => new BroadcastManager());
        $this->app->bind(BroadcastManager::class, fn ($app) => $app->make('reverb.broadcast'));
    }

    public function boot(): void
    {
        $this->commands([
            StartCommand::class,
        ]);

        if ($this->app->bound('events')) {
            $this->app->make('events')->listen('*', function ($event = null) {
                if ($event instanceof ShouldBroadcast) {
                    $this->app->make('reverb.broadcast')->event($event);
                }
            });
        }
    }
}
