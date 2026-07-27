<?php

namespace Eyika\Atom\Reverb\Support;

use Eyika\Atom\Framework\Support\Facade\Facade;

/**
 * Facade over the BroadcastManager.
 *
 * @method static bool send(string $channel, string $event, mixed $data = [])
 * @method static bool event(\Eyika\Atom\Reverb\Contracts\ShouldBroadcast $event)
 */
class Broadcast extends Facade
{
    protected static function getFacadeAccessor()
    {
        return 'reverb.broadcast';
    }
}
