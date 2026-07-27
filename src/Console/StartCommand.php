<?php

namespace Eyika\Atom\Reverb\Console;

use Eyika\Atom\Framework\Foundation\Console\Command;
use Eyika\Atom\Reverb\Server;
use Throwable;

/**
 * Start the WebSocket broadcast server:
 *
 *   php console reverb:start --host=127.0.0.1 --ws-port=8091 --ingest-port=8092
 */
class StartCommand extends Command
{
    public string $signature = 'reverb:start';
    public string $description = 'Start the Reverb-style WebSocket broadcast server';

    public function handle(): bool
    {
        $host       = (string) ($this->option('host') ?: '127.0.0.1');
        $wsPort     = (int) ($this->option('ws-port') ?: 8091);
        $ingestPort = (int) ($this->option('ingest-port') ?: 8092);

        $server = new Server(null, fn (string $msg) => $this->info($msg));

        try {
            $server->start($host, $wsPort, $ingestPort);
        } catch (Throwable $e) {
            $this->error('reverb:start failed: ' . $e->getMessage());
            return false;
        }

        return true;
    }
}
