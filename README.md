# Atom Reverb

A **lightweight, dependency-free WebSocket broadcast server** for the
[Atom framework](https://github.com/eyika/atomframework) — Laravel-Reverb-style. No
ratchet / swoole / react; just PHP's own `stream_socket_server` + `stream_select`.

Like `atom-octane`, this package doubles as a proof that the framework's **package
auto-discovery** (`extra.atom.providers`) and **event system** compose cleanly with a
long-lived server process.

## Install

```bash
composer require eyika/atom-reverb
```

The `ReverbServiceProvider` is auto-discovered — it registers the `reverb:start` command,
the `Broadcast` facade / `broadcast()` helper, and a wildcard event listener that
auto-forwards `ShouldBroadcast` events.

## Run the server

```bash
php console reverb:start --host=127.0.0.1 --ws-port=8091 --ingest-port=8092
```

It listens on two ports:

- **WS port** — browsers connect, subscribe to channels, and receive broadcasts.
- **ingest port** — the application POSTs events here; the server fans each out to that
  channel's subscribers. This split is what lets a short-lived PHP-FPM request reach
  long-lived socket clients **without Redis**.

## Broadcast from your app

Explicitly:

```php
use Eyika\Atom\Reverb\Support\Broadcast;

Broadcast::send('orders', 'OrderShipped', ['id' => 42]);
// or the helper:
broadcast('orders', 'OrderShipped', ['id' => 42]);
```

Or automatically — dispatch a `ShouldBroadcast` event through the framework's event system
and the provider forwards it:

```php
use Eyika\Atom\Reverb\Contracts\ShouldBroadcast;

class OrderShipped implements ShouldBroadcast
{
    public function __construct(private int $id) {}
    public function broadcastOn(): string { return 'orders'; }
    public function broadcastAs(): string { return 'OrderShipped'; }
    public function broadcastWith(): array { return ['id' => $this->id]; }
}

event(new OrderShipped(42)); // → reaches every subscriber of 'orders'
```

## Connect from the browser

```js
const ws = new WebSocket('ws://127.0.0.1:8091');
ws.onopen = () => ws.send(JSON.stringify({ event: 'subscribe', data: { channel: 'orders' } }));
ws.onmessage = (e) => {
  const { event, channel, data } = JSON.parse(e.data);
  console.log(event, channel, data); // OrderShipped orders {id: 42}
};
```

## Architecture

```
browser ──ws──► Server (stream_select loop)          app (PHP-FPM)
  subscribe ───► ChannelManager: conn ↔ channels
                                                        Broadcast::send(...)
  ◄── frame ◄─── broadcast(channel,event,data) ◄──http─── POST /broadcast (ingest port)
```

- **`Protocol\Handshake`** — RFC 6455 opening handshake (with the spec's test vectors).
- **`Protocol\Frame`** — RFC 6455 framing: masked client→server decode, unmasked
  server→client encode, control opcodes.
- **`ChannelManager`** — pure connection ↔ channel bookkeeping (unit-tested in isolation).
- **`Server`** — the `stream_select` loop over the WS + ingest listeners.
- **`Broadcasting\BroadcastManager`** — app side; POSTs events to the ingest port.

Everything except the socket loop is pure and unit-tested (see the framework's
`ReverbProtocolTest`); the full loop is validated by a live socket smoke.

## License

MIT
