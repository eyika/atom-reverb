# Atom Reverb

> 📖 **Documentation:** the canonical guide for this package lives in the Atom docs —
> **[Official Packages → atom-reverb](https://basttyydev.serv00.net/docs/beta/packages#atom-reverb)**.
> This README is a quick reference; the docs cover channel auth, presence, and the Redis backplane in full.

A **production WebSocket broadcast server** for the [Atom framework](https://github.com/eyika/atomframework) —
Pusher-protocol compatible, dependency-free (built on `stream_socket_server` + `stream_select`),
and hardened for real deployments:

- **Private / presence channel authorisation** (HMAC), presence membership + member events
- **Authenticated broadcast ingest** (HMAC-signed app → server)
- **Non-blocking writes with back-pressure**, ping/pong heartbeat + idle timeouts, fragmented-message reassembly
- **Horizontal scaling** via a Redis pub/sub backplane
- **Opt-in native TLS** (`wss://`) — a reverse proxy remains the recommended default

Because it speaks the Pusher protocol, standard **`pusher-js`** clients (and Laravel Echo) connect to it.

## Install

```bash
composer require eyika/atom-reverb
php artisan vendor:publish --tag=reverb-config   # config/reverb.php
```

Auto-discovered via `extra.atom.providers`. Set credentials in `.env`:

```dotenv
REVERB_APP_KEY=your-app-key
REVERB_APP_SECRET=a-long-random-secret   # signs channel auth + ingest — REQUIRED in production
REVERB_PORT=8091                         # → config key `ws_port`
REVERB_INGEST_PORT=8092                  # → config key `ingest_port`
```

**Publish the config rather than hand-writing it.** The config keys are not named after the env
vars: `REVERB_PORT` feeds **`ws_port`**, and Redis settings live under a **nested** `redis` array,
not a flat boolean.

```php
// config/reverb.php — the shape the package actually reads
'host'        => env('REVERB_HOST', '127.0.0.1'),
'ws_port'     => (int) env('REVERB_PORT', 8091),
'ingest_port' => (int) env('REVERB_INGEST_PORT', 8092),

'redis' => [
    'enabled'  => (bool) env('REVERB_REDIS', false),
    'host'     => env('REVERB_REDIS_HOST', '127.0.0.1'),
    'port'     => (int) env('REVERB_REDIS_PORT', 6379),
    'password' => env('REVERB_REDIS_PASSWORD', null),
    'channel'  => env('REVERB_REDIS_CHANNEL', 'atom-reverb'),
],
```

A config written as `'port' => …` or `'redis' => true` is simply never read. That is easy to miss,
because `ws_port` defaults to 8091 too — so the mismatch only shows itself once someone overrides
`REVERB_PORT`, and then it looks like the override is broken rather than the key.

## Run

```bash
php artisan reverb:start
php artisan reverb:start --host=0.0.0.0 --ws-port=8091 --ingest-port=8092
```

### Two ports, and why there is no `--port`

The server binds **two** sockets, and they are a security boundary rather than tidiness:

| Port | Option | Who reaches it |
|---|---|---|
| WebSocket | `--ws-port` | the public edge — browsers connect here |
| Ingest | `--ingest-port` | **your app servers only** — this is where broadcasts are published |

**Anyone who can reach the ingest port can publish to any channel, including another tenant's
private ones.** Keep it on the private network, firewalled to the app servers.

There is deliberately no single `--port`, and passing one is **silently ignored** — the daemon binds
its defaults, so everything downstream can end up pointing at a port nothing serves. Always pass the
two explicitly, or set them in config.

Run it under a process supervisor (systemd / supervisor). Put nginx/Caddy in front for TLS and to
expose only the WS port publicly.

## Connect from the browser

```js
import Pusher from 'pusher-js';

const pusher = new Pusher('your-app-key', {
  wsHost: 'your-host', wsPort: 8091, forceTLS: false, enabledTransports: ['ws'],
  // private/presence channels call your app's auth endpoint:
  authEndpoint: '/broadcasting/auth',
});

pusher.subscribe('orders').bind('OrderShipped', (data) => console.log(data));
const presence = pusher.subscribe('presence-room');
presence.bind('pusher:subscription_succeeded', (members) => console.log(members.count));
```

## Broadcast from your app

```php
use Eyika\Atom\Reverb\Support\Broadcast;

Broadcast::send('orders', 'OrderShipped', ['id' => 42]);   // or broadcast('orders', 'OrderShipped', [...])
```

Or dispatch a `ShouldBroadcast` event through the framework's event system and the provider forwards it:

```php
class OrderShipped implements \Eyika\Atom\Reverb\Contracts\ShouldBroadcast
{
    public function __construct(private int $id) {}
    public function broadcastOn(): string { return 'orders'; }
    public function broadcastAs(): string { return 'OrderShipped'; }
    public function broadcastWith(): array { return ['id' => $this->id]; }
}

event(new OrderShipped(42));
```

Every broadcast the app POSTs is **HMAC-signed** with `REVERB_APP_SECRET`; the server rejects unsigned
ingests (`401`) when a secret is configured.

## Private & presence channels

Channels prefixed `private-` / `presence-` require authorisation. Add a broadcasting-auth endpoint that,
after checking the logged-in user, returns the signed payload:

```php
// POST /broadcasting/auth  { socket_id, channel_name }
use Eyika\Atom\Reverb\Broadcasting\BroadcastManager;

$auth = app(BroadcastManager::class)->channelAuth(
    $request->input('socket_id'),
    $request->input('channel_name'),
    // presence only — the member payload:
    ['user_id' => $user->id, 'user_info' => ['name' => $user->name]]
);

return JsonResponse::ok('', $auth);   // { auth: "key:hmac", channel_data?: "..." }
```

The server verifies the signature against the connection's socket id before allowing the subscription, then
(for presence) tracks members and emits `member_added` / `member_removed`.

## Scale out (Redis backplane)

Run several Reverb nodes behind a load balancer and enable Redis so a broadcast on one node reaches clients
on all nodes:

```dotenv
REVERB_REDIS=true
REVERB_REDIS_HOST=127.0.0.1
REVERB_REDIS_PORT=6379
```

Each node fans a broadcast out to its own connections and relays it to peers via Redis pub/sub (a minimal
built-in RESP client — no `ext-redis`/predis needed).

**Cross-node presence is aggregated too**: with Redis enabled, presence membership lives in Redis (hashes +
Lua-atomic reference counting), so `subscription_succeeded` reports the *global* member list and
`member_added`/`member_removed` propagate across nodes — deduplicated per `user_id` (a user with several
connections is one member). A liveness heartbeat + reaper cleans up members left behind by a **crashed**
node. (Without Redis, presence is correct but single-node.) The distributed logic needs a real Redis cluster
to validate under load; the reference-counting semantics are unit-tested.

## TLS

Terminate `wss://` at a reverse proxy (recommended), or opt into native TLS:

```dotenv
REVERB_TLS=true
REVERB_TLS_CERT=/path/fullchain.pem
REVERB_TLS_KEY=/path/privkey.pem
```

## Architecture

- **`Server`** — the `stream_select` loop over the WS + ingest listeners (+ the backplane socket), with
  back-pressure, heartbeat, reassembly, auth, presence, and fan-out.
- **`Connection`** — per-connection read/write buffers, fragment assembly, heartbeat + socket id.
- **`Protocol\Handshake` / `Protocol\Frame`** — RFC 6455 handshake + framing (fin-aware).
- **`Auth\Signature`** — Pusher-style HMAC for channels + ingest.
- **`ChannelManager`** — channel subscriptions (who to deliver to).
- **`Presence\{Local,Redis}PresenceStore`** — presence membership (who is present), single-node vs
  Redis-aggregated with Lua-atomic dedup + a crashed-node reaper.
- **`Backplane\{Local,Redis}Backplane`** + **`Redis\RedisClient`** — single-node vs Redis-clustered
  fan-out (pub/sub) and the blocking command client the presence store uses.
- **`Broadcasting\BroadcastManager`** — app side: signed ingest + `channelAuth()` for the auth endpoint.

## License

MIT
