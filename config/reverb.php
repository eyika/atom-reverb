<?php

return [

    /*
    |--------------------------------------------------------------------------
    | Bind Address + Ports
    |--------------------------------------------------------------------------
    |
    | The WebSocket port clients connect to, and the (localhost) ingest port the
    | application POSTs broadcasts to. Keep the ingest port firewalled off from the
    | public internet.
    |
    */

    'host'        => env('REVERB_HOST', '127.0.0.1'),
    'ws_port'     => (int) env('REVERB_PORT', 8091),
    'ingest_port' => (int) env('REVERB_INGEST_PORT', 8092),

    /*
    |--------------------------------------------------------------------------
    | Application Credentials
    |--------------------------------------------------------------------------
    |
    | The app key is public (clients use it); the secret signs private/presence
    | channel subscriptions and broadcast ingests. Set a strong REVERB_APP_SECRET
    | in production — an empty secret disables auth (development only).
    |
    */

    'app_key'    => env('REVERB_APP_KEY', 'atom'),
    'app_secret' => env('REVERB_APP_SECRET', ''),

    /*
    |--------------------------------------------------------------------------
    | Connection Management
    |--------------------------------------------------------------------------
    */

    'max_connections'    => (int) env('REVERB_MAX_CONNECTIONS', 10000),
    'heartbeat_interval' => (int) env('REVERB_HEARTBEAT', 30),   // seconds
    'idle_timeout'       => (int) env('REVERB_IDLE_TIMEOUT', 120),

    /*
    |--------------------------------------------------------------------------
    | Native TLS (opt-in)
    |--------------------------------------------------------------------------
    |
    | Terminate wss:// in the server itself. Off by default — a reverse proxy
    | (nginx/Caddy) in front is the recommended way to do TLS at scale.
    |
    */

    'tls' => [
        'enabled'           => (bool) env('REVERB_TLS', false),
        'cert'              => env('REVERB_TLS_CERT', ''),
        'key'               => env('REVERB_TLS_KEY', ''),
        'allow_self_signed' => (bool) env('REVERB_TLS_SELF_SIGNED', false),
    ],

    /*
    |--------------------------------------------------------------------------
    | Redis Backplane (horizontal scaling)
    |--------------------------------------------------------------------------
    |
    | Enable to run multiple Reverb nodes behind a load balancer: each node fans a
    | broadcast out to its own connections and relays it to peers via Redis pub/sub.
    |
    */

    'redis' => [
        'enabled'  => (bool) env('REVERB_REDIS', false),
        'host'     => env('REVERB_REDIS_HOST', '127.0.0.1'),
        'port'     => (int) env('REVERB_REDIS_PORT', 6379),
        'password' => env('REVERB_REDIS_PASSWORD', null),
        'channel'  => env('REVERB_REDIS_CHANNEL', 'atom-reverb'),
    ],

];
