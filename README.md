# Mercure Redis Transport

A [Redis](https://redis.io) transport for the [Mercure](https://mercure.rocks) hub, packaged as a
[Caddy](https://caddyserver.com) module.

The open-source Mercure hub ships single-process transports (`local`, `bolt`), so a horizontally
scaled hub cannot deliver an update published on one instance to subscribers connected to another.
This module fixes that. Redis Streams hold the event history, and Redis Pub/Sub fans out to the
other instances.

- Durable, size-capped event history in a Redis Stream
- O(1) `Last-Event-ID` replay via a UUID → stream-ID index (never linear-scans)
- Gap recovery after a Pub/Sub disconnect
- `history_limit` bounds the work a client can trigger per reconnect

Requires Go 1.26+, Redis 5.0+, Mercure 0.22 / Caddy 2.11.

## Install

Build Caddy with [xcaddy](https://github.com/caddyserver/xcaddy):

```bash
xcaddy build \
    --with github.com/dunglas/mercure/caddy \
    --with github.com/desk-ly/mercure-transport-redis
```

## Configure

```caddy
localhost {
    route {
        mercure {
            transport redis {
                url {$REDIS_URL}
                stream mercure
                size 10000
                history_limit 1000
            }

            publisher_jwt {$MERCURE_PUBLISHER_JWT_KEY}
            subscriber_jwt {$MERCURE_SUBSCRIBER_JWT_KEY}
        }

        respond 404
    }
}
```

| Parameter | Default | Description |
|---|---|---|
| `url` | *(required)* | Redis URL, e.g. `redis://host:6379/0` or `rediss://` for TLS. Parsed by [go-redis `ParseURL`](https://pkg.go.dev/github.com/redis/go-redis/v9#ParseURL), so it also carries credentials and DB index. |
| `stream` | `mercure` | Stream key. Also derives the Pub/Sub channel (`<stream>:pubsub`) and index hash (`<stream>:index`). |
| `size` | `0` | Max stream entries, enforced by a background `XTRIM` every 10s. **`0` means no trimming, so the stream grows without bound.** Set this. |
| `history_limit` | `1000` | Max entries read per SSE history replay. `0` disables replay, so subscribers fast-forward to live. |

### Sizing

Pick a value that covers your longest expected client disconnect at your peak event rate. There is no
TTL and no eviction fallback, so `size` is the only bound on Redis memory.

`Last-Event-ID` is client-controlled, so `history_limit` caps what a single reconnect can cost. Set it
to `0` if your clients re-sync through your own API on connect, which makes reconnects constant-time.

### Security

The transport trusts its Redis. Anything that can write to `<stream>:pubsub` can inject updates
without a publisher JWT, and anything that can read the stream sees every update in plain text.
Subscriber authorization still applies, so an injected update only reaches subscribers whose topic
selectors already match it, but its topics and content are attacker-controlled.

Run Redis with AUTH/ACLs and TLS, and do not share the instance with untrusted workloads.

## Limitations

- All topics share one stream, and cross-instance replay filters locally rather than at the stream
  level.
- `history_limit` bounds the client-facing path only. Internal gap recovery after a Pub/Sub outage may
  dispatch up to `size` events at once. The anchor is server-controlled, so this is not
  client-triggerable.
- The transport cannot signal Mercure that Redis is unreachable. While Pub/Sub is down, local delivery
  continues but instances diverge until reconnect and replay.
- Pub/Sub delivery is best-effort. Consistency comes from the stream plus replay, not the channel.

## Development

```bash
make test
```

Runs in Docker, so no local Go toolchain is required. Without Docker, `go test -race -count=1 ./...`
does the same. Tests run against [miniredis](https://github.com/alicebob/miniredis), so no Redis
instance is needed.

## Status

Developed at [desk.ly](https://desk.ly) and published under AGPL-3.0. It is covered by an extensive
test suite but is not yet running in production, so treat the configuration surface as unstable.
Issues and pull requests are welcome, but we cannot commit to a support or release schedule.

## License

Copyright © 2026 desk.ly GmbH. Licensed under [AGPL-3.0-or-later](./LICENSE).

This module links against the [Mercure hub](https://github.com/dunglas/mercure), which is AGPL-3.0.
A Caddy binary built with it is therefore a combined AGPL-3.0 work. If you run it as a network
service, AGPL-3.0 §13 requires you to offer its complete corresponding source to your users.
