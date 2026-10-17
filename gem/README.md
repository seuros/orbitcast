# orbitcast (Ruby gem)

Rails integration for the Orbitcast relay. The gem ships the Ruby side plus one
native executable per platform; the relay itself is a separately supervised Rust
process.

Two modes. In `mode: :signed` (default) the relay authorizes subscriptions
from signed stream names and no application Ruby runs on the socket path.
In `mode: :dispatch` the relay calls back into Rails over a private HTTP
endpoint, and `ApplicationCable::Connection#connect`, channel callbacks,
and `perform` actions run here as ordinary Rails code.

## What is in here

| Path | Role |
| --- | --- |
| `exe/orbitcast` | wrapper that execs the vendored binary |
| `lib/orbitcast/launcher.rb` | argv/env handling, single Rails boot for `serve` |
| `lib/orbitcast/binary.rb` | vendored binary resolution, `ORBITCAST_BINARY` override |
| `lib/orbitcast/configuration.rb` | validated settings |
| `lib/orbitcast/streams.rb` | `Orbitcast.signed_stream_name` |
| `lib/orbitcast/recovery.rb` | `Orbitcast.recovery_token` (opt-in retained replay) |
| `lib/orbitcast/publisher/postgres.rb` | pooled `pg_notify` publisher |
| `lib/orbitcast/publisher/http.rb` | private-ingress publisher (`publish_via: :http`) |
| `lib/orbitcast/publisher/instrumentation.rb` | `publish.orbitcast` notifications for both publishers |
| `lib/orbitcast/publisher/control.rb` | control-message publisher (`Orbitcast.disconnect`) |
| `lib/action_cable/subscription_adapter/orbitcast.rb` | publish-only cable adapter |
| `lib/orbitcast/connection.rb`, `lib/orbitcast/channel.rb` | dispatch-mode `ApplicationCable` bases |
| `lib/orbitcast/rails_endpoint.rb` | the private Rack endpoint the relay calls (dispatch mode) |
| `lib/orbitcast/engine.rb` | `config.orbitcast.*`, `db/orbitcast_migrate` migration path, derived stream key, mounts the RPC endpoint in dispatch mode; never launches the relay |
| `lib/generators/orbitcast/install/` | `bin/rails generate orbitcast:install` |
| `db/orbitcast_migrate/` | overflow, presence, and broker migrations |

## Install

```bash
bin/rails generate orbitcast:install            # or --overflow
```

The installer applies, idempotently: `config/cable.yml` (`adapter: orbitcast`),
`config/initializers/orbitcast.rb`, the four cable lines in
`config/environments/{development,production}.rb` (`mount_path = nil`, `url`,
`allowed_request_origins`, `config.orbitcast.mode = :signed` — existing values
kept), `action_cable_meta_tag` in the layout `<head>`, and a `cable:` entry in
`Procfile.dev`. Importmap pins and Solid Queue/Cache settings are untouched.
`--overflow` also copies the overflow migration (picked up by `bin/rails
db:migrate` through the engine) and sets `overflow_enabled: true`.

```yaml
# config/cable.yml
production:
  adapter: orbitcast
  database_url: <%= ENV.fetch("ORBITCAST_DATABASE_URL") %>
  namespace: "example_production"
  publish_via: postgres      # or http (ingress_url + ingress_token)
  connection: pool           # or active_record: join the app transaction
```

```text
# Procfile.dev
cable: bundle exec orbitcast serve
```

`bundle exec orbitcast --version` and `--help` run the binary directly, without
booting Rails. `serve` (also a bare `orbitcast`, flags understood) inside an
application boots Rails once, exports an allowlisted `ORBITCAST_*` environment
(secrets as strict base64, never argv; `RAILS_MASTER_KEY`/`SECRET_KEY_BASE`
never), and replaces the Ruby process with the relay. The listener defaults to
`127.0.0.1:28080` in development/test and must be configured elsewhere
(`listen_addr` or `ORBITCAST_LISTEN_ADDR`).

Rails-side keys beyond the basics: `listener_database_url`, `listen_addr`,
`trusted_proxies`, `outbound_queue_messages/bytes`,
`streams_previous_secrets`, `turbo_previous_secrets`, `connection`. Without an
explicit `streams_secret` the stream key is derived from
`Rails.application.key_generator` under `orbitcast/streams/v1/<namespace>`.
Every publish emits `publish.orbitcast` through `ActiveSupport::Notifications`
(`stream:`, `bytes:`, `via:`), failures included.

## Platforms

`arm64-darwin`, `x86_64-darwin`, `x86_64-linux-gnu`, `aarch64-linux-gnu`,
`x86_64-linux-musl`. On anything else, build the crate and point
`ORBITCAST_BINARY` at the result; the gem never downloads or compiles a binary.
An override is checked with `<binary> doctor --json` before `serve` uses it:
the reported `version` must equal the gem's, `features` must include
`postgres` when publishing through PostgreSQL, and `envelope_versions` must
include `1` — otherwise `Orbitcast::BinaryError` says exactly what is wrong.

## Tests

No database and no Rails boot required:

```bash
ruby -Ilib -Itest test/configuration_test.rb   # etc.
```

## License

BSD-3-Clause. See `LICENSE.txt`.
