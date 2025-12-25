# OrbitCast

ActionCable WebSocket server for [Mothership](https://github.com/seuros/mothership). Runs as a bay using the docking protocol.

## What It Does

OrbitCast handles ActionCable WebSocket connections, multiplexed through Mothership's docking protocol. Multiple client connections share a single Unix socket.

```
Clients (WS) → Mothership → Unix Socket → OrbitCast
                                              ↓
                                          RPC
                                     (anycable-rails)
                                              ↓
                                         Pub/Sub
                                    (PostgreSQL or Memory)
```

## Installation

```bash
# Production (multi-node coordination via PostgreSQL)
cargo install orbitcast
# Or explicitly:
# cargo install orbitcast --features postgres

# Development (single-node, in-memory)
cargo install orbitcast --no-default-features --features memory
```

> Exactly one backend is required. Use `--no-default-features` to disable the default PostgreSQL backend when using `memory`.

## Configuration

Add to your `ship-manifest.toml`:

```toml
[[bays.websocket]]
name = "orbitcast"
command = "orbitcast"
routes = [{ bind = "ws", pattern = "/cable" }]
config = { database_url = "postgres://localhost/myapp" }
```

### Config Options

| Key | Description |
|-----|-------------|
| `database_url` | PostgreSQL connection (required for `postgres` feature) |
| `ping_interval` | Seconds between pings (default: 3) |
| `rpc_host` | AnyCable RPC host (default: `127.0.0.1:50051`) |
| `rpc_request_timeout_ms` | RPC request timeout in ms (optional) |
| `rpc_headers` | Comma-separated header allowlist (default: `cookie`, `*` for all) |

### Environment Variables

Set automatically by Mothership:

| Variable | Description |
|----------|-------------|
| `MS_PID` | Mothership process ID |
| `MS_SHIP` | Bay name |
| `MS_SOCKET_DIR` | Unix socket directory |

## Pub/Sub Backends

### PostgreSQL (`--features postgres`)

Uses `LISTEN/NOTIFY` for cross-node broadcasting. Required for multi-instance deployments.

#### TLS / `sslmode`

OrbitCast honors the `sslmode` query param in `database_url`:

- `disable`: non-TLS only
- `allow`: try non-TLS, then TLS
- `prefer` (default): try TLS, then non-TLS
- `require`: TLS only
- `verify-ca`, `verify-full`: TLS only (certificate verification uses system roots)

### Memory (`--features memory`)

Uses `tokio::sync::broadcast`. Single process only. Good for development.

**Limitations:**
- No cross-node coordination
- No persistence
- Lagging receivers lose messages

## CLI

```bash
orbitcast --help
orbitcast --log-level debug
```

## AnyCable RPC

OrbitCast speaks the AnyCable RPC protocol. Run the `anycable-rails` RPC server and
point OrbitCast to it via `rpc_host`.

Example ship config:

```toml
[[bays.websocket]]
name = "orbitcast"
command = "orbitcast"
routes = [{ bind = "ws", pattern = "/cable" }]
config = { rpc_host = "127.0.0.1:50051", database_url = "postgres://localhost/myapp" }
```

## License

MIT
