# OrbitCast

ActionCable WebSocket server for [Mothership](https://github.com/seuros/mothership). Runs as a bay using the docking protocol.

## What It Does

OrbitCast handles ActionCable WebSocket connections, multiplexed through Mothership's docking protocol. Multiple client connections share a single Unix socket.

```
Clients (WS) → Mothership → Unix Socket → OrbitCast
                                              ↓
                                         Pub/Sub
                                    (PostgreSQL or Memory)
```

## Installation

```bash
# Production (multi-node coordination via PostgreSQL)
cargo install orbitcast --features postgres

# Development (single-node, in-memory)
cargo install orbitcast --features memory
```

> Exactly one backend required. Mutually exclusive at compile time.

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

## License

MIT
