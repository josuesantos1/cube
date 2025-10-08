# Cube

A high-performance, sharded key-value database with ACID transactions built in Elixir.

## Features

- **Sharded Storage:** 20 parallel shards for concurrent access
- **ACID Transactions:** Full support for BEGIN, COMMIT, ROLLBACK
- **MVCC (Multi-Version Concurrency Control)**: Snapshot isolation
- **In-Memory Cache:** Fast read performance with write-through cache
- **Bloom Filters:** Read performance optimization
- **WAL (Write-Ahead Logging):** Data durability
- **HTTP API:** RESTful interface via Bandit/Plug
- **First-committer-wins:** Conflict detection
- **LTTLV Format:** Storage format (Length-Type-Tag-Length-Value)
- **Docker Support:** Containerized deployment with persistent storage


## Quick Start

### Docker (Recommended)

```bash
# Start with Docker Compose
docker compose up -d

# View logs
docker compose logs -f cube

# Stop
docker compose down

# Stop and remove data
docker compose down -v
```

### Local Installation

```bash
# Install dependencies
mix deps.get

# Run tests
mix test

# Start server
mix run --no-halt
```

### Usage

The Cube runs on `http://localhost:4000` and uses the `X-Client-Name` header to identify clients.

#### SET - Store a value

```bash
curl -X POST http://localhost:4000 \
  -H "X-Client-Name: alice" \
  -d "SET name \"Alice\""
# Response: NIL Alice
```

#### GET - Retrieve a value

```bash
curl -X POST http://localhost:4000 \
  -H "X-Client-Name: alice" \
  -d "GET name"
# Response: Alice
```

#### Transactions

```bash
# Begin transaction
curl -X POST http://localhost:4000 \
  -H "X-Client-Name: alice" \
  -d "BEGIN"
# Response: OK

# Read value (snapshot at BEGIN timestamp)
curl -X POST http://localhost:4000 \
  -H "X-Client-Name: alice" \
  -d "GET balance"
# Response: 100

# Write value (buffered in transaction)
curl -X POST http://localhost:4000 \
  -H "X-Client-Name: alice" \
  -d "SET balance 150"
# Response: 100 150

# Commit transaction (with conflict detection)
curl -X POST http://localhost:4000 \
  -H "X-Client-Name: alice" \
  -d "COMMIT"
# Response: OK (or ERR Atomicity failure if conflict detected)

# Rollback transaction
curl -X POST http://localhost:4000 \
  -H "X-Client-Name: alice" \
  -d "ROLLBACK"
# Response: OK
```

## Data Types

Supports 4 data types:

- **String**: `"hello world"`
- **Integer**: `42`, `-10`
- **Boolean**: `true`, `false`
- **Nil**: `nil`

## Transaction Semantics

### Snapshot Isolation

All reads within a transaction see a consistent snapshot from the BEGIN timestamp:

```bash
# Client A
BEGIN
GET x  # Returns: NIL

# Client B (concurrent)
SET x 1

# Client A (still sees snapshot)
GET x  # Returns: NIL (not 1)
COMMIT # OK
```

## Technical Details

### MVCC Implementation

- Each shard maintains version history (last 100 versions per key)
- Transactions capture BEGIN timestamp
- Reads use timestamp to fetch correct version
- First-committer-wins on conflicts

### Sharding

- 20 shards using consistent hashing 
- Keys distributed via hexadecimal encoding
- Parallel reads/writes across shards

### Storage Format

LTTLV (Length-Type-Tag-Length-Value):
```
[3 hex][key hex][1 type][4 hex value length][value hex]
```

### Durability

- WAL logs all writes before applying
- WAL replay on startup
- Cleared after successful persistence

### Performance Optimizations

- **In-Memory Cache:** GenServer-based caching layer for frequently accessed keys
- **Streaming I/O:** Uses `File.stream!` for lazy evaluation, avoiding full file loads
- **Write Optimization:** Append-only for new keys, in-place updates for existing keys
- **Cache Invalidation:** Write-through cache ensures consistency

## Docker Deployment

### Environment Variables

- `PORT` - HTTP server port (default: 4000)
- `DATA_DIR` - Directory for persistent storage (default: current directory)
- `MIX_ENV` - Elixir environment (production/development)

### Volume Mounts

The Docker setup uses a named volume `cube_data` mounted at `/app/data` to persist:
- Shard data files (`shard_XX_data.txt`)
- WAL files (`wal_shard_XX.log`)

### Building from Source

```bash
# Build image
docker build -t cube:latest .

# Run with custom data directory
docker run -d \
  -p 4000:4000 \
  -v $(pwd)/data:/app/data \
  -e DATA_DIR=/app/data \
  cube:latest
```

## Testing

```bash
# Run all tests
mix test

# Run with coverage
mix test --cover
```
