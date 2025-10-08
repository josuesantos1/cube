# Cube

A high-performance, sharded key-value database with ACID transactions built in Elixir.

## Features

- **Sharded Storage:** 20 parallel shards for concurrent access
- **ACID Transactions:** Full support for BEGIN, COMMIT, ROLLBACK
- **MVCC (Multi-Version Concurrency Control)**: Snapshot isolation
- **Bloom Filters:** Read performance optimization
- **WAL (Write-Ahead Logging):** Data durability
- **HTTP API:** RESTful interface via Bandit/Plug
- **First-committer-wins:** Conflict detection
- **LTTLV Format:** Storage format (Length-Type-Tag-Length-Value)


## Quick Start

### Installation

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

- 20 shards using consistent hashing (`erlang.phash2/2`)
- Keys distributed via hexadecimal encoding
- Parallel reads/writes across shards

### Storage Format

LTTLV (Length-Type-Tag-Length-Value):
```
[3 hex][key hex][1 type][4 hex value length][value hex]
```

### Durability

- WAL logs all writes before applying
- fsync on every write
- WAL replay on startup
- Cleared after successful persistence

## Testing

```bash
# Run all tests
mix test

# Run with coverage
mix test --cover
```
