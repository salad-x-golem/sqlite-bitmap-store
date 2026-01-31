# SQLite Bitmap Store

A Go-based system for efficiently storing, indexing, and querying blockchain event data from the Arkiv Network. Uses SQLite for persistence and Roaring Bitmaps for fast attribute-based filtering.

## Features

- **Bitmap Indexing**: Uses Roaring Bitmap compression for memory-efficient attribute indexes
- **Custom Query Language**: Boolean expressions with comparisons, glob patterns, and set operations
- **Blockchain Event Processing**: Handles Create, Update, Delete, Expire, ExtendBTL, and ChangeOwner operations
- **Synthetic Attributes**: Automatic indexing of `$owner`, `$creator`, `$key`, `$expiration`, `$createdAtBlock`, `$sequence`, `$txIndex`, `$opIndex`
- **WAL Mode**: Write-Ahead Logging for reliability and concurrent reads


## Usage

### Loading Data

Load blockchain events from a TAR archive:

```bash
go run ./cmd/load-from-tar --db-path arkiv-data.db <tar-file>

# Or using environment variable
DB_PATH=arkiv-data.db go run ./cmd/load-from-tar <tar-file>
```

### Querying

Query the database with filter expressions:

```bash
go run ./cmd/query --db-path arkiv-data.db 'type = "thing"'
go run ./cmd/query --db-path arkiv-data.db '$owner = "0x1234..."'
```

## Query Language

### Operators

| Operator | Description |
|----------|-------------|
| `&&` | Logical AND |
| `\|\|` | Logical OR |
| `!` | Logical NOT |
| `=`, `!=` | Equality comparison |
| `<`, `>`, `<=`, `>=` | Numeric comparison |
| `~` | Glob pattern match |
| `!~` | Glob pattern not match |

### Special Attributes

| Attribute | Description |
|-----------|-------------|
| `$owner` | Entity owner address |
| `$creator` | Entity creator address |
| `$key` | Entity key |
| `$expiration` | Expiration block number |
| `$sequence` | Sequence number |
| `$all` | Match all entities |
| `*` | Wildcard (match all) |

### Examples

```
type = "nft" && status = "active"
$owner = "0xabc..." || $creator = "0xabc..."
name ~ "test*" && !(status = "deleted")
price >= 100 && price <= 1000
```

## Database Schema

Four main tables:

- **payloads**: Entity data with key, owner, creator, expiration, and JSON payload
- **last_block**: Tracks the last processed block number
- **string_attributes_values_bitmaps**: Bitmap indexes for string attributes
- **numeric_attributes_values_bitmaps**: Bitmap indexes for numeric attributes

## Dependencies

| Package | Purpose |
|---------|---------|
| [roaring](https://github.com/RoaringBitmap/roaring) | Bitmap compression and operations |
| [participle/v2](https://github.com/alecthomas/participle) | Query language parser |
| [go-sqlite3](https://github.com/mattn/go-sqlite3) | SQLite driver |
| [golang-migrate](https://github.com/golang-migrate/migrate) | Database migrations |
| [arkiv-events](https://github.com/salad-x-golem/arkiv-events) | Blockchain event structures |
| [go-ethereum](https://github.com/ethereum/go-ethereum) | Address and Hash types |
| [urfave/cli](https://github.com/urfave/cli) | CLI framework |
| [sqlc](https://sqlc.dev) | Type-safe SQL code generation |

## Development

Uses Nix flakes for reproducible development:

```bash
# Enter development shell
nix develop

# Or with direnv
direnv allow
```

Regenerate SQL code:

```bash
go generate ./...
```
