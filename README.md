# MirseoDB

MirseoDB is a high-performance, lightweight database system written in Rust with built-in AnySQL HYPERTHINKING engine that automatically detects and supports multiple SQL dialects.

**Warning**: This project is in early development, expect breaking changes.

## Features

- **AnySQL HYPERTHINKING Engine**: Automatically detects SQL dialects (Standard SQL, MS-SQL, MySQL/MariaDB, Oracle) without requiring users to specify the dialect
- **Bloom Filter Optimization**: Column-based row skipping for improved query performance
- **Composite Indexing**: Multi-column index support with query optimization
- **Chunked Table Scanning**: Memory-efficient processing with early termination support
- **Advanced Security**: SQL injection protection, two-factor authentication, and API token validation
- **Web Console**: Modern SvelteKit-based administration interface
- **Route Forwarding**: Built-in request routing and forwarding capabilities

## Quick Start

### Prerequisites

- Rust toolchain (edition 2021)
- Node.js 18+ with npm

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/mirseodb.git
   cd mirseodb
   ```

2. Install console dependencies:
   ```bash
   cd console
   npm install
   cd ..
   ```

3. Run the server:
   ```bash
   cargo run
   ```

The server will start on `http://127.0.0.1:3306/` with the web console proxied. The underlying SvelteKit dev server runs on `http://localhost:5173`.

### Environment Variables

- `MIRSEODB_SKIP_CONSOLE=1`: Disable web console startup
- `MIRSEODB_API_TOKEN`: Set API authentication token
- `SQL_INJECTION_PROTECT=1`: Enable SQL injection protection

## API Usage

### Authentication

All API requests require authentication via the `Authorization` header:

```bash
curl -X POST http://127.0.0.1:3306/query \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users"}'
```

### Query Endpoints

- `POST /query`: Execute SQL queries (JSON format)
- `GET /query?sql=SELECT * FROM users`: Execute SQL queries (URL parameter)
- `POST /api/query`: Alternative query endpoint
- `GET /health`: Health check endpoint
- `GET /api/health`: Alternative health check endpoint

### Request Format

```json
{
  "sql": "CREATE TABLE users (id INTEGER, name TEXT)",
  "auth_token": "optional_token_override",
  "totp_token": "optional_2fa_token",
  "email": "optional_user_email"
}
```

### Response Format

```json
{
  "success": true,
  "data": [...],
  "execution_time_ms": 25,
  "rows_affected": 1
}
```

## Security Features

### Authentication Failure Behavior

When authentication fails on `/query` endpoints, the server returns random HTTP error codes (404, 403, 502, 500) to obscure the API's existence from unauthorized users.

### SQL Injection Protection

Enable with `SQL_INJECTION_PROTECT=1`. The system automatically sanitizes suspicious SQL patterns.

### Two-Factor Authentication

Configure 2FA for sensitive operations like DROP TABLE, DROP DATABASE, and bulk DELETE/UPDATE operations.

## Architecture

### Core Components

- **Engine** (`src/engine.rs`): Main database engine with CRUD operations
- **AnySQL Parser** (`src/smart_parser.rs`): Intelligent SQL dialect detection and parsing
- **Bloom Filters** (`src/bloom_filter.rs`): Column-based filtering and chunked scanning
- **Indexing** (`src/indexing.rs`): Composite indexing and query optimization
- **Security** (`src/auth.rs`, `src/two_factor_auth.rs`): Authentication and security features
- **Server** (`src/server.rs`): HTTP server and API endpoints
- **Persistence** (`src/persistence.rs`): File-based storage engine

### Data Flow

1. SQL Input → AnySQL Parser → SqlStatement enum
2. SqlStatement → Database.execute() → Storage operations
3. Results → Formatted output

### Storage

- Database files: `.mirseoDB/*.mdb` (binary serialized format)
- In-memory operations with periodic disk persistence
- Automatic backup and recovery

## Development

### Building

```bash
cargo build --release
```

### Testing

```bash
cargo test
```

### Development Mode

```bash
cargo run
```

### Disable Console

```bash
MIRSEODB_SKIP_CONSOLE=1 cargo run
```

## Performance Optimizations

### Recent Optimizations

1. **Parser Optimizations**:
   - Dialect caching with LRU eviction
   - Keyword hash matching with weighted detection
   - Performance metrics tracking

2. **Query Optimizations**:
   - Composite index support for multi-column queries
   - WHERE clause analysis for optimal index selection
   - Index hint system (USE, FORCE, IGNORE)

3. **Table Scan Optimizations**:
   - Bloom filters for column-based row skipping
   - Chunk-based processing for memory efficiency
   - Early termination with LIMIT support

### Benchmark Results

The optimizations provide significant performance improvements:
- Parser: 90% reduction in repeated SQL parsing overhead
- Indexing: Up to 10x faster multi-column queries
- Scanning: 50-80% reduction in unnecessary row processing

## Documentation

Comprehensive documentation is available in the `docs/` directory:

- `docs/en/`: English documentation
- `docs/ko/`: Korean documentation

Each module has detailed documentation covering:
- API reference
- Implementation details
- Usage examples
- Performance considerations

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For questions, issues, or contributions, please visit the GitHub repository or create an issue.