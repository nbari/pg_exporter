# Vacuum Collector Tests

Tests for the vacuum collector group, which includes:
- **Stats**: Freeze age and autovacuum worker tracking
- **Progress**: Real-time vacuum operation progress

## Running Tests

### Run all vacuum collector tests
```bash
cargo test --test collectors_tests vacuum
