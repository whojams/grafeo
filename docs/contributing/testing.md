---
title: Testing
description: Test strategy and running tests.
tags:
  - contributing
---

# Testing

## Running Tests

```bash
# All tests
cargo test --all-features --workspace

# Specific crate
cargo test -p grafeo-core

# Single test with output
cargo test test_name -- --nocapture

# Release mode (catches optimization-specific issues)
cargo test --all-features --workspace --release
```

### Python Tests

```bash
cd crates/bindings/python
maturin develop
pytest tests/python/ -v
```

### Node.js Tests

```bash
cd crates/bindings/node
npm install && npm run build
npm test
```

## Coverage

```bash
# Install tarpaulin
cargo install cargo-tarpaulin

# Generate report
cargo tarpaulin --workspace --out Html
```

## Coverage Targets

| Crate | Target |
|-------|--------|
| grafeo-common | 95% |
| grafeo-core | 90% |
| grafeo-adapters | 85% |
| grafeo-engine | 85% |
| grafeo-python | 80% |
| grafeo-node | 80% |
| Overall workspace | 82%+ |

## Test Categories

- **Unit tests** - Same file, `#[cfg(test)]` module
- **Integration tests** - `tests/` directory
- **Property tests** - Using `proptest` crate

## Writing Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_creation() {
        let store = LpgStore::new();
        let id = store.create_node(&["Person"], Default::default());
        assert!(store.get_node(id).is_some());
    }
}
```
