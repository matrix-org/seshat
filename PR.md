# feat: Add N-gram Tokenizer Mode for CJK Language Support

## Summary

This PR introduces a new tokenizer mode feature that enables N-gram tokenization, which is essential for languages without clear word boundaries such as Japanese, Chinese, and Korean (CJK languages).

## Motivation

The default language-based tokenizer uses word stemming which works well for languages like English and German that have clear word boundaries. However, for CJK languages, this approach fails to properly tokenize text, resulting in poor search results.

N-gram tokenization splits text into overlapping character sequences of configurable sizes (e.g., 2-4 characters), enabling partial matching that works effectively for all languages regardless of word boundaries.

## Changes

### Core Features

- **New `TokenizerMode` enum** (`src/config.rs`)
  - `LanguageBased` - Default mode using SimpleTokenizer with language-specific stemming
  - `Ngram { min_gram, max_gram }` - N-gram tokenizer with configurable gram sizes

- **Config API** (`src/config.rs`)
  - `use_ngram_tokenizer(min_gram, max_gram)` - Enable N-gram mode
  - `use_language_based_tokenizer()` - Use default language-based mode

- **Schema mismatch detection** (`src/index/mod.rs`)
  - Changing tokenizer mode on an existing database will cause a schema mismatch error
  - Clients should delete and recreate the database when switching tokenizer modes

### Node.js Bindings (`seshat-node`)

- **New config options** (`seshat-node/src/utils.rs`)
  - `tokenizerMode`: `"ngram"` or `"language"` (default)
  - `ngramMinSize`: Minimum n-gram size (default: 2)
  - `ngramMaxSize`: Maximum n-gram size (default: 4)

- **Updated JSDoc** (`seshat-node/index.js`)
  - Documented new tokenizer options for `Seshat` and `SeshatRecovery` constructors

### Project Structure - Cargo Workspace

This PR also converts the project to a **Cargo Workspace** for better dependency management between `seshat` and `seshat-node`.

#### Before (separate crates)

```
seshat/
├── Cargo.toml          # seshat crate
└── seshat-node/
    └── Cargo.toml      # seshat-node crate (depended on crates.io version)
```

#### After (workspace)

```
seshat/
├── Cargo.toml          # Workspace root + seshat crate
└── seshat-node/
    └── Cargo.toml      # Workspace member (uses local seshat)
```

#### Workspace Configuration

**Root `Cargo.toml`:**

```toml
[workspace]
members = ["seshat-node"]

[workspace.package]
version = "4.0.1"
authors = ["Damir Jelić <poljar@termina.org.uk>"]
edition = "2018"
license = "Apache-2.0"

[workspace.dependencies]
seshat = { path = "." }

[package]
name = "seshat"
version.workspace = true
authors.workspace = true
edition.workspace = true
license.workspace = true
# ...
```

**`seshat-node/Cargo.toml`:**

```toml
[package]
name = "matrix-seshat"
version = "4.0.0"
authors.workspace = true
license.workspace = true
edition.workspace = true
# ...

[dependencies]
seshat.workspace = true
# ...
```

#### Benefits of Workspace

1. **Unified metadata** - Version, authors, edition, and license are defined once and shared
2. **Local development** - `seshat-node` automatically uses the local `seshat` crate without manual path switching
3. **Single build command** - `cargo build --workspace` builds both crates
4. **Consistent testing** - `cargo test --workspace` runs all tests
5. **Publishing compatibility** - When publishing to crates.io, workspace dependencies are resolved to version numbers

### Removed Dependencies

- **Removed `tinysegmenter`** - This Japanese tokenizer library was not used in the codebase

### Test Improvements

- **Test database handling** (`src/database/mod.rs`, `src/database/recovery.rs`)
  - Tests now copy database fixtures to temp directories to avoid modifying source files
  - **Problem**: Previously, tests like `database_upgrade_v1_2` and `test_recovery` directly operated on files in `data/database/`, causing them to be modified during test execution. This led to test failures on subsequent runs and dirty git state.
  - **Solution**: Copy the test database to a temporary directory before running the test:
    ```rust
    let tmpdir = tempfile::tempdir().unwrap();
    let path = tmpdir.path().to_path_buf();
    let mut options = fs_extra::dir::CopyOptions::new();
    options.content_only = true;
    fs_extra::dir::copy(&src_path, &path, &options).expect("Failed to copy test database");
    ```

- **New tests** (`src/index/mod.rs`)
  - `ngram_tokenizer_mode` - Verifies N-gram partial matching
  - `schema_mismatch_on_tokenizer_mode_change` - Verifies schema validation when switching between language-based and N-gram tokenizer
  - `schema_mismatch_on_ngram_size_change` - Verifies schema validation when changing N-gram sizes (min/max)
  - `ngram_tokenizer_japanese` - Verifies Japanese text search with N-gram (partial matching for CJK characters)

## Usage Example

### Rust

```rust
use seshat::{Config, Database};

// Create database with N-gram tokenizer (recommended for CJK languages)
let config = Config::new().use_ngram_tokenizer(2, 4);
let db = Database::new_with_config("/path/to/db", &config)?;
```

### JavaScript (Node.js)

```javascript
const { Seshat } = require('matrix-seshat');

// Create database with N-gram tokenizer
const db = new Seshat('/path/to/db', {
    tokenizerMode: 'ngram',
    ngramMinSize: 2,
    ngramMaxSize: 4
});
```

## Breaking Changes

- **Schema incompatibility**: Databases created with different tokenizer modes are incompatible. Attempting to open a database with a different tokenizer mode will result in a `TantivyError::SchemaError`. Clients must handle this by deleting and recreating the database.

## Test Plan

- [x] `cargo test --workspace` passes
- [x] `yarn build` in `seshat-node` succeeds
- [x] N-gram tokenizer correctly matches partial Japanese text
- [x] Schema mismatch is properly detected when tokenizer mode changes

## Files Changed

| File | Description |
|------|-------------|
| `Cargo.toml` | Workspace configuration, removed tinysegmenter |
| `seshat-node/Cargo.toml` | Use workspace dependencies |
| `src/config.rs` | Added `TokenizerMode` enum and config methods |
| `src/lib.rs` | Export `TokenizerMode` |
| `src/index/mod.rs` | N-gram tokenizer registration, new tests |
| `src/database/mod.rs` | Test improvement (temp directory) |
| `src/database/recovery.rs` | Test improvement (temp directory) |
| `seshat-node/src/utils.rs` | Parse tokenizer mode from JS config |
| `seshat-node/index.js` | JSDoc updates |
