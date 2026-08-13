# Seshat-node

Node.js bindings for the Matrix message database/indexer Seshat.

## Installation

    $ yarn add @matrix-org/seshat

Prebuilt native modules are published as per-platform `optionalDependencies`;
your package manager installs only the one matching your machine. Prebuilts
are published for:

| Platform | Architectures | SQLCipher                    |
| -------- | -------------- | ---------------------------- |
| Linux    | x64, arm64     | static (default) or dynamic  |
| macOS    | x64, arm64     | static                       |
| Windows  | x64, arm64     | static                       |

Static links SQLCipher and OpenSSL into the module; dynamic links the
system's SQLCipher instead. Dynamic is not installed by default. To use it,
install the matching package explicitly alongside the main one:

    $ yarn add @matrix-org/seshat @matrix-org/seshat-linux-x64-dynamic

### Building from source

For unsupported platforms, or to link the system SQLCipher yourself:

    $ git clone https://github.com/matrix-org/seshat
    $ cd seshat/seshat-node
    $ yarn install
    $ yarn run build            # links the system SQLCipher
    $ yarn run build-bundled    # statically links SQLCipher + OpenSSL

Requires a Rust toolchain (and the system SQLCipher headers, for `build`).

Once installed, the library can be used inside of node as usual:

```javascript
const Seshat = require(".")
```

## Usage

```javascript

let db = new Seshat("/home/example/database_dir");
// Add a Matrix event to the database.
db.addEvent(textEvent, profile);
// Commit events waiting in the queue to the database.
await db.commit();
// Search the database for messages containing the word 'Test'
let results = await db.search({search_term: 'Test'});

```
