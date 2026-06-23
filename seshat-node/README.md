# Seshat-node

Node.js bindings for the Matrix message database/indexer Seshat.

## Installation

Binaries for common platforms are build for each release, check
the releases page to see if your platform is supported.

To install for a supported platform, you only need yarn. Otherwise
you will also need to install rust.

    $ yarn

The above command will compile all the necessary rust libraries, install
javascript dependencies and build a node module (unless a prebuilt is available).

This will build a fully static version, with SQLCipher and OpenSSL statically
built and linked by cargo.

If you'd rather use SQLCipher from the system you can use `build` instead.

After the command is done building the library can be used inside of node as usual:

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
