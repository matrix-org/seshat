# Seshat-node

Node.js bindings for the Matrix message database/indexer Seshat.

## Installation

To install the bindings rust and yarn are needed:

    $ yarn && yarn run build-bundled

The above command will compile all the necessary rust libraries, install
javascript dependencies and build a node module.

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
