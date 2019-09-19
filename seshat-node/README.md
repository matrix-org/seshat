# Seshat-node

Node.js bindings for the Matrix message database/indexer Seshat.

## Instalation

To install the bindings rust and npm are needed:

    $ npm install

The above command will compile all the necessary rust libraries, install
javascript dependencies and build a node module. After the command is done
building the library can be used inside of node as usual:

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
