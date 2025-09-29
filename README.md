[![Build Status](https://travis-ci.org/matrix-org/seshat.svg?branch=master)](https://travis-ci.org/matrix-org/seshat)
[![Crates.io](https://img.shields.io/crates/v/seshat.svg)](https://crates.io/crates/seshat)
[![Documentation](https://docs.rs/seshat/badge.svg)](https://docs.rs/seshat)
[![#seshat](https://img.shields.io/badge/matrix-%23seshat:matrix.org-blue.svg)](https://matrix.to/#/!VYQqtuzngcvIzsyyOV:matrix.org?via=matrix.org&via=t2l.io&via=t2bot.io)


# Seshat

Seshat is a database and indexer for Matrix events.

Its main use is to be used as a full text search backend for Matrix clients.

## JavaScript bindings

Seshat provides JavaScript bindings which can be found in the
[seshat-node](seshat-node) subdir.

## Usage

There are two modes of operation for Seshat, adding live events as they
come in:

```rust
use seshat::{Database, Event, Profile};
use tempfile::tempdir;

let tmpdir = tempdir().unwrap();
let mut db = Database::new(tmpdir.path()).unwrap();

/// Method to call for every live event that gets received during a sync.
fn add_live_event(event: Event, profile: Profile, database: &Database) {
    database.add_event(event, profile);
}
/// Method to call on every successful sync after live events were added.
fn on_sync(database: &mut Database) {
    database.commit().unwrap();
}
```

The other mode is to add events from the room history using the
`/room/{room_id}/messages` Matrix API endpoint. This method supports
storing checkpoints which remember the arguments to continue fetching events
from the `/room/{room_id}/messages` API:

```rust
database.add_historic_events(events, old_checkpoint, new_checkpoint)?;
```

Once events have been added a search can be done:
```rust
let result = database.search("test", &SearchConfig::new()).unwrap();
```

## Development

Seshat uses standard cargo commands `build` and `test`.

You can install [pre-commit](https://pre-commit.com/) and then `pre-commit install` 
to ensure your work is linted on commit.
