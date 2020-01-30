// Copyright 2019 The Matrix.org Foundation C.I.C.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Seshat - a full text search library for Matrix clients.
//!
//! There are two modes of operation for Seshat, adding live events as they
//! come in:
//!
//! ```
//! use seshat::{Database, Event, Profile};
//! use tempfile::tempdir;
//!
//! let tmpdir = tempdir().unwrap();
//! let mut database = Database::new(tmpdir.path()).unwrap();
//!
//! /// Method to call for every live event that gets received during a sync.
//! fn add_live_event(event: Event, profile: Profile, database: &Database) {
//!     database.add_event(event, profile);
//! }
//! /// Method to call on every successful sync after live events were added.
//! fn on_sync(database: &mut Database) {
//!     database.commit().unwrap();
//! }
//! ```
//!
//! The other mode is to add events from the room history using the
//! `/room/{room_id}/messages` Matrix API endpoint. This method supports
//! storing checkpoints which remember the arguments to continue fetching events
//! from the `/room/{room_id}/messages` API:
//!
//! ```noexecute
//! database.add_historic_events(events, old_checkpoint, new_checkpoint)?;
//! ```
//!
//! Once events have been added a search can be done:
//! ```noexecute
//! let result = database.search("test", &SearchConfig::new()).unwrap();
//! ```

#![warn(missing_docs)]

#[cfg(test)]
#[macro_use]
extern crate lazy_static;

mod config;
mod database;
mod error;
mod events;
mod index;

pub use database::{Connection, Database, DatabaseStats, SearchResult, Searcher};

pub use error::{Error, Result};

pub use config::{Config, Language, LoadConfig, LoadDirection, SearchConfig};
pub use events::{CheckpointDirection, CrawlerCheckpoint, Event, EventType, Profile};

pub use std::sync::mpsc::Receiver;

#[cfg(test)]
pub use events::{EVENT, EVENT_SOURCE, TOPIC_EVENT, TOPIC_EVENT_SOURCE};
