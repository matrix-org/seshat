// Copyright 2019 The Matrix.org Foundation CIC
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

use failure::Fail;
use std::collections::HashMap;
use std::sync::mpsc::Sender;

use r2d2;
use rusqlite;
use tantivy;

#[cfg(test)]
use fake::faker::internet::raw::*;
#[cfg(test)]
use fake::locales::*;
#[cfg(test)]
use fake::{Dummy, Fake};

/// Struct representing a Matrix event that should be added to the database.
#[derive(Debug, PartialEq, Default, Clone)]
pub struct Event {
    /// The textual representation of a message, this part of the event will be
    /// indexed.
    pub body: String,
    /// The unique identifier of this event.
    pub event_id: String,
    /// The MXID of the user who sent this event.
    pub sender: String,
    /// Timestamp in milliseconds on the originating homeserver when this event
    /// was sent.
    pub server_ts: i64,
    /// The ID of the room associated with this event.
    pub room_id: String,
    /// The serialized json string of the event. This string will be returned
    /// by a search later on.
    pub source: String,
}

#[derive(Debug, PartialEq, Default, Clone)]
/// A checkpoint that remembers the current point in a room timeline when
/// fetching the backlog of the room.
pub struct BacklogCheckpoint {
    /// The unique id of the room that this checkpoint belongs to.
    pub room_id: String,
    /// The token that can be used to go further back in the event timeline of
    /// the room and fetch more messages from the backlog.
    pub token: String,
}

#[cfg(test)]
impl<T> Dummy<T> for Event {
    fn dummy_with_rng<R: ?Sized>(_config: &T, _rng: &mut R) -> Self {
        let domain: String = FreeEmailProvider(EN).fake();
        Event::new(
            "Hello world",
            &format!("${}:{}", (0..10).fake::<u8>(), &domain),
            &format!(
                "@{}:{}",
                Username(EN).fake::<String>(),
                FreeEmailProvider(EN).fake::<String>()
            ),
            151636_2244026,
            "!test_room:localhost",
            EVENT_SOURCE,
        )
    }
}

pub(crate) type BacklogEventsT = (
    Option<BacklogCheckpoint>,
    Option<BacklogCheckpoint>,
    Vec<(Event, Profile)>,
    Sender<Result<bool>>,
);

pub(crate) enum ThreadMessage {
    Event((Event, Profile)),
    BacklogEvents(BacklogEventsT),
    Write,
}

#[derive(Fail, Debug)]
/// Seshat error types.
pub enum Error {
    #[fail(display = "Sqlite pool error: {}", _0)]
    /// Error signaling that there was an error with the Sqlite connection
    /// pool.
    PoolError(r2d2::Error),
    #[fail(display = "Sqlite database error: {}", _0)]
    /// Error signaling that there was an error an Sqlite transaction.
    DatabaseError(rusqlite::Error),
    #[fail(display = "Index error: {}", _0)]
    /// Error signaling that there was an with the event indexer.
    IndexError(tantivy::Error),
}

/// Result type for seshat operations.
pub type Result<T> = std::result::Result<T, Error>;

pub(crate) type RoomId = String;
pub(crate) type EventId = String;

impl From<r2d2::Error> for Error {
    fn from(err: r2d2::Error) -> Self {
        Error::PoolError(err)
    }
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        Error::DatabaseError(err)
    }
}

impl From<tantivy::Error> for Error {
    fn from(err: tantivy::Error) -> Self {
        Error::IndexError(err)
    }
}

impl Event {
    #[cfg(test)]
    pub(crate) fn new(
        body: &str,
        event_id: &str,
        sender: &str,
        server_ts: i64,
        room_id: &str,
        source: &str,
    ) -> Event {
        Event {
            body: body.to_string(),
            event_id: event_id.to_string(),
            sender: sender.to_string(),
            server_ts,
            room_id: room_id.to_string(),
            source: source.to_string(),
        }
    }
}

/// A users profile information at the time an event was posted.
#[derive(Debug, PartialEq, Default, Clone)]
pub struct Profile {
    /// The users display name if one is set.
    pub display_name: Option<String>,
    /// The user's avatar URL if they have set one.
    pub avatar_url: Option<String>,
}

impl Profile {
    #[cfg(test)]
    pub fn new(display_name: &str, avatar_url: &str) -> Profile {
        Profile {
            display_name: Some(display_name.to_string()),
            avatar_url: Some(avatar_url.to_string()),
        }
    }
}

#[derive(Debug, PartialEq, Default, Clone)]
/// A search result
pub struct SearchResult {
    /// The score that the full text search assigned to this event.
    pub score: f32,
    /// The source of the event that matched a search.
    pub event_source: String,
    /// Events that happened before our matched event.
    pub events_before: Vec<String>,
    /// Events that happened after our matched event.
    pub events_after: Vec<String>,
    /// The profile os the sender of the matched event.
    pub profile_info: HashMap<String, Profile>,
}

#[cfg(test)]
pub(crate) static EVENT_SOURCE: &str = "{
    content: {
        body: Test message, msgtype: m.text
    },
    event_id: $15163622445EBvZJ:localhost,
    origin_server_ts: 1516362244026,
    sender: @example2:localhost,
    type: m.room.message,
    unsigned: {age: 43289803095},
    user_id: @example2:localhost,
    age: 43289803095
}";

#[cfg(test)]
lazy_static! {
    pub(crate) static ref EVENT: Event = Event::new(
        "Test message",
        "$15163622445EBvZJ:localhost",
        "@example2:localhost",
        151636_2244026,
        "!test_room:localhost",
        EVENT_SOURCE
    );
}
