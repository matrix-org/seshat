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

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::mpsc::Sender;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef};
use rusqlite::ToSql;

use crate::error::Result;

#[cfg(test)]
use fake::faker::internet::raw::*;
#[cfg(test)]
use fake::locales::*;
#[cfg(test)]
use fake::{Dummy, Fake};

/// Matrix event types.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum EventType {
    /// Matrix room messages, corresponds to the m.room.message type, has a body
    /// inside of the content.
    Message,
    /// Matrix room messages, corresponds to the m.room.name type, has a name
    /// inside of the content.
    Name,
    /// Matrix room messages, corresponds to the m.room.topic type, has a topic
    /// inside of the content.
    Topic,
}

impl Display for EventType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let string = match self {
            EventType::Message => "m.room.message",
            EventType::Topic => "m.room.topic",
            EventType::Name => "m.room.name",
        };

        write!(f, "{}", string)
    }
}

impl ToSql for EventType {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(format!("{}", self)))
    }
}

impl FromSql for EventType {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Text(s) => {
                let s = std::str::from_utf8(s).map_err(|e| FromSqlError::Other(Box::new(e)))?;

                let e = match s {
                    "m.room.message" => EventType::Message,
                    "m.room.name" => EventType::Name,
                    "m.room.topic" => EventType::Topic,
                    _ => return Err(FromSqlError::InvalidType),
                };

                Ok(e)
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

/// Matrix event that can be added to the database.
#[derive(Debug, PartialEq, Clone)]
pub struct Event {
    /// The type of the event.
    pub event_type: EventType,
    /// The textual representation of a message, this part of the event will be
    /// indexed.
    pub content_value: String,
    /// The unique identifier of this event.
    pub event_id: String,
    /// The MXID of the user who sent this event.
    pub sender: String,
    /// Timestamp in milliseconds on the originating Homeserver when this event
    /// was sent.
    pub server_ts: i64,
    /// The ID of the room associated with this event.
    pub room_id: String,
    /// The serialized JSON string of the event. This string will be returned
    /// by a search later on.
    pub source: String,
}

#[cfg(test)]
impl<T> Dummy<T> for Event {
    fn dummy_with_rng<R: ?Sized>(_config: &T, _rng: &mut R) -> Self {
        let domain: String = FreeEmailProvider(EN).fake();
        Event::new(
            EventType::Message,
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

pub(crate) type HistoricEventsT = (
    Option<CrawlerCheckpoint>,
    Option<CrawlerCheckpoint>,
    Vec<(Event, Profile)>,
    Sender<Result<bool>>,
);

pub(crate) type EventContext = (
    Vec<SerializedEvent>,
    Vec<SerializedEvent>,
    HashMap<MxId, Profile>,
);

pub(crate) type RoomId = String;
pub(crate) type MxId = String;
pub(crate) type EventId = String;
pub(crate) type SerializedEvent = String;

impl Event {
    /// Create a new event.
    /// # Arguments
    ///
    /// * `event_type` - The type of the event.
    /// * `content_value` - The plain text value of the content, body for a
    /// message event, topic for a topic event and name for a name event.
    /// * `event_id` - The unique identifier of the event.
    /// * `sender` - The unique identifier of the event author.
    /// * `server_ts` - The timestamp of the event.
    /// * `room_id` - The unique identifier of the room that the event belongs
    /// to.
    /// * `source` - The serialized version of the event.
    pub fn new(
        event_type: EventType,
        content_value: &str,
        event_id: &str,
        sender: &str,
        server_ts: i64,
        room_id: &str,
        source: &str,
    ) -> Event {
        Event {
            event_type: event_type.clone(),
            content_value: content_value.to_string(),
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
    pub displayname: Option<String>,
    /// The user's avatar URL if they have set one.
    pub avatar_url: Option<String>,
}

impl Profile {
    // Create a new profile.
    /// # Arguments
    ///
    /// * `displayname` - The human readable name of the user.
    /// * `avatar_url` - The URL of the avatar of the user.
    pub fn new(displayname: &str, avatar_url: &str) -> Profile {
        Profile {
            displayname: Some(displayname.to_string()),
            avatar_url: Some(avatar_url.to_string()),
        }
    }
}

#[cfg(test)]
pub static EVENT_SOURCE: &str = "{
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
pub static TOPIC_EVENT_SOURCE: &str = "{
    content: {
        topic: Test topic
    },
    event_id: $15163622448EBvZJ:localhost,
    origin_server_ts: 1516362244050,
    sender: @example2:localhost,
    type: m.room.topic,
    unsigned: {age: 43289803098},
    user_id: @example2:localhost,
    age: 43289803098
}";

#[cfg(test)]
lazy_static! {
    pub static ref EVENT: Event = Event::new(
        EventType::Message,
        "Test message",
        "$15163622445EBvZJ:localhost",
        "@example2:localhost",
        151636_2244026,
        "!test_room:localhost",
        EVENT_SOURCE
    );
}

#[cfg(test)]
lazy_static! {
    pub static ref TOPIC_EVENT: Event = Event::new(
        EventType::Topic,
        "Test topic",
        "$15163622445EBvZE:localhost",
        "@example2:localhost",
        151636_2244038,
        "!test_room:localhost",
        TOPIC_EVENT_SOURCE
    );
}

#[cfg(test)]
lazy_static! {
    pub static ref JAPANESE_EVENTS: Vec<Event> = vec![
        Event::new(
            EventType::Message,
            "日本語の本文",
            "$15163622445EBvZE:localhost",
            "@example2:localhost",
            151636_2244038,
            "!test_room:localhost",
            ""
        ),
        Event::new(
            EventType::Message,
            "ルダの伝説 時のオカリナ",
            "$15163622445ZERuD:localhost",
            "@example2:localhost",
            151636_2244063,
            "!test_room:localhost",
            ""
        ),
    ];
}

#[derive(Debug, PartialEq, Clone)]
/// A checkpoint that remembers the current point in a room timeline when
/// fetching the history of the room.
pub struct CrawlerCheckpoint {
    /// The unique id of the room that this checkpoint belongs to.
    pub room_id: String,
    /// The token that can be used to go further back in the event timeline of
    /// the room and fetch more messages from the room history.
    pub token: String,
    /// Is this a checkpoint for a complete crawl of the message history.
    pub full_crawl: bool,
    /// The direction which should be used to crawl the room timeline.
    pub direction: CheckpointDirection,
}

#[derive(Debug, PartialEq, Clone)]
#[allow(missing_docs)]
pub enum CheckpointDirection {
    Forwards,
    Backwards,
}

impl Display for CheckpointDirection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let string = match self {
            CheckpointDirection::Forwards => "Forwards",
            CheckpointDirection::Backwards => "Backwards",
        };

        write!(f, "{}", string)
    }
}

impl ToSql for CheckpointDirection {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(format!("{}", self)))
    }
}

impl FromSql for CheckpointDirection {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Text(s) => {
                let s = std::str::from_utf8(s).map_err(|e| FromSqlError::Other(Box::new(e)))?;

                let e = match s {
                    "Forwards" => CheckpointDirection::Forwards,
                    "Backwards" => CheckpointDirection::Backwards,
                    _ => return Err(FromSqlError::InvalidType),
                };

                Ok(e)
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}
