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

use std::cmp::Ordering;
use std::collections::HashMap;

use rusqlite::{ToSql, NO_PARAMS};

#[cfg(test)]
use r2d2_sqlite::SqliteConnectionManager;
#[cfg(test)]
use r2d2::PooledConnection;

use crate::index::Writer as IndexWriter;
use crate::database::{DATABASE_VERSION, SearchResult};
use crate::{Database};
use crate::config::LoadDirection;
use crate::error::Result;
use crate::events::{
    CrawlerCheckpoint, Event, EventContext, EventId, Profile,
    SerializedEvent,
};

const FILE_EVENT_TYPES: &str = "'m.image', 'm.file', 'm.audio', 'm.video'";

impl Database {
    /// Write the events to the database.
    /// Returns a tuple containing a boolean and an array if integers. The
    /// boolean notifies us if all the events were already added to the
    /// database, the integers are the database ids of our events.
    pub(crate) fn write_events_helper(
        connection: &rusqlite::Connection,
        index_writer: &mut IndexWriter,
        events: &mut Vec<(Event, Profile)>,
    ) -> Result<(bool, Vec<i64>)> {
        let mut ret = Vec::new();
        let mut event_ids = Vec::new();

        for (e, p) in events.drain(..) {
            let event_id = Database::save_event(&connection, &e, &p)?;
            match event_id {
                Some(id) => {
                    index_writer.add_event(&e);
                    ret.push(false);
                    event_ids.push(id);
                }
                None => {
                    ret.push(true);
                    continue;
                }
            }
        }

        Ok((ret.iter().all(|&x| x), event_ids))
    }

    /// Delete non committed events from the database that were committed
    pub(crate) fn mark_events_as_indexed(
        connection: &mut rusqlite::Connection,
        events: &mut Vec<i64>,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let transaction = connection.transaction()?;

        for chunk in events.chunks(50) {
            let parameter_str = std::iter::repeat(", ?")
                .take(chunk.len() - 1)
                .collect::<String>();

            let mut stmt = transaction.prepare(&format!(
                "DELETE from uncommitted_events
                     WHERE id IN (?{})",
                &parameter_str
            ))?;

            stmt.execute(chunk)?;
        }

        transaction.commit()?;
        events.clear();

        Ok(())
    }

    pub(crate) fn write_events(
        connection: &mut rusqlite::Connection,
        index_writer: &mut IndexWriter,
        message: (
            Option<CrawlerCheckpoint>,
            Option<CrawlerCheckpoint>,
            &mut Vec<(Event, Profile)>,
        ),
        force_commit: bool,
        uncommitted_events: &mut Vec<i64>,
    ) -> Result<bool> {
        let (new_checkpoint, old_checkpoint, mut events) = message;
        let transaction = connection.transaction()?;

        let (ret, event_ids) =
            Database::write_events_helper(&transaction, index_writer, &mut events)?;
        Database::replace_crawler_checkpoint(
            &transaction,
            new_checkpoint.as_ref(),
            old_checkpoint.as_ref(),
        )?;

        transaction.commit()?;

        uncommitted_events.extend(event_ids);

        let committed = if force_commit {
            index_writer.force_commit()?;
            true
        } else {
            index_writer.commit()?
        };

        if committed {
            Database::mark_events_as_indexed(connection, uncommitted_events)?;
        }

        Ok(ret)
    }

    pub(crate) fn get_version(connection: &rusqlite::Connection) -> Result<i64> {
        connection.execute(
            "INSERT OR IGNORE INTO version ( version ) VALUES(?1)",
            &[DATABASE_VERSION],
        )?;

        let version: i64 =
            connection.query_row("SELECT version FROM version", NO_PARAMS, |row| row.get(0))?;

        // Do database migrations here before bumping the database version.

        Ok(version)
    }

    pub(crate) fn create_tables(conn: &rusqlite::Connection) -> Result<()> {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS profiles (
                id INTEGER NOT NULL PRIMARY KEY,
                user_id TEXT NOT NULL,
                displayname TEXT NOT NULL,
                avatar_url TEXT NOT NULL,
                UNIQUE(user_id,displayname,avatar_url)
            )",
            NO_PARAMS,
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS events (
                id INTEGER NOT NULL PRIMARY KEY,
                event_id TEXT NOT NULL,
                sender TEXT NOT NULL,
                server_ts DATETIME NOT NULL,
                room_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                msgtype TEXT,
                source TEXT NOT NULL,
                profile_id INTEGER NOT NULL,
                FOREIGN KEY (profile_id) REFERENCES profile (id),
                FOREIGN KEY (room_id) REFERENCES rooms (id),
                UNIQUE(event_id, room_id)
            )",
            NO_PARAMS,
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS uncommitted_events (
                id INTEGER NOT NULL PRIMARY KEY,
                event_id INTEGER NOT NULL,
                content_value TEXT NOT NULL,
                FOREIGN KEY (event_id) REFERENCES events (id),
                UNIQUE(event_id)
            )",
            NO_PARAMS,
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS rooms (
                id INTEGER NOT NULL PRIMARY KEY,
                room_id TEXT NOT NULL,
                UNIQUE(room_id)
            )",
            NO_PARAMS,
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS crawlercheckpoints (
                id INTEGER NOT NULL PRIMARY KEY,
                room_id TEXT NOT NULL,
                token TEXT NOT NULL,
                full_crawl BOOLEAN NOT NULL,
                direction TEXT NOT NULL,
                UNIQUE(room_id,token,full_crawl,direction)
            )",
            NO_PARAMS,
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS version (
                id INTEGER NOT NULL PRIMARY KEY CHECK (id = 1),
                version INTEGER NOT NULL
            )",
            NO_PARAMS,
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS event_profile_id ON events (profile_id)",
            NO_PARAMS,
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS room_events ON events (room_id, type, msgtype)",
            NO_PARAMS,
        )?;

        Ok(())
    }

    pub(crate) fn get_event_count(connection: &rusqlite::Connection) -> rusqlite::Result<i64> {
        connection.query_row("SELECT COUNT(*) FROM events", NO_PARAMS, |row| row.get(0))
    }

    pub(crate) fn get_room_count(connection: &rusqlite::Connection) -> rusqlite::Result<i64> {
        // TODO once we support upgraded rooms we should return only leaf rooms
        // here, rooms that are not ancestors to another one.
        connection.query_row("SELECT COUNT(*) FROM rooms", NO_PARAMS, |row| row.get(0))
    }

    pub(crate) fn save_profile(
        connection: &rusqlite::Connection,
        user_id: &str,
        profile: &Profile,
    ) -> Result<i64> {
        let displayname = profile.displayname.as_ref();
        let avatar_url = profile.avatar_url.as_ref();

        // unwrap_or_default doesn't work on references sadly.
        let displayname = if let Some(d) = displayname { d } else { "" };

        let avatar_url = if let Some(a) = avatar_url { a } else { "" };

        connection.execute(
            "
            INSERT OR IGNORE INTO profiles (
                user_id, displayname, avatar_url
            ) VALUES(?1, ?2, ?3)",
            &[user_id, displayname, avatar_url],
        )?;

        let profile_id: i64 = connection.query_row(
            "
            SELECT id FROM profiles WHERE (
                user_id=?1
                and displayname=?2
                and avatar_url=?3)",
            &[user_id, displayname, avatar_url],
            |row| row.get(0),
        )?;

        Ok(profile_id)
    }

    #[cfg(test)]
    pub(crate) fn load_profile(
        connection: &PooledConnection<SqliteConnectionManager>,
        profile_id: i64,
    ) -> Result<Profile> {
        let profile = connection.query_row(
            "SELECT displayname, avatar_url FROM profiles WHERE id=?1",
            &[profile_id],
            |row| {
                Ok(Profile {
                    displayname: row.get(0)?,
                    avatar_url: row.get(1)?,
                })
            },
        )?;

        Ok(profile)
    }

    pub(crate) fn get_room_id(
        connection: &rusqlite::Connection,
        room: &str,
    ) -> rusqlite::Result<i64> {
        connection.execute("INSERT OR IGNORE INTO rooms (room_id) VALUES(?1)", &[room])?;

        let room_id: i64 =
            connection.query_row("SELECT id FROM rooms WHERE (room_id=?1)", &[room], |row| {
                row.get(0)
            })?;

        Ok(room_id)
    }

    pub(crate) fn load_uncommitted_events(
        connection: &rusqlite::Connection,
    ) -> rusqlite::Result<Vec<(i64, Event)>> {
        let mut stmt = connection.prepare(
                "SELECT uncommitted_events.id, uncommitted_events.event_id, content_value, type, msgtype,
                 events.event_id, sender, server_ts, rooms.room_id, source
                 FROM uncommitted_events
                 INNER JOIN events on events.id = uncommitted_events.event_id
                 INNER JOIN rooms on rooms.id = events.room_id
                 ")?;

        let events = stmt.query_map(NO_PARAMS, |row| {
            Ok((
                row.get(0)?,
                Event {
                    event_type: row.get(3)?,
                    content_value: row.get(2)?,
                    msgtype: row.get(4)?,
                    event_id: row.get(5)?,
                    sender: row.get(6)?,
                    server_ts: row.get(7)?,
                    room_id: row.get(8)?,
                    source: row.get(9)?,
                },
            ))
        })?;

        events.collect()
    }

    pub(crate) fn save_event_helper(
        connection: &rusqlite::Connection,
        event: &Event,
        profile_id: i64,
    ) -> rusqlite::Result<i64> {
        let room_id = Database::get_room_id(connection, &event.room_id)?;

        let mut statement = connection.prepare(
            "
            INSERT INTO events (
                event_id, sender, server_ts, room_id, type,
                msgtype, source, profile_id
            ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )?;

        let event_id = statement.insert(&[
            &event.event_id,
            &event.sender,
            &event.server_ts as &dyn ToSql,
            &room_id as &dyn ToSql,
            &event.event_type as &dyn ToSql,
            &event.msgtype,
            &event.source,
            &profile_id as &dyn ToSql,
        ])?;

        let mut stmt = connection.prepare(
            "
            INSERT OR IGNORE INTO uncommitted_events (
                event_id, content_value
            ) VALUES (?1, ?2)",
        )?;

        let id = stmt.insert(&[&event_id as &dyn ToSql, &event.content_value])?;

        Ok(id)
    }

    pub(crate) fn save_event(
        connection: &rusqlite::Connection,
        event: &Event,
        profile: &Profile,
    ) -> Result<Option<i64>> {
        if Database::event_in_store(connection, event)? {
            return Ok(None);
        }

        let profile_id = Database::save_profile(connection, &event.sender, profile)?;
        let event_id = Database::save_event_helper(connection, event, profile_id)?;

        Ok(Some(event_id))
    }

    pub(crate) fn event_in_store(
        connection: &rusqlite::Connection,
        event: &Event,
    ) -> rusqlite::Result<bool> {
        let room_id = Database::get_room_id(connection, &event.room_id)?;
        let ret: std::result::Result<i64, rusqlite::Error> = connection.query_row(
            "
            SELECT id FROM events WHERE (
                event_id=?1
                and room_id=?2)",
            &[&event.event_id, &room_id as &dyn ToSql],
            |row| row.get(0),
        );

        match ret {
            Ok(_event_id) => Ok(true),
            Err(_e) => Ok(false),
        }
    }

    pub(crate) fn load_file_events(
        connection: &rusqlite::Connection,
        room_id: &str,
        limit: usize,
        from_event: Option<&str>,
        direction: &LoadDirection,
    ) -> rusqlite::Result<Vec<(SerializedEvent, Profile)>> {
        match from_event {
            Some(e) => {
                let event = Database::load_event(connection, room_id, e)?;

                let (direction, sort) = match direction {
                    LoadDirection::Backwards => ("<=", "DESC"),
                    LoadDirection::Forwards => (">=", "ASC"),
                };

                let mut stmt = connection.prepare(&format!(
                    "SELECT source, displayname, avatar_url
                     FROM events
                     INNER JOIN profiles on profiles.id = events.profile_id
                     WHERE (
                         (events.room_id == ?1) &
                         (type == 'm.room.message') &
                         (msgtype in ({})) &
                         (event_id != ?2) &
                         (server_ts {} ?3)
                     ) ORDER BY server_ts {} LIMIT ?4
                     ",
                    FILE_EVENT_TYPES, direction, sort
                ))?;

                let room_id = Database::get_room_id(connection, &room_id)?;
                let events = stmt.query_map(
                    &vec![
                        &room_id as &dyn ToSql,
                        &event.event_id as &dyn ToSql,
                        &event.server_ts as &dyn ToSql,
                        &(limit as i64),
                    ],
                    |row| {
                        Ok((
                            row.get(0)?,
                            Profile {
                                displayname: row.get(1)?,
                                avatar_url: row.get(2)?,
                            },
                        ))
                    },
                )?;
                events.collect()
            }
            None => {
                let mut stmt = connection.prepare(&format!(
                    "SELECT source, displayname, avatar_url
                     FROM events
                     INNER JOIN profiles on profiles.id = events.profile_id
                     WHERE (
                         (events.room_id == ?1) &
                         (type == 'm.room.message') &
                         (msgtype in ({}))
                     ) ORDER BY server_ts DESC LIMIT ?2
                     ",
                    FILE_EVENT_TYPES
                ))?;

                let room_id = Database::get_room_id(connection, &room_id)?;
                let events =
                    stmt.query_map(&vec![&room_id as &dyn ToSql, &(limit as i64)], |row| {
                        Ok((
                            row.get(0)?,
                            Profile {
                                displayname: row.get(1)?,
                                avatar_url: row.get(2)?,
                            },
                        ))
                    })?;
                events.collect()
            }
        }
    }

    /// Load events surounding the given event.
    pub(crate) fn load_event_context(
        connection: &rusqlite::Connection,
        event: &Event,
        before_limit: usize,
        after_limit: usize,
    ) -> rusqlite::Result<EventContext> {
        let mut profiles: HashMap<String, Profile> = HashMap::new();
        let room_id = Database::get_room_id(connection, &event.room_id)?;

        let before = if before_limit == 0 {
            vec![]
        } else {
            let mut stmt = connection.prepare(
                "SELECT source, sender, displayname, avatar_url
                 FROM events
                 INNER JOIN profiles on profiles.id = events.profile_id
                 WHERE (
                     (event_id != ?1) &
                     (room_id == ?2) &
                     (server_ts <= ?3)
                 ) ORDER BY server_ts DESC LIMIT ?4
                 ",
            )?;
            let context = stmt.query_map(
                &vec![
                    &event.event_id as &dyn ToSql,
                    &room_id,
                    &event.server_ts,
                    &(after_limit as i64),
                ],
                |row| {
                    Ok((
                        row.get(0),
                        row.get(1),
                        Profile {
                            displayname: row.get(2)?,
                            avatar_url: row.get(3)?,
                        },
                    ))
                },
            )?;
            let mut ret: Vec<String> = Vec::new();

            for row in context {
                let (source, sender, profile) = row?;
                profiles.insert(sender?, profile);
                ret.push(source?)
            }

            ret
        };

        let after = if after_limit == 0 {
            vec![]
        } else {
            let mut stmt = connection.prepare(
                "SELECT source, sender, displayname, avatar_url
                 FROM events
                 INNER JOIN profiles on profiles.id = events.profile_id
                 WHERE (
                     (event_id != ?1) &
                     (room_id == ?2) &
                     (server_ts >= ?3)
                 ) ORDER BY server_ts ASC LIMIT ?4
                 ",
            )?;
            let context = stmt.query_map(
                &vec![
                    &event.event_id as &dyn ToSql,
                    &room_id,
                    &event.server_ts,
                    &(after_limit as i64),
                ],
                |row| {
                    Ok((
                        row.get(0),
                        row.get(1),
                        Profile {
                            displayname: row.get(2)?,
                            avatar_url: row.get(3)?,
                        },
                    ))
                },
            )?;

            let mut ret: Vec<String> = Vec::new();

            for row in context {
                let (source, sender, profile) = row?;
                profiles.insert(sender?, profile);
                ret.push(source?)
            }

            ret
        };

        Ok((before, after, profiles))
    }

    pub(crate) fn load_event(
        connection: &rusqlite::Connection,
        room_id: &str,
        event_id: &str,
    ) -> rusqlite::Result<Event> {
        let room_id = Database::get_room_id(connection, &room_id)?;

        connection.query_row(
            "SELECT type, msgtype, event_id, sender,
             server_ts, rooms.room_id, source
             FROM events
             INNER JOIN rooms on rooms.id = events.room_id
             WHERE (events.room_id == ?1) & (event_id == ?2)",
            &[&room_id as &dyn ToSql, &event_id],
            |row| {
                Ok(Event {
                    event_type: row.get(0)?,
                    content_value: "".to_string(),
                    msgtype: row.get(1)?,
                    event_id: row.get(2)?,
                    sender: row.get(3)?,
                    server_ts: row.get(4)?,
                    room_id: row.get(5)?,
                    source: row.get(6)?,
                })
            },
        )
    }

    pub(crate) fn load_events(
        connection: &rusqlite::Connection,
        search_result: &[(f32, EventId)],
        before_limit: usize,
        after_limit: usize,
        order_by_recency: bool,
    ) -> rusqlite::Result<Vec<SearchResult>> {
        if search_result.is_empty() {
            return Ok(vec![]);
        }

        let event_num = search_result.len();
        let parameter_str = std::iter::repeat(", ?")
            .take(event_num - 1)
            .collect::<String>();

        let mut stmt = if order_by_recency {
            connection.prepare(&format!(
                "SELECT type, msgtype, event_id, sender,
                 server_ts, rooms.room_id, source, displayname, avatar_url
                 FROM events
                 INNER JOIN profiles on profiles.id = events.profile_id
                 INNER JOIN rooms on rooms.id = events.room_id
                 WHERE event_id IN (?{})
                 ORDER BY server_ts DESC
                 ",
                &parameter_str
            ))?
        } else {
            connection.prepare(&format!(
                "SELECT type, msgtype, event_id, sender,
                 server_ts, rooms.room_id, source, displayname, avatar_url
                 FROM events
                 INNER JOIN profiles on profiles.id = events.profile_id
                 INNER JOIN rooms on rooms.id = events.room_id
                 WHERE event_id IN (?{})
                 ",
                &parameter_str
            ))?
        };

        let (mut scores, event_ids): (HashMap<String, f32>, Vec<String>) = {
            let mut s = HashMap::new();
            let mut e = Vec::new();

            for (score, id) in search_result {
                e.push(id.clone());
                s.insert(id.clone(), *score);
            }
            (s, e)
        };

        let db_events = stmt.query_map(event_ids, |row| {
            Ok((
                Event {
                    event_type: row.get(0)?,
                    content_value: "".to_string(),
                    msgtype: row.get(1)?,
                    event_id: row.get(2)?,
                    sender: row.get(3)?,
                    server_ts: row.get(4)?,
                    room_id: row.get(5)?,
                    source: row.get(6)?,
                },
                Profile {
                    displayname: row.get(7)?,
                    avatar_url: row.get(8)?,
                },
            ))
        })?;

        let mut events = Vec::new();
        for row in db_events {
            let (event, profile): (Event, Profile) = row?;
            let (before, after, profiles) =
                Database::load_event_context(connection, &event, before_limit, after_limit)?;

            let mut profiles = profiles;
            profiles.insert(event.sender.clone(), profile);

            let result = SearchResult {
                score: scores.remove(&event.event_id).unwrap(),
                event_source: event.source,
                events_before: before,
                events_after: after,
                profile_info: profiles,
            };
            events.push(result);
        }

        // Sqlite orders by recency for us, but if we score by rank sqlite will
        // mess up our order, re-sort our events here.
        if !order_by_recency {
            events.sort_by(|a, b| {
                a.score
                    .partial_cmp(&b.score)
                    .unwrap_or_else(|| Ordering::Equal)
            });
        }

        Ok(events)
    }

    pub(crate) fn replace_crawler_checkpoint(
        connection: &rusqlite::Connection,
        new: Option<&CrawlerCheckpoint>,
        old: Option<&CrawlerCheckpoint>,
    ) -> Result<()> {
        if let Some(checkpoint) = new {
            connection.execute(
                "INSERT OR IGNORE INTO crawlercheckpoints
                (room_id, token, full_crawl, direction) VALUES(?1, ?2, ?3, ?4)",
                &[
                    &checkpoint.room_id,
                    &checkpoint.token,
                    &checkpoint.full_crawl as &dyn ToSql,
                    &checkpoint.direction,
                ],
            )?;
        }

        if let Some(checkpoint) = old {
            connection.execute(
                "DELETE FROM crawlercheckpoints
                WHERE (room_id=?1 AND token=?2 AND full_crawl=?3 AND direction=?4)",
                &[
                    &checkpoint.room_id,
                    &checkpoint.token,
                    &checkpoint.full_crawl as &dyn ToSql,
                    &checkpoint.direction,
                ],
            )?;
        }

        Ok(())
    }
}
