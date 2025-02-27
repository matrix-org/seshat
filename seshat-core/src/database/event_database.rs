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

use std::{
    cmp::Ordering,
    collections::HashMap,
    path::PathBuf,
    sync::mpsc::{channel, Receiver, Sender},
    thread,
};

use rusqlite::{
    params, params_from_iter,
    trace::{TraceEvent, TraceEventCodes},
    ToSql,
};
use web_sys::console;

use rusqlite::Connection;

use crate::{
    config::LoadDirection,
    database::SearchResult,
    error::Result,
    events::{CrawlerCheckpoint, Event, EventContext, EventId, Profile, SerializedEvent},
    index::Writer as IndexWriter,
    Config, Database,
};

#[cfg(test)]
use fake::{Fake, Faker};
#[cfg(test)]
use std::time;
#[cfg(test)]
use tempfile::tempdir;

#[cfg(test)]
use crate::events::CheckpointDirection;
#[cfg(test)]
use crate::{EVENT, TOPIC_EVENT};

const FILE_EVENT_TYPES: &str = "'m.image', 'm.file', 'm.audio', 'm.video'";
const DATABASE_VERSION: i64 = 4;
const EVENTS_DB_NAME: &str = "events.db";

/// The Seshat database.
pub struct EventDatabase {
    path: PathBuf,
    // shutdown_rx: Receiver<()>,
    tx: Sender<DatabaseMessage>,
}

pub enum DatabaseMessage {
    PendingDelete(EventId, Sender<Result<()>>),
    MarkEventsAsDeleted(Vec<EventId>, Sender<Result<()>>),
    SaveEvent(Event, Profile, Sender<Result<Option<i64>>>),
    ReplaceCrawlerCheckpoint(
        Option<CrawlerCheckpoint>,
        Option<CrawlerCheckpoint>,
        Sender<Result<()>>,
        Sender<Result<()>>,
    ),
    MarkEventsAsIndexed(Vec<i64>, Sender<Result<()>>),
    LoadUncomittedEvents(Sender<Result<Vec<(i64, Event)>>>),
    LoadPendingDeletionEvents(Sender<Result<Vec<EventId>>>),
    ChangePassphrase(String, Sender<Result<()>>),
    LoadEvents(
        Vec<(f32, EventId)>,
        usize,
        usize,
        bool,
        Sender<Result<Vec<SearchResult>>>,
    ),
}

type EventDatabaseThreadRet = (Sender<DatabaseMessage>);

impl EventDatabase {
    pub fn new(path: PathBuf) -> Result<Self> {
        let tx = Self::spawn_database_thread(path.clone());

        Ok(Self {
            path: path,
            // shutdown_rx: shutdown_rx,
            tx: tx,
        })
    }

    pub fn spawn_database_thread(path: PathBuf) -> EventDatabaseThreadRet {
        let (tx, rx): (_, Receiver<DatabaseMessage>) = channel();
        // let (shutdown_tx, shutdown_rx): (_, Receiver<()>) = channel();
        rayon::spawn(move || {
            let db_path = path.join(EVENTS_DB_NAME);
            let mut connection: Connection = Connection::open(db_path.clone()).unwrap();

            fn rusqlite_log(event: TraceEvent) {
                match event {
                    TraceEvent::Stmt(stmt_ref, _) => {
                        let sql = stmt_ref.sql();
                        console::log_1(&format!("SQLite Stmt {sql}").into())
                    }
                    TraceEvent::Profile(stmt_ref, duration) => {
                        let sql = stmt_ref.sql();
                        let millis = duration.as_millis();
                        console::log_1(&format!("SQLite Profile {sql} duration {millis}").into())
                    }
                    TraceEvent::Row(stmt_ref) => {
                        let sql = stmt_ref.sql();
                        console::log_1(&format!("SQLite Row {sql}").into())
                    }
                    TraceEvent::Close(conn_ref) => console::log_1(&format!("SQLite Close").into()),
                    _ => todo!(),
                }
            }

            connection.trace_v2(TraceEventCodes::all(), Some(rusqlite_log));

            // Self::static_unlock(&mut connection, config);
            Self::set_pragmas(&mut connection);

            let (version, reindex_needed) = match Self::get_version(&mut connection) {
                Ok(ret) => ret,
                Err(e) => return, //TODO handle Err(Error::DatabaseOpenError(e.to_string())),
            };

            Self::create_tables(&mut connection);

            // if version != DATABASE_VERSION {
            //     return Err(Error::DatabaseVersionError);
            // }

            // if reindex_needed {
            //     return Err(Error::ReindexError);
            // }
            while let Ok(message) = rx.recv() {
                match message {
                    DatabaseMessage::PendingDelete(event_id, sender) => {
                        let ret = Self::static_pending_delete(&mut connection, event_id);
                        sender.send(ret).unwrap();
                    }
                    DatabaseMessage::MarkEventsAsDeleted(events, sender) => {
                        let ret = Self::static_mark_events_as_deleted(&mut connection, events);
                        sender.send(ret).unwrap();
                    }
                    DatabaseMessage::SaveEvent(event, profile, sender) => {
                        let ret = Self::static_save_event(&mut connection, event, profile);
                        sender.send(ret).unwrap();
                    }
                    DatabaseMessage::ReplaceCrawlerCheckpoint(
                        new_checkpoint,
                        old_checkpoint,
                        write_events_sender,
                        result_sender,
                    ) => {
                        let write_events = Box::new(|| {
                            write_events_sender.send(Ok(())).unwrap();
                        });
                        let ret = Self::static_replace_crawler(
                            &mut connection,
                            new_checkpoint,
                            old_checkpoint,
                            write_events,
                        );
                        result_sender.send(ret).unwrap();
                    }
                    DatabaseMessage::MarkEventsAsIndexed(events, sender) => {
                        let ret = Self::static_mark_events_as_indexed(&mut connection, events);
                        sender.send(ret).unwrap();
                    }
                    DatabaseMessage::LoadUncomittedEvents(sender) => {
                        let ret = Self::static_load_uncommitted_events(&mut connection);
                        sender.send(Ok(ret.unwrap())).unwrap();
                    }
                    DatabaseMessage::LoadPendingDeletionEvents(sender) => {
                        let ret = Self::static_load_pending_deletion_events(&mut connection);
                        sender.send(Ok(ret.unwrap())).unwrap();
                    }
                    DatabaseMessage::ChangePassphrase(new_passphrase, sender) => {
                        let ret = Self::static_change_passphrase(&mut connection, new_passphrase);
                        sender.send(Ok(ret.unwrap())).unwrap();
                    }
                    DatabaseMessage::LoadEvents(
                        search_results,
                        before_limit,
                        after_limit,
                        order_by_recency,
                        sender,
                    ) => {
                        let ret = Self::static_load_events(
                            &mut connection,
                            &search_results,
                            before_limit,
                            after_limit,
                            order_by_recency,
                        );
                        sender.send(Ok(ret.unwrap())).unwrap();
                    }
                }
            }
            return;
        });

        return tx;
    }

    pub fn pending_delete(&self, event_id: EventId) -> Result<()> {
        let (sender, receiver): (_, Receiver<Result<()>>) = channel();
        let message = DatabaseMessage::PendingDelete(event_id, sender);
        self.tx.send(message).unwrap();
        receiver.recv().unwrap()
    }

    pub fn mark_events_as_deleted(&self, events: Vec<EventId>) -> Result<()> {
        let (sender, receiver): (_, Receiver<Result<()>>) = channel();
        let message = DatabaseMessage::MarkEventsAsDeleted(events, sender);
        self.tx.send(message).unwrap();
        receiver.recv().unwrap()
    }

    pub fn save_event(&self, event: Event, profile: Profile) -> Result<Option<i64>> {
        let (sender, receiver): (_, Receiver<Result<Option<i64>>>) = channel();
        let message = DatabaseMessage::SaveEvent(event, profile, sender);
        self.tx.send(message).unwrap();
        receiver.recv().unwrap()
    }

    pub fn replace_crawler<'a>(
        &self,
        new_checkpoint: Option<CrawlerCheckpoint>,
        old_checkpoint: Option<CrawlerCheckpoint>,
        write_events: Box<dyn FnOnce() + Send + 'a>,
    ) -> Result<()> {
        let (write_events_sender, write_events_receiver): (_, Receiver<Result<()>>) = channel();
        let (result_sender, result_receiver): (_, Receiver<Result<()>>) = channel();
        let message = DatabaseMessage::ReplaceCrawlerCheckpoint(
            new_checkpoint,
            old_checkpoint,
            write_events_sender,
            result_sender,
        );
        self.tx.send(message).unwrap();
        write_events_receiver.recv().unwrap();
        write_events();
        result_receiver.recv().unwrap()
    }

    pub fn mark_events_as_indexed(&self, events: Vec<i64>) -> Result<()> {
        let (sender, receiver): (_, Receiver<Result<()>>) = channel();
        let message = DatabaseMessage::MarkEventsAsIndexed(events, sender);
        self.tx.send(message).unwrap();
        receiver.recv().unwrap()
    }

    pub(crate) fn load_uncommitted_events(&self) -> Result<Vec<(i64, Event)>> {
        let (sender, receiver): (_, Receiver<Result<Vec<(i64, Event)>>>) = channel();
        let message = DatabaseMessage::LoadUncomittedEvents(sender);
        self.tx.send(message).unwrap();
        receiver.recv().unwrap()
    }

    pub(crate) fn load_pending_deletion_events(&self) -> Result<Vec<EventId>> {
        let (sender, receiver): (_, Receiver<Result<Vec<EventId>>>) = channel();
        let message = DatabaseMessage::LoadPendingDeletionEvents(sender);
        self.tx.send(message).unwrap();
        receiver.recv().unwrap()
    }

    pub(crate) fn change_passphrase(&self, new_passphrase: &str) -> Result<()> {
        let (sender, receiver): (_, Receiver<Result<()>>) = channel();
        let message = DatabaseMessage::ChangePassphrase(new_passphrase.to_string(), sender);
        self.tx.send(message).unwrap();
        receiver.recv().unwrap()
    }

    pub(crate) fn load_events(
        &self,
        search_result: &[(f32, EventId)],
        before_limit: usize,
        after_limit: usize,
        order_by_recency: bool,
    ) -> Result<Vec<SearchResult>> {
        let (sender, receiver): (_, Receiver<Result<Vec<SearchResult>>>) = channel();
        let message = DatabaseMessage::LoadEvents(
            search_result.to_vec(),
            before_limit,
            after_limit,
            order_by_recency,
            sender,
        );
        self.tx.send(message).unwrap();
        receiver.recv().unwrap()
    }

    pub fn static_replace_crawler<'a>(
        connection: &mut rusqlite::Connection,
        new_checkpoint: Option<CrawlerCheckpoint>,
        old_checkpoint: Option<CrawlerCheckpoint>,
        write_events: Box<dyn FnOnce() + 'a>,
    ) -> Result<()> {
        let transaction = connection.transaction()?;
        write_events();
        Self::replace_crawler_checkpoint(
            &transaction,
            new_checkpoint.as_ref(),
            old_checkpoint.as_ref(),
        )?;
        transaction.commit()?;
        return Ok(());
    }

    /// Delete non committed events from the database that were committed
    pub(crate) fn static_mark_events_as_indexed(
        connection: &mut rusqlite::Connection,
        events: Vec<i64>,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let transaction = connection.transaction()?;

        for chunk in events.chunks(50) {
            let parameter_str = ", ?".repeat(chunk.len() - 1);

            let mut stmt = transaction.prepare(&format!(
                "DELETE from uncommitted_events
                     WHERE id IN (?{})",
                &parameter_str
            ))?;

            stmt.execute(params_from_iter(chunk))?;
        }

        transaction.commit()?;
        Ok(())
    }

    pub fn static_pending_delete(
        connection: &mut rusqlite::Connection,
        event_id: EventId,
    ) -> Result<()> {
        let transaction = connection.transaction()?;

        Self::delete_event_by_id(&transaction, &event_id)?;
        transaction.execute(
            "INSERT OR IGNORE INTO pending_deletion_events (event_id) VALUES (?1)",
            [&event_id],
        )?;
        transaction.commit().unwrap();
        return Ok(());
    }

    pub(crate) fn static_mark_events_as_deleted(
        connection: &mut rusqlite::Connection,
        events: Vec<EventId>,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let transaction = connection.transaction()?;

        for chunk in events.chunks(50) {
            let parameter_str = ", ?".repeat(chunk.len() - 1);

            let mut stmt = transaction.prepare(&format!(
                "DELETE from pending_deletion_events
                     WHERE event_id IN (?{})",
                &parameter_str
            ))?;

            stmt.execute(params_from_iter(chunk))?;
        }

        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn static_save_event(
        connection: &rusqlite::Connection,
        event: Event,
        profile: Profile,
    ) -> Result<Option<i64>> {
        if Self::event_in_store(connection, &event)? {
            return Ok(None);
        }

        let ret = Self::save_profile(connection, &event.sender, &profile);
        let profile_id = match ret {
            Ok(p) => p,
            Err(e) => match e {
                rusqlite::Error::NulError(..) => {
                    // A nul error is thrown because the string contains nul
                    // bytes and converting it to a C string isn't possible this
                    // way. This is likely some string containing malicious nul
                    // bytes so we filter them out.
                    let mut profile_mut = profile.clone();
                    profile_mut.displayname = profile_mut
                        .displayname
                        .as_mut()
                        .map(|d| d.replace('\0', ""));
                    profile_mut.avatar_url =
                        profile_mut.avatar_url.as_mut().map(|u| u.replace('\0', ""));
                    Self::save_profile(connection, &event.sender, &profile_mut)?
                }
                _ => return Err(e.into()),
            },
        };
        let ret = Self::save_event_helper(connection, &event, profile_id);

        let event_id = match ret {
            Ok(e) => e,
            Err(e) => match e {
                rusqlite::Error::NulError(..) => {
                    // Same deal for the event, but we need to delete the event
                    // in case a transaction was used. Sqlite will otherwise
                    // complain about the unique constraint that we have for
                    // the event id.
                    Self::delete_event_by_id(connection, &event.event_id)?;
                    let mut event_mut = event.clone();
                    event_mut.content_value = event_mut.content_value.replace('\0', "");
                    event_mut.msgtype = event_mut.msgtype.as_mut().map(|m| m.replace('\0', ""));
                    Self::save_event_helper(connection, &event_mut, profile_id)?
                }
                _ => return Err(e.into()),
            },
        };

        Ok(Some(event_id))
    }

    fn set_pragmas(connection: &Connection) -> Result<()> {
        connection.pragma_update(None, "foreign_keys", &1 as &dyn ToSql)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        Ok(())
    }

    pub(crate) fn get_user_version(connection: &rusqlite::Connection) -> Result<i64> {
        Ok(connection.query_row("SELECT version FROM user_version", [], |row| row.get(0))?)
    }

    pub(crate) fn set_user_version(connection: &rusqlite::Connection, version: i64) -> Result<()> {
        connection.execute("UPDATE user_version SET version = ?", [version])?;
        Ok(())
    }

    pub(crate) fn get_version(connection: &mut rusqlite::Connection) -> Result<(i64, bool)> {
        connection.execute(
            "CREATE TABLE IF NOT EXISTS version (
                id INTEGER NOT NULL PRIMARY KEY CHECK (id = 1),
                version INTEGER NOT NULL
            )",
            [],
        )?;

        connection.execute(
            "CREATE TABLE IF NOT EXISTS reindex_needed (
                id INTEGER NOT NULL PRIMARY KEY CHECK (id = 1),
                reindex_needed BOOL NOT NULL
            )",
            [],
        )?;

        connection.execute(
            "INSERT OR IGNORE INTO reindex_needed ( reindex_needed ) VALUES(?1)",
            [false],
        )?;

        connection.execute(
            "INSERT OR IGNORE INTO version ( version ) VALUES(?1)",
            [DATABASE_VERSION],
        )?;

        let mut version: i64 =
            connection.query_row("SELECT version FROM version", [], |row| row.get(0))?;

        let mut reindex_needed: bool =
            connection.query_row("SELECT reindex_needed FROM reindex_needed", [], |row| {
                row.get(0)
            })?;

        // Do database migrations here before bumping the database version.

        if version == 1 {
            // rusqlite claims that this execute call returns rows even though
            // it does not, running it using query() fails as well. We catch the
            // error and check if it's the ExecuteReturnedResults error, if it
            // is we safely ignore it.
            let result = connection.execute("ALTER TABLE profiles RENAME TO profile", []);
            match result {
                Ok(_) => (),
                Err(e) => match e {
                    rusqlite::Error::ExecuteReturnedResults => (),
                    _ => return Err(e.into()),
                },
            }
            connection.execute("UPDATE version SET version = '2'", [])?;

            version = 2;
        }

        if version == 2 {
            let transaction = connection.transaction()?;

            transaction.execute("UPDATE reindex_needed SET reindex_needed = ?1", [true])?;
            transaction.execute("UPDATE version SET version = '3'", [])?;
            transaction.commit()?;

            reindex_needed = true;
            version = 3;
        }

        if version == 3 {
            let transaction = connection.transaction()?;

            transaction.execute("UPDATE reindex_needed SET reindex_needed = ?1", [true])?;
            transaction.execute("UPDATE version SET version = '4'", [])?;
            transaction.commit()?;

            reindex_needed = true;
            version = 4;
        }

        Ok((version, reindex_needed))
    }

    pub(crate) fn create_tables(conn: &rusqlite::Connection) -> Result<()> {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS profile (
                id INTEGER NOT NULL PRIMARY KEY,
                user_id TEXT NOT NULL,
                displayname TEXT NOT NULL,
                avatar_url TEXT NOT NULL,
                UNIQUE(user_id,displayname,avatar_url)
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS rooms (
                id INTEGER NOT NULL PRIMARY KEY,
                room_id TEXT NOT NULL,
                UNIQUE(room_id)
            )",
            [],
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
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS uncommitted_events (
                id INTEGER NOT NULL PRIMARY KEY,
                event_id INTEGER NOT NULL,
                content_value TEXT NOT NULL,
                FOREIGN KEY (event_id) REFERENCES events (id),
                UNIQUE(event_id)
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS pending_deletion_events (
                id INTEGER NOT NULL PRIMARY KEY,
                event_id TEXT NOT NULL,
                UNIQUE(event_id)
            )",
            [],
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
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS event_profile_id ON events (profile_id)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS room_events_by_timestamp ON events (room_id, server_ts DESC, event_id)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS event_id ON events (event_id)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS room_events ON events (room_id, type, msgtype)",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS user_version (
                id INTEGER NOT NULL PRIMARY KEY CHECK (id = 1),
                version INTEGER NOT NULL
            )",
            [],
        )?;

        conn.execute(
            "INSERT OR IGNORE INTO user_version ( version ) VALUES(?1)",
            [0],
        )?;

        Ok(())
    }

    pub(crate) fn get_event_count(connection: &rusqlite::Connection) -> rusqlite::Result<i64> {
        connection.query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
    }

    pub(crate) fn get_event_count_for_room(
        connection: &rusqlite::Connection,
        room_id: &str,
    ) -> rusqlite::Result<i64> {
        let room_id = Self::get_room_id(connection, room_id)?;
        connection.query_row(
            "SELECT COUNT(*) FROM events WHERE room_id=?1",
            [room_id],
            |row| row.get(0),
        )
    }

    pub(crate) fn get_room_count(connection: &rusqlite::Connection) -> rusqlite::Result<i64> {
        // TODO once we support upgraded rooms we should return only leaf rooms
        // here, rooms that are not ancestors to another one.
        connection.query_row("SELECT COUNT(*) FROM rooms", [], |row| row.get(0))
    }

    pub(crate) fn save_profile(
        connection: &rusqlite::Connection,
        user_id: &str,
        profile: &Profile,
    ) -> rusqlite::Result<i64> {
        let displayname = profile.displayname.as_ref();
        let avatar_url = profile.avatar_url.as_ref();

        // unwrap_or_default doesn't work on references sadly.
        let displayname = if let Some(d) = displayname { d } else { "" };

        let avatar_url = if let Some(a) = avatar_url { a } else { "" };

        connection.execute(
            "
            INSERT OR IGNORE INTO profile (
                user_id, displayname, avatar_url
            ) VALUES(?1, ?2, ?3)",
            [user_id, displayname, avatar_url],
        )?;

        let profile_id: i64 = connection.query_row(
            "
            SELECT id FROM profile WHERE (
                user_id=?1
                and displayname=?2
                and avatar_url=?3)",
            [user_id, displayname, avatar_url],
            |row| row.get(0),
        )?;

        Ok(profile_id)
    }

    #[cfg(test)]
    pub(crate) fn load_profile(connection: &Connection, profile_id: i64) -> Result<Profile> {
        let profile = connection.query_row(
            "SELECT displayname, avatar_url FROM profile WHERE id=?1",
            [profile_id],
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
        connection.execute("INSERT OR IGNORE INTO rooms (room_id) VALUES(?1)", [room])?;

        let room_id: i64 =
            connection.query_row("SELECT id FROM rooms WHERE (room_id=?1)", [room], |row| {
                row.get(0)
            })?;

        Ok(room_id)
    }

    pub(crate) fn static_load_pending_deletion_events(
        connection: &rusqlite::Connection,
    ) -> rusqlite::Result<Vec<EventId>> {
        let mut stmt = connection.prepare("SELECT event_id from pending_deletion_events")?;
        let events = stmt.query_map([], |row| row.get(0))?;
        events.collect()
    }

    pub(crate) fn static_load_uncommitted_events(
        connection: &rusqlite::Connection,
    ) -> rusqlite::Result<Vec<(i64, Event)>> {
        console::log_1(&"!static load_unprocessed_events ret".into());
        let mut stmt = connection.prepare(
                "SELECT uncommitted_events.id, uncommitted_events.event_id, content_value, type, msgtype,
                 events.event_id, sender, server_ts, rooms.room_id, source
                 FROM uncommitted_events
                 INNER JOIN events on events.id = uncommitted_events.event_id
                 INNER JOIN rooms on rooms.id = events.room_id
                 ")?;

        console::log_1(&"!static load_unprocessed_events ret2".into());
        let events = stmt.query_map([], |row| {
            let a: i64 = row.get(0)?;
            Ok((
                a,
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
        console::log_1(&"!static load_unprocessed_events ret3".into());
        // return Ok(vec![]);
        let es = events.collect();
        console::log_1(&format!("static load_unprocessed_events events: {es:?}").into());
        es
    }

    pub(crate) fn save_event_helper(
        connection: &rusqlite::Connection,
        event: &Event,
        profile_id: i64,
    ) -> rusqlite::Result<i64> {
        let room_id = Self::get_room_id(connection, &event.room_id)?;

        let mut statement = connection.prepare(
            "
            INSERT INTO events (
                event_id, sender, server_ts, room_id, type,
                msgtype, source, profile_id
            ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )?;

        let event_id = statement.insert([
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

        let id = stmt.insert([&event_id as &dyn ToSql, &event.content_value])?;

        Ok(id)
    }

    pub(crate) fn delete_event_by_id(
        connection: &rusqlite::Connection,
        event_id: &str,
    ) -> rusqlite::Result<usize> {
        connection.execute("DELETE from events WHERE event_id == ?1", [event_id])
    }

    pub(crate) fn event_in_store(
        connection: &rusqlite::Connection,
        event: &Event,
    ) -> rusqlite::Result<bool> {
        let room_id = Self::get_room_id(connection, &event.room_id)?;
        let count: i64 = connection.query_row(
            "
            SELECT COUNT(*) FROM events WHERE (
                event_id=?1
                and room_id=?2)",
            [&event.event_id, &room_id as &dyn ToSql],
            |row| row.get(0),
        )?;

        match count {
            0 => Ok(false),
            1 => Ok(true),
            // This is fine becauese event_id and room_id are a unique pair in
            // our events table.
            _ => unreachable!(),
        }
    }

    pub(crate) fn load_all_events(
        connection: &rusqlite::Connection,
        limit: usize,
        from_event: Option<&Event>,
    ) -> rusqlite::Result<Vec<SerializedEvent>> {
        match from_event {
            Some(event) => {
                let mut stmt = connection.prepare(
                    "SELECT source FROM events
                     WHERE (
                         (type == 'm.room.message') &
                         (event_id != ?1) &
                         (server_ts <= ?2)
                     ) ORDER BY server_ts DESC LIMIT ?3
                     ",
                )?;

                let events = stmt
                    .query_map(params![&event.event_id, &event.server_ts, &limit,], |row| {
                        row.get(0)
                    })?;
                events.collect()
            }
            None => {
                let mut stmt = connection.prepare(
                    "SELECT source FROM events
                     WHERE type == 'm.room.message'
                     ORDER BY server_ts DESC LIMIT ?1
                     ",
                )?;

                let events = stmt.query_map([limit], |row| row.get(0))?;
                events.collect()
            }
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
                let event = Self::load_event(connection, room_id, e)?;

                let (direction, sort) = match direction {
                    LoadDirection::Backwards => ("<=", "DESC"),
                    LoadDirection::Forwards => (">=", "ASC"),
                };

                let mut stmt = connection.prepare(&format!(
                    "SELECT source, displayname, avatar_url
                     FROM events
                     INNER JOIN profile on profile.id = events.profile_id
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

                let room_id = Self::get_room_id(connection, room_id)?;
                let events = stmt.query_map(
                    params![&room_id, &event.event_id, &event.server_ts, &limit,],
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
                     INNER JOIN profile on profile.id = events.profile_id
                     WHERE (
                         (events.room_id == ?1) &
                         (type == 'm.room.message') &
                         (msgtype in ({}))
                     ) ORDER BY server_ts DESC LIMIT ?2
                     ",
                    FILE_EVENT_TYPES
                ))?;

                let room_id = Self::get_room_id(connection, room_id)?;
                let events = stmt.query_map(params![room_id, limit], |row| {
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
        let room_id = Self::get_room_id(connection, &event.room_id)?;

        let before = if before_limit == 0 {
            vec![]
        } else {
            let mut stmt = connection.prepare(
                "
                WITH room_events AS (
                    SELECT *
                    FROM events
                    WHERE room_id == ?2
                )
                SELECT source, sender, displayname, avatar_url
                FROM room_events
                INNER JOIN profile on profile.id = room_events.profile_id
                WHERE (
                    (event_id != ?1) &
                    (server_ts <= ?3)
                ) ORDER BY server_ts DESC LIMIT ?4
                ",
            )?;
            let context = stmt.query_map(
                params![&event.event_id, &room_id, &event.server_ts, &after_limit,],
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
                "
                WITH room_events AS (
                    SELECT *
                    FROM events
                    WHERE room_id == ?2
                )
                SELECT source, sender, displayname, avatar_url
                FROM room_events
                INNER JOIN profile on profile.id = room_events.profile_id
                WHERE (
                    (event_id != ?1) &
                    (server_ts >= ?3)
                ) ORDER BY server_ts ASC LIMIT ?4
                ",
            )?;
            let context = stmt.query_map(
                params![&event.event_id, &room_id, &event.server_ts, &after_limit,],
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
        let room_id = Self::get_room_id(connection, room_id)?;

        connection.query_row(
            "SELECT type, msgtype, event_id, sender,
             server_ts, rooms.room_id, source
             FROM events
             INNER JOIN rooms on rooms.id = events.room_id
             WHERE (events.room_id == ?1) & (event_id == ?2)",
            [&room_id as &dyn ToSql, &event_id],
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

    pub(crate) fn static_load_events(
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
        let parameter_str = ", ?".repeat(event_num - 1);

        let mut stmt = if order_by_recency {
            connection.prepare(&format!(
                "SELECT type, msgtype, event_id, sender,
                 server_ts, rooms.room_id, source, displayname, avatar_url
                 FROM events
                 INNER JOIN profile on profile.id = events.profile_id
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
                 INNER JOIN profile on profile.id = events.profile_id
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

        let db_events = stmt.query_map(params_from_iter(event_ids), |row| {
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
                Self::load_event_context(connection, &event, before_limit, after_limit)?;

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
            events.sort_by(|a, b| a.score.partial_cmp(&b.score).unwrap_or(Ordering::Equal));
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
                [
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
                [
                    &checkpoint.room_id,
                    &checkpoint.token,
                    &checkpoint.full_crawl as &dyn ToSql,
                    &checkpoint.direction,
                ],
            )?;
        }

        Ok(())
    }

    pub(crate) fn static_change_passphrase(
        connection: &rusqlite::Connection,
        new_passphrase: String,
    ) -> Result<()> {
        connection.pragma_update(None, "rekey", &new_passphrase as &dyn ToSql)?;
        Ok(())
    }

    pub(crate) fn unlock(connection: &rusqlite::Connection, config: &Config) -> Result<()> {
        let passphrase: &String = if let Some(ref p) = config.passphrase {
            p
        } else {
            return Ok(());
        };

        let mut statement = connection.prepare("PRAGMA cipher_version")?;
        let results = statement.query_map([], |row| row.get::<usize, String>(0))?;

        if results.count() != 1 {
            return Err(crate::Error::SqlCipherError(
                "Sqlcipher support is missing".to_string(),
            ));
        }

        connection.pragma_update(None, "key", passphrase as &dyn ToSql)?;

        let count: std::result::Result<i64, rusqlite::Error> =
            connection.query_row("SELECT COUNT(*) FROM sqlite_master", [], |row| row.get(0));

        match count {
            Ok(_) => Ok(()),
            Err(_) => Err(crate::Error::DatabaseUnlockError(
                "Invalid passphrase".to_owned(),
            )),
        }
    }
}

#[test]
fn create_event_db() {
    let tmpdir = tempdir().unwrap();
    let _db = Database::new(tmpdir.path()).unwrap();
}

// #[test]
// fn store_profile() {
//     let tmpdir = tempdir().unwrap();
//     let db = Database::new(tmpdir.path()).unwrap();

//     let profile = Profile::new("Alice", "");

//     let id = EventDatabase::save_profile(
//         &db.connection.lock().unwrap(),
//         "@alice.example.org",
//         &profile,
//     );
//     assert_eq!(id.unwrap(), 1);

//     let id = EventDatabase::save_profile(
//         &db.connection.lock().unwrap(),
//         "@alice.example.org",
//         &profile,
//     );
//     assert_eq!(id.unwrap(), 1);

//     let profile_new = Profile::new("Alice", "mxc://some_url");

//     let id = EventDatabase::save_profile(
//         &db.connection.lock().unwrap(),
//         "@alice.example.org",
//         &profile_new,
//     );
//     assert_eq!(id.unwrap(), 2);
// }

// #[test]
// fn store_empty_profile() {
//     let tmpdir = tempdir().unwrap();
//     let db = Database::new(tmpdir.path()).unwrap();

//     let profile = Profile {
//         displayname: None,
//         avatar_url: None,
//     };
//     let id = EventDatabase::save_profile(
//         &db.connection.lock().unwrap(),
//         "@alice.example.org",
//         &profile,
//     );
//     assert_eq!(id.unwrap(), 1);
// }

// #[test]
// fn store_event() {
//     let tmpdir = tempdir().unwrap();
//     let db = Database::new(tmpdir.path()).unwrap();
//     let profile = Profile::new("Alice", "");
//     let id = EventDatabase::save_profile(
//         &db.connection.lock().unwrap(),
//         "@alice.example.org",
//         &profile,
//     )
//     .unwrap();

//     let mut event = EVENT.clone();
//     let id =
//         EventDatabase::save_event_helper(&db.connection.lock().unwrap(), &mut event, id).unwrap();
//     assert_eq!(id, 1);
// }

// #[test]
// fn store_event_and_profile() {
//     let tmpdir = tempdir().unwrap();
//     let db = Database::new(tmpdir.path()).unwrap();
//     let profile = Profile::new("Alice", "");
//     let event = EVENT.clone();
//     EventDatabase::static_save_event(&db.connection.lock().unwrap(), event, profile).unwrap();
// }

// #[test]
// fn load_event() {
//     let tmpdir = tempdir().unwrap();
//     let db = Database::new(tmpdir.path()).unwrap();
//     let profile = Profile::new("Alice", "");

//     let event = EVENT.clone();
//     EventDatabase::static_save_event(&db.connection.lock().unwrap(), event, profile).unwrap();
//     let events = EventDatabase::load_events(
//         &db.connection.lock().unwrap(),
//         &[
//             (1.0, "$15163622445EBvZJ:localhost".to_string()),
//             (0.3, "$FAKE".to_string()),
//         ],
//         0,
//         0,
//         false,
//     )
//     .unwrap();

//     assert_eq!(*EVENT.source, events[0].event_source)
// }

// #[test]
// fn commit_a_write() {
//     let tmpdir = tempdir().unwrap();
//     let mut db = Database::new(tmpdir.path()).unwrap();
//     db.commit().unwrap();
// }

// #[test]
// fn save_the_event_multithreaded() {
//     let tmpdir = tempdir().unwrap();
//     let mut db = Database::new(tmpdir.path()).unwrap();
//     let profile = Profile::new("Alice", "");

//     db.add_event(EVENT.clone(), profile);
//     db.commit().unwrap();
//     db.reload().unwrap();

//     let events = EventDatabase::load_events(
//         &db.connection.lock().unwrap(),
//         &[
//             (1.0, "$15163622445EBvZJ:localhost".to_string()),
//             (0.3, "$FAKE".to_string()),
//         ],
//         0,
//         0,
//         false,
//     )
//     .unwrap();

//     assert_eq!(*EVENT.source, events[0].event_source)
// }

// #[test]
// fn load_a_profile() {
//     let tmpdir = tempdir().unwrap();
//     let db = Database::new(tmpdir.path()).unwrap();

//     let profile = Profile::new("Alice", "");
//     let user_id = "@alice.example.org";
//     let profile_id =
//         EventDatabase::save_profile(&db.connection.lock().unwrap(), user_id, &profile).unwrap();

//     let loaded_profile =
//         EventDatabase::load_profile(&db.connection.lock().unwrap(), profile_id).unwrap();

//     assert_eq!(profile, loaded_profile);
// }

// #[test]
// fn load_event_context() {
//     let tmpdir = tempdir().unwrap();
//     let mut db = Database::new(tmpdir.path()).unwrap();
//     let profile = Profile::new("Alice", "");

//     db.add_event(EVENT.clone(), profile.clone());

//     let mut before_event = None;

//     for i in 1..6 {
//         let mut event: Event = Faker.fake();
//         event.server_ts = EVENT.server_ts - i;
//         event.source = format!("Hello before event {}", i);

//         if before_event.is_none() {
//             before_event = Some(event.clone());
//         }

//         db.add_event(event, profile.clone());
//     }

//     let mut after_event = None;

//     for i in 1..6 {
//         let mut event: Event = Faker.fake();
//         event.server_ts = EVENT.server_ts + i;
//         event.source = format!("Hello after event {}", i);

//         if after_event.is_none() {
//             after_event = Some(event.clone());
//         }

//         db.add_event(event, profile.clone());
//     }

//     db.commit().unwrap();

//     for i in 1..5 {
//         let (before, after, _) =
//             EventDatabase::load_event_context(&db.connection.lock().unwrap(), &EVENT, 1, 1)
//                 .unwrap();

//         if (before.len() != 1
//             || after.len() != 1
//             || before[0] != before_event.as_ref().unwrap().source
//             || after[0] != after_event.as_ref().unwrap().source)
//             && i != 10
//         {
//             thread::sleep(time::Duration::from_millis(10));
//             continue;
//         }

//         assert_eq!(before.len(), 1);
//         assert_eq!(before[0], before_event.as_ref().unwrap().source);
//         assert_eq!(after.len(), 1);
//         assert_eq!(after[0], after_event.as_ref().unwrap().source);

//         return;
//     }
// }

// #[test]
// fn save_and_load_checkpoints() {
//     let tmpdir = tempdir().unwrap();
//     let db = Database::new(tmpdir.path()).unwrap();

//     let checkpoint = CrawlerCheckpoint {
//         room_id: "!test:room".to_string(),
//         token: "1234".to_string(),
//         full_crawl: false,
//         direction: CheckpointDirection::Backwards,
//     };

//     let mut connection = db.get_connection();
//     let transaction = connection.transaction().unwrap();

//     Database::replace_crawler_checkpoint(&transaction, Some(&checkpoint), None).unwrap();
//     transaction.commit().unwrap();

//     let checkpoints = connection.load_checkpoints().unwrap();

//     println!("{:?}", checkpoints);

//     assert!(checkpoints.contains(&checkpoint));

//     let new_checkpoint = CrawlerCheckpoint {
//         room_id: "!test:room".to_string(),
//         token: "12345".to_string(),
//         full_crawl: false,
//         direction: CheckpointDirection::Backwards,
//     };

//     Database::replace_crawler_checkpoint(&connection, Some(&new_checkpoint), Some(&checkpoint))
//         .unwrap();

//     let checkpoints = connection.load_checkpoints().unwrap();

//     assert!(!checkpoints.contains(&checkpoint));
//     assert!(checkpoints.contains(&new_checkpoint));
// }

// #[test]
// fn duplicate_empty_profiles() {
//     let tmpdir = tempdir().unwrap();
//     let db = Database::new(tmpdir.path()).unwrap();
//     let profile = Profile {
//         displayname: None,
//         avatar_url: None,
//     };
//     let user_id = "@alice.example.org";

//     let first_id =
//         Database::save_profile(&db.connection.lock().unwrap(), user_id, &profile).unwrap();
//     let second_id =
//         Database::save_profile(&db.connection.lock().unwrap(), user_id, &profile).unwrap();

//     assert_eq!(first_id, second_id);

//     let connection = db.connection.lock().unwrap();

//     let mut stmt = connection
//         .prepare("SELECT id FROM profile WHERE user_id=?1")
//         .unwrap();

//     let profile_ids = stmt.query_map([user_id], |row| row.get(0)).unwrap();

//     let mut id_count = 0;

//     for row in profile_ids {
//         let _profile_id: i64 = row.unwrap();
//         id_count += 1;
//     }

//     assert_eq!(id_count, 1);
// }

// #[test]
// fn is_empty() {
//     let tmpdir = tempdir().unwrap();
//     let mut db = Database::new(tmpdir.path()).unwrap();
//     let connection = db.get_connection().unwrap();
//     assert!(connection.is_empty().unwrap());

//     let profile = Profile::new("Alice", "");
//     db.add_event(EVENT.clone(), profile);
//     db.commit().unwrap();
//     assert!(!connection.is_empty().unwrap());
// }

// #[cfg(feature = "encryption")]
// #[test]
// fn encrypted_db() {
//     let tmpdir = tempdir().unwrap();
//     let db_config = Config::new().set_passphrase("test");
//     let mut db = match Database::new_with_config(tmpdir.path(), &db_config) {
//         Ok(db) => db,
//         Err(e) => panic!("Coulnd't open encrypted database {}", e),
//     };

//     let connection = match db.get_connection() {
//         Ok(c) => c,
//         Err(e) => panic!("Could not get database connection {}", e),
//     };

//     assert!(
//         connection.is_empty().unwrap(),
//         "New database should be empty"
//     );

//     let profile = Profile::new("Alice", "");
//     db.add_event(EVENT.clone(), profile);

//     match db.commit() {
//         Ok(_) => (),
//         Err(e) => panic!("Could not commit events to database {}", e),
//     }
//     assert!(
//         !connection.is_empty().unwrap(),
//         "Database shouldn't be empty anymore"
//     );

//     drop(db);

//     let db = Database::new(tmpdir.path());
//     assert!(
//         db.is_err(),
//         "opening the database without a passphrase should fail"
//     );
// }

// #[cfg(feature = "encryption")]
// #[test]
// fn change_passphrase() {
//     let tmpdir = tempdir().unwrap();
//     let db_config = Config::new().set_passphrase("test");
//     let mut db = match Database::new_with_config(tmpdir.path(), &db_config) {
//         Ok(db) => db,
//         Err(e) => panic!("Coulnd't open encrypted database {}", e),
//     };

//     let connection = db
//         .get_connection()
//         .expect("Could not get database connection");
//     assert!(
//         connection.is_empty().unwrap(),
//         "New database should be empty"
//     );

//     let profile = Profile::new("Alice", "");
//     db.add_event(EVENT.clone(), profile);

//     db.commit().expect("Could not commit events to database");
//     db.change_passphrase("wordpass")
//         .expect("Could not change the database passphrase");

//     let db_config = Config::new().set_passphrase("wordpass");
//     let db = Database::new_with_config(tmpdir.path(), &db_config)
//         .expect("Could not open database with the new passphrase");
//     let connection = db
//         .get_connection()
//         .expect("Could not get database connection");
//     assert!(
//         !connection.is_empty().unwrap(),
//         "Database shouldn't be empty anymore"
//     );
//     drop(db);

//     let db_config = Config::new().set_passphrase("test");
//     let db = Database::new_with_config(tmpdir.path(), &db_config);
//     assert!(
//         db.is_err(),
//         "opening the database without a passphrase should fail"
//     );
// }

// #[test]
// fn resume_committing() {
//     let tmpdir = tempdir().unwrap();
//     let mut db = Database::new(tmpdir.path()).unwrap();
//     let profile = Profile::new("Alice", "");

//     // Check that we don't have any uncommitted events.
//     assert!(
//         Database::load_uncommitted_events(&db.connection.lock().unwrap())
//             .unwrap()
//             .is_empty()
//     );

//     db.add_event(EVENT.clone(), profile);
//     db.commit().unwrap();
//     db.reload().unwrap();

//     // Now we do have uncommitted events.
//     assert!(
//         !Database::load_uncommitted_events(&db.connection.lock().unwrap())
//             .unwrap()
//             .is_empty()
//     );

//     // Since the event wasn't committed to the index the search should fail.
//     assert!(db
//         .search("test", &SearchConfig::new())
//         .unwrap()
//         .results
//         .is_empty());

//     // Let us drop the DB to check if we're loading the uncommitted events
//     // correctly.
//     drop(db);
//     let mut counter = 0;
//     let mut db = Database::new(tmpdir.path());

//     // Tantivy might still be in the process of being shut down
//     // and hold on to the write lock. Meaning that opening the database might
//     // not succeed immediately. Retry a couple of times before giving up.
//     while db.is_err() {
//         counter += 1;
//         if counter > 10 {
//             break;
//         }
//         thread::sleep(time::Duration::from_millis(100));
//         db = Database::new(tmpdir.path())
//     }

//     let mut db = db.unwrap();

//     // We still have uncommitted events.
//     assert_eq!(
//         Database::load_uncommitted_events(&db.connection.lock().unwrap()).unwrap()[0].1,
//         *EVENT
//     );

//     db.force_commit().unwrap();
//     db.reload().unwrap();

//     // A forced commit gets rid of our uncommitted events.
//     assert!(
//         Database::load_uncommitted_events(&db.connection.lock().unwrap())
//             .unwrap()
//             .is_empty()
//     );

//     let result = db.search("test", &SearchConfig::new()).unwrap().results;

//     // The search is now successful.
//     assert!(!result.is_empty());
//     assert_eq!(result.len(), 1);
//     assert_eq!(result[0].event_source, EVENT.source);
// }

// #[test]
// fn delete_uncommitted() {
//     let tmpdir = tempdir().unwrap();
//     let mut db = Database::new(tmpdir.path()).unwrap();
//     let profile = Profile::new("Alice", "");

//     for i in 1..1000 {
//         let mut event: Event = Faker.fake();
//         event.server_ts += i;
//         db.add_event(event, profile.clone());

//         if i % 100 == 0 {
//             db.commit().unwrap();
//         }
//     }

//     db.force_commit().unwrap();
//     assert!(
//         Database::load_uncommitted_events(&db.connection.lock().unwrap())
//             .unwrap()
//             .is_empty()
//     );
// }

// #[test]
// fn stats_getting() {
//     let tmpdir = tempdir().unwrap();
//     let mut db = Database::new(tmpdir.path()).unwrap();
//     let profile = Profile::new("Alice", "");

//     for i in 0..1000 {
//         let mut event: Event = Faker.fake();
//         event.server_ts += i;
//         db.add_event(event, profile.clone());
//     }

//     db.commit().unwrap();

//     let connection = db.get_connection().unwrap();

//     let stats = connection.get_stats().unwrap();

//     assert_eq!(stats.event_count, 1000);
//     assert_eq!(stats.room_count, 1);
//     assert!(stats.size > 0);
// }

// #[test]
// fn database_upgrade_v1() {
//     let mut path = PathBuf::from(file!());
//     path.pop();
//     path.pop();
//     path.pop();
//     path.push("data/database/v1");
//     let db = Database::new(path);

//     // Sadly the v1 database has invalid json in the source field, reindexing it
//     // won't be possible. Let's check that it's marked for a reindex.
//     match db {
//         Ok(_) => panic!("Database doesn't need a reindex."),
//         Err(e) => match e {
//             Error::ReindexError => (),
//             e => panic!("Database doesn't need a reindex: {}", e),
//         },
//     }
// }

// #[cfg(test)]
// use crate::database::recovery::test::reindex_loop;

// #[test]
// fn database_upgrade_v1_2() {
//     let mut path = PathBuf::from(file!());
//     path.pop();
//     path.pop();
//     path.pop();
//     path.push("data/database/v1_2");
//     let db = Database::new(&path);
//     match db {
//         Ok(_) => panic!("Database doesn't need a reindex."),
//         Err(e) => match e {
//             Error::ReindexError => (),
//             e => panic!("Database doesn't need a reindex: {}", e),
//         },
//     }

//     let mut recovery_db = RecoveryDatabase::new(&path).expect("Can't open recovery db");

//     recovery_db.delete_the_index().unwrap();
//     recovery_db.open_index().unwrap();

//     let events = recovery_db.load_events_deserialized(100, None).unwrap();

//     recovery_db.index_events(&events).unwrap();
//     reindex_loop(&mut recovery_db, events).unwrap();
//     recovery_db.commit_and_close().unwrap();

//     let db = Database::new(&path).expect("Can't open the db event after a reindex");

//     let mut connection = db.get_connection().unwrap();
//     let (version, _) = Database::get_version(&mut connection).unwrap();
//     assert_eq!(version, DATABASE_VERSION);

//     let result = db.search("Hello", &SearchConfig::new()).unwrap().results;
//     assert!(!result.is_empty())
// }

// #[test]
// fn delete_an_event() {
//     let tmpdir = tempdir().unwrap();
//     let mut db = Database::new(tmpdir.path()).unwrap();
//     let profile = Profile::new("Alice", "");

//     db.add_event(EVENT.clone(), profile.clone());
//     db.add_event(TOPIC_EVENT.clone(), profile);

//     db.force_commit().unwrap();

//     assert!(
//         Database::load_pending_deletion_events(&db.connection.lock().unwrap())
//             .unwrap()
//             .is_empty()
//     );

//     let recv = db.delete_event(&EVENT.event_id);
//     recv.recv().unwrap().unwrap();

//     assert_eq!(
//         Database::load_pending_deletion_events(&db.connection.lock().unwrap())
//             .unwrap()
//             .len(),
//         1
//     );

//     drop(db);

//     let mut db = Database::new(tmpdir.path()).unwrap();
//     assert_eq!(
//         Database::load_pending_deletion_events(&db.connection.lock().unwrap())
//             .unwrap()
//             .len(),
//         1
//     );

//     db.force_commit().unwrap();
//     assert_eq!(
//         Database::load_pending_deletion_events(&db.connection.lock().unwrap())
//             .unwrap()
//             .len(),
//         0
//     );
// }

// #[test]
// fn add_events_with_null_byte() {
//     let event_source: &str = r#"{
//         "content": {
//             "body": "\u00000",
//             "msgtype": "m.text"
//         },
//         "event_id": "$15163622448EBvZJ:localhost",
//         "origin_server_ts": 1516362244050,
//         "sender": "@example2:localhost",
//         "type": "m.room.message",
//         "unsigned": {"age": 43289803098},
//         "user_id": "@example2:localhost",
//         "age": 43289803098,
//         "room_id": "!test:example.org"
//     }"#;

//     let event = RecoveryDatabase::event_from_json(event_source).unwrap();

//     let tmpdir = tempdir().unwrap();
//     let db = Database::new(tmpdir.path()).unwrap();
//     let profile = Profile::new("Alice", &event.content_value);

//     let events = vec![(event, profile)];
//     db.add_historic_events(events, None, None)
//         .recv()
//         .unwrap()
//         .expect("Event should be added");
// }

// #[test]
// fn is_room_indexed() {
//     let tmpdir = tempdir().unwrap();
//     let mut db = Database::new(tmpdir.path()).unwrap();

//     let connection = db.get_connection().unwrap();

//     assert!(connection.is_empty().unwrap());
//     assert!(!connection.is_room_indexed("!test_room:localhost").unwrap());

//     let profile = Profile::new("Alice", "");
//     db.add_event(EVENT.clone(), profile);
//     db.force_commit().unwrap();

//     assert!(connection.is_room_indexed("!test_room:localhost").unwrap());
//     assert!(!connection.is_room_indexed("!test_room2:localhost").unwrap());
// }

// #[test]
// fn user_version() {
//     let tmpdir = tempdir().unwrap();
//     let db = Database::new(tmpdir.path()).unwrap();
//     let connection = db.get_connection().unwrap();

//     assert_eq!(connection.get_user_version().unwrap(), 0);
//     connection.set_user_version(10).unwrap();
//     assert_eq!(connection.get_user_version().unwrap(), 10);
// }

// #[test]
// #[cfg(feature = "encryption")]
// fn sqlcipher_cipher_settings_update() {
//     let mut path = PathBuf::from(file!());
//     path.pop();
//     path.pop();
//     path.pop();
//     path.push("data/database/sqlcipher-v3");

//     let config = Config::new().set_passphrase("qR17RdpWurSh2pQRSc/EnsaO9V041kOwsZk0iSdUY/g");
//     let _db =
//         Database::new_with_config(&path, &config).expect("We should be able to open the database");
// }
