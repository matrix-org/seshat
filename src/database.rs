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

use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{ToSql, NO_PARAMS};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;

#[cfg(test)]
use fake::{Fake, Faker};
#[cfg(test)]
use tempfile::tempdir;

use crate::index::{Index, IndexSearcher, Writer};
use crate::types::{
    BacklogCheckpoint, Event, EventId, Profile, Result, RoomId, SearchResult, ThreadMessage,
};

#[cfg(test)]
use crate::types::EVENT;

/// The main entry point to the index and database.
pub struct Searcher {
    inner: IndexSearcher,
    database: Arc<PooledConnection<SqliteConnectionManager>>,
}

impl Searcher {
    /// Search the index and return events matching a search term.
    /// # Arguments
    ///
    /// * `term` - The search term that should be used to search the index.
    pub fn search(
        &self,
        term: &str,
        limit: usize,
        before_limit: usize,
        after_limit: usize,
        order_by_recent: bool,
        room_id: Option<&RoomId>,
    ) -> Vec<SearchResult> {
        let search_result = self.inner.search(term, limit, order_by_recent, room_id);

        if search_result.is_empty() {
            return vec![];
        }

        match Database::load_events(&self.database, &search_result, before_limit, after_limit) {
            Ok(result) => result,
            Err(_e) => vec![],
        }
    }
}

unsafe impl Send for Searcher {}

/// The Seshat database.
pub struct Database {
    connection: Arc<PooledConnection<SqliteConnectionManager>>,
    pool: r2d2::Pool<SqliteConnectionManager>,
    _write_thread: JoinHandle<()>,
    tx: Sender<ThreadMessage>,
    condvar: Arc<(Mutex<AtomicUsize>, Condvar)>,
    last_opstamp: usize,
    index: Index,
}

/// A Seshat database connection.
/// The connection can be used to read data out of the database using a
/// separate thread.
pub struct Connection(PooledConnection<SqliteConnectionManager>);

impl Connection {
    /// Load all the previously stored backlog checkpoints from the database.
    /// # Arguments
    pub fn load_checkpoints(&self) -> Result<Vec<BacklogCheckpoint>> {
        let mut stmt = self.prepare("SELECT room_id, token FROM backlogcheckpoints")?;

        let rows = stmt.query_map(NO_PARAMS, |row| {
            Ok(BacklogCheckpoint {
                room_id: row.get(0)?,
                token: row.get(1)?,
            })
        })?;

        let mut checkpoints = Vec::new();

        for row in rows {
            let checkpoint: BacklogCheckpoint = row?;
            checkpoints.push(checkpoint);
        }
        Ok(checkpoints)
    }
}

impl Deref for Connection {
    type Target = PooledConnection<SqliteConnectionManager>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Connection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

type WriterRet = (
    JoinHandle<()>,
    Sender<ThreadMessage>,
    Arc<(Mutex<AtomicUsize>, Condvar)>,
);

impl Database {
    /// Create a new Seshat database or open an existing one.
    /// # Arguments
    ///
    /// * `path` - The directory where the database will be stored in. This
    /// should be an empty directory if a new database will be created.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Database> {
        let db_path = path.as_ref().join("events.db");
        let manager = SqliteConnectionManager::file(&db_path);
        let pool = r2d2::Pool::new(manager)?;

        let connection = Arc::new(pool.get()?);

        Database::create_tables(&connection)?;

        let index = Database::create_index(&path)?;
        let writer = index.get_writer()?;
        let (t_handle, tx, condvar) = Database::spawn_writer(pool.get()?, writer);

        Ok(Database {
            connection,
            pool,
            _write_thread: t_handle,
            tx,
            condvar,
            last_opstamp: 0,
            index,
        })
    }

    fn create_index<P: AsRef<Path>>(path: &P) -> Result<Index> {
        Ok(Index::new(path)?)
    }

    fn write_events_helper(
        connection: &rusqlite::Connection,
        index_writer: &mut Writer,
        events: &mut Vec<(Event, Profile)>,
    ) -> Result<()> {
        for (e, p) in events.drain(..) {
            if Database::event_in_store(&connection, &e) {
                continue;
            }
            Database::save_event(&connection, &e, &p)?;
            index_writer.add_event(&e.body, &e.event_id, &e.room_id, e.server_ts as u64);
        }

        Ok(())
    }

    fn write_queued_events(
        connection: &rusqlite::Connection,
        index_writer: &mut Writer,
        events: &mut Vec<(Event, Profile)>,
    ) -> Result<()> {
        Database::write_events_helper(connection, index_writer, events)?;
        index_writer.commit()?;

        Ok(())
    }

    fn write_backlog_events(
        connection: &mut rusqlite::Connection,
        index_writer: &mut Writer,
        message: (
            Option<BacklogCheckpoint>,
            Option<BacklogCheckpoint>,
            Vec<(Event, Profile)>,
        ),
    ) -> Result<()> {
        let (new_checkpoint, old_checkpoint, mut events) = message;
        let transaction = connection.transaction()?;

        Database::write_events_helper(&transaction, index_writer, &mut events)?;
        Database::replace_backlog_checkpoint(
            &transaction,
            new_checkpoint.as_ref(),
            old_checkpoint.as_ref(),
        )?;

        transaction.commit()?;
        index_writer.commit()?;

        Ok(())
    }

    fn spawn_writer(
        mut connection: PooledConnection<SqliteConnectionManager>,
        mut index_writer: Writer,
    ) -> WriterRet {
        let (tx, rx): (_, Receiver<ThreadMessage>) = channel();

        let pair = Arc::new((Mutex::new(AtomicUsize::new(0)), Condvar::new()));
        let pair2 = pair.clone();

        let t_handle = thread::spawn(move || loop {
            let mut events: Vec<(Event, Profile)> = Vec::new();
            let &(ref lock, ref cvar) = &*pair;

            loop {
                let message = rx.recv();
                let opstamp = lock.lock().unwrap();

                match message {
                    Ok(m) => {
                        match m {
                            ThreadMessage::Event(e) => events.push(e),
                            ThreadMessage::Write => {
                                Database::write_queued_events(
                                    &connection,
                                    &mut index_writer,
                                    &mut events,
                                )
                                .unwrap();
                                // Notify that we are done with the write.
                                opstamp.fetch_add(1, Ordering::SeqCst);
                                cvar.notify_all();
                            }
                            ThreadMessage::BacklogEvents(m) => {
                                let (check, old_check, events, sender) = m;
                                let ret = Database::write_backlog_events(
                                    &mut connection,
                                    &mut index_writer,
                                    (check, old_check, events),
                                );
                                sender.send(ret).unwrap_or(());
                            }
                        };
                    }
                    Err(_e) => return,
                };
            }
        });

        (t_handle, tx, pair2)
    }

    /// Add an event with the given profile to the database.
    /// # Arguments
    ///
    /// * `event` - The directory where the database will be stored in. This
    /// * `profile` - The directory where the database will be stored in. This
    ///
    /// This is a fast non-blocking operation, it only queues up the event to be
    /// added to the database. The events will be commited to the database
    /// only when the user calls the `commit()` method.
    pub fn add_event(&self, event: Event, profile: Profile) {
        let message = ThreadMessage::Event((event, profile));
        self.tx.send(message).unwrap();
    }

    /// Commit the currently queued up events. This method will block. A
    /// non-blocking version of this method exists in the `commit_no_wait()`
    /// method. Getting a Condvar that will signal that the commit was done is
    /// possible using the `commit_get_cvar()` method.
    pub fn commit(&mut self) -> usize {
        let (last_opstamp, cvar) = self.commit_get_cvar();
        self.last_opstamp = Database::wait_for_commit(last_opstamp, &cvar);
        self.last_opstamp
    }

    /// Reload the database to reflect the latest commit.
    pub fn reload(&mut self) -> Result<()> {
        self.index.reload()?;
        Ok(())
    }

    /// Commit the currently queued up events without waiting for confirmation
    /// that the operation is done.
    pub fn commit_no_wait(&mut self) {
        self.tx.send(ThreadMessage::Write).unwrap();
    }

    /// Commit the currently queued up events and get a Condvar that will be
    /// notified when the commit operation is done.
    pub fn commit_get_cvar(&mut self) -> (usize, Arc<(Mutex<AtomicUsize>, Condvar)>) {
        self.tx.send(ThreadMessage::Write).unwrap();
        (self.last_opstamp, self.condvar.clone())
    }

    /// Wait for a Convdvar returned by `commit_get_cvar()` to trigger.
    pub fn wait_for_commit(
        last_opstamp: usize,
        condvar: &Arc<(Mutex<AtomicUsize>, Condvar)>,
    ) -> usize {
        let (ref lock, ref cvar) = **condvar;
        let mut opstamp = lock.lock().unwrap();

        while *opstamp.get_mut() == last_opstamp {
            opstamp = cvar.wait(opstamp).unwrap();
        }

        *opstamp.get_mut()
    }

    /// Add the given events from the backlog to the database.
    /// # Arguments
    ///
    /// * `events` - The events that will be added.
    /// * `new_checkpoint` - A checkpoint that states where we need to continue
    /// fetching events from the backlog. This checkpoint will be persisted in
    /// the database.
    /// * `old_checkpoint` - The checkpoint that was used to fetch the given
    /// events. This checkpoint will be removed from the database.
    pub fn add_backlog_events(
        &self,
        events: Vec<(Event, Profile)>,
        new_checkpoint: Option<BacklogCheckpoint>,
        old_checkpoint: Option<BacklogCheckpoint>,
    ) -> Receiver<Result<()>> {
        let (sender, receiver): (_, Receiver<Result<()>>) = channel();
        let payload = (new_checkpoint, old_checkpoint, events, sender);
        let message = ThreadMessage::BacklogEvents(payload);
        self.tx.send(message).unwrap();

        receiver
    }

    fn create_tables(conn: &rusqlite::Connection) -> Result<()> {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS profiles (
                id INTEGER NOT NULL PRIMARY KEY,
                user_id TEXT NOT NULL,
                display_name TEXT NOT NULL,
                avatar_url TEXT NOT NULL,
                UNIQUE(user_id,display_name,avatar_url)
            )",
            NO_PARAMS,
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS events (
                id INTEGER NOT NULL PRIMARY KEY,
                event_id TEXT NOT NULL,
                sender TEXT NOT NULL,
                server_ts DATETIME NOT NULL,
                room_id TEXT NOT NULL,
                content_value TEXT NOT NULL,
                type TEXT NOT NULL,
                source TEXT NOT NULL,
                profile_id INTEGER NOT NULL,
                FOREIGN KEY (profile_id) REFERENCES profile (id),
                UNIQUE(event_id, room_id)
            )",
            NO_PARAMS,
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS backlogcheckpoints (
                id INTEGER NOT NULL PRIMARY KEY,
                room_id TEXT NOT NULL,
                token TEXT NOT NULL,
                UNIQUE(room_id,token)
            )",
            NO_PARAMS,
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS event_profile_id ON events (profile_id)",
            NO_PARAMS,
        )?;

        Ok(())
    }

    pub(crate) fn save_profile(
        connection: &rusqlite::Connection,
        user_id: &str,
        profile: &Profile,
    ) -> Result<i64> {
        let display_name = profile.display_name.as_ref();
        let avatar_url = profile.avatar_url.as_ref();

        // unwrap_or_default doesn't work on references sadly.
        let display_name = if let Some(d) = display_name { d } else { "" };

        let avatar_url = if let Some(a) = avatar_url { a } else { "" };

        connection.execute(
            "
            INSERT OR IGNORE INTO profiles (
                user_id, display_name, avatar_url
            ) VALUES(?1, ?2, ?3)",
            &[user_id, display_name, avatar_url],
        )?;

        let profile_id: i64 = connection.query_row(
            "
            SELECT id FROM profiles WHERE (
                user_id=?1
                and display_name=?2
                and avatar_url=?3)",
            &[user_id, display_name, avatar_url],
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
            "SELECT display_name, avatar_url FROM profiles WHERE id=?1",
            &[profile_id],
            |row| {
                Ok(Profile {
                    display_name: row.get(0)?,
                    avatar_url: row.get(1)?,
                })
            },
        )?;

        Ok(profile)
    }

    pub(crate) fn save_event_helper(
        connection: &rusqlite::Connection,
        event: &Event,
        profile_id: i64,
    ) -> Result<()> {
        connection.execute(
            "
            INSERT OR IGNORE INTO events (
                event_id, sender, server_ts, room_id, content_value, type,
                source, profile_id
            ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            &[
                &event.event_id,
                &event.sender,
                &event.server_ts.to_string(),
                &event.room_id,
                &event.body,
                "m.room.message",
                &event.source,
                &profile_id.to_string(),
            ],
        )?;

        Ok(())
    }

    pub(crate) fn save_event(
        connection: &rusqlite::Connection,
        event: &Event,
        profile: &Profile,
    ) -> Result<()> {
        if Database::event_in_store(connection, event) {
            return Ok(());
        }

        let profile_id = Database::save_profile(connection, &event.sender, profile)?;
        Database::save_event_helper(connection, event, profile_id)?;

        Ok(())
    }

    pub(crate) fn event_in_store(connection: &rusqlite::Connection, event: &Event) -> bool {
        let ret: std::result::Result<i64, rusqlite::Error> = connection.query_row(
            "
            SELECT id FROM events WHERE (
                event_id=?1
                and room_id=?2)",
            &[&event.event_id, &event.room_id],
            |row| row.get(0),
        );

        match ret {
            Ok(_event_id) => true,
            Err(_e) => false,
        }
    }

    /// Load events surounding the given event.
    pub fn load_event_context(
        connection: &rusqlite::Connection,
        event: &Event,
        before_limit: usize,
        after_limit: usize,
    ) -> (Vec<String>, Vec<String>, HashMap<String, Profile>) {
        let mut profiles: HashMap<String, Profile> = HashMap::new();

        let before = if before_limit == 0 {
            vec![]
        } else {
            let mut stmt = connection
                .prepare(
                    "SELECT source, sender, display_name, avatar_url
                 FROM events
                 INNER JOIN profiles on profiles.id = events.profile_id
                 WHERE (
                     (event_id != ?1) &
                     (room_id == ?2) &
                     (server_ts <= ?3)
                 ) ORDER BY server_ts DESC LIMIT ?4
                 ",
                )
                .unwrap();
            let context = stmt
                .query_map(
                    &vec![
                        &event.event_id as &dyn ToSql,
                        &event.room_id,
                        &event.server_ts,
                        &(after_limit as i64),
                    ],
                    |row| {
                        Ok((
                            row.get(0),
                            row.get(1),
                            Profile {
                                display_name: row.get(2)?,
                                avatar_url: row.get(3)?,
                            },
                        ))
                    },
                )
                .unwrap();
            let mut ret: Vec<String> = Vec::new();

            for row in context {
                let (source, sender, profile) = row.unwrap();
                profiles.insert(sender.unwrap(), profile);
                ret.push(source.unwrap())
            }

            ret
        };

        let after = if after_limit == 0 {
            vec![]
        } else {
            let mut stmt = connection
                .prepare(
                    "SELECT source, sender, display_name, avatar_url
                 FROM events
                 INNER JOIN profiles on profiles.id = events.profile_id
                 WHERE (
                     (event_id != ?1) &
                     (room_id == ?2) &
                     (server_ts >= ?3)
                 ) ORDER BY server_ts ASC LIMIT ?4
                 ",
                )
                .unwrap();
            let context = stmt
                .query_map(
                    &vec![
                        &event.event_id as &dyn ToSql,
                        &event.room_id,
                        &event.server_ts,
                        &(after_limit as i64),
                    ],
                    |row| {
                        Ok((
                            row.get(0),
                            row.get(1),
                            Profile {
                                display_name: row.get(2)?,
                                avatar_url: row.get(3)?,
                            },
                        ))
                    },
                )
                .unwrap();

            let mut ret: Vec<String> = Vec::new();

            for row in context {
                let (source, sender, profile) = row.unwrap();
                profiles.insert(sender.unwrap(), profile);
                ret.push(source.unwrap())
            }

            ret
        };

        (before, after, profiles)
    }

    pub(crate) fn load_events(
        connection: &rusqlite::Connection,
        search_result: &[(f32, EventId)],
        before_limit: usize,
        after_limit: usize,
    ) -> rusqlite::Result<Vec<SearchResult>> {
        if search_result.is_empty() {
            return Ok(vec![]);
        }

        let event_num = search_result.len();
        let parameter_str = std::iter::repeat(", ?")
            .take(event_num - 1)
            .collect::<String>();

        let mut stmt = connection.prepare(&format!(
            "SELECT content_value, event_id, sender, server_ts, room_id, source, display_name, avatar_url
             FROM events
             INNER JOIN profiles on profiles.id = events.profile_id
             WHERE event_id IN (?{})
             ",
            &parameter_str
        ))?;

        let (scores, event_ids): (Vec<f32>, Vec<String>) = search_result.iter().cloned().unzip();
        let db_events = stmt.query_map(event_ids, |row| {
            Ok((
                Event {
                    body: row.get(0)?,
                    event_id: row.get(1)?,
                    sender: row.get(2)?,
                    server_ts: row.get(3)?,
                    room_id: row.get(4)?,
                    source: row.get(5)?,
                },
                Profile {
                    display_name: row.get(6)?,
                    avatar_url: row.get(7)?,
                },
            ))
        })?;

        let mut events = Vec::new();
        let i = 0;

        for row in db_events {
            let (event, profile): (Event, Profile) = row?;
            let (before, after, profiles) =
                Database::load_event_context(connection, &event, before_limit, after_limit);

            let mut profiles = profiles;
            profiles.insert(event.sender.clone(), profile);

            let result = SearchResult {
                score: scores[i],
                event_source: event.source,
                events_before: before,
                events_after: after,
                profile_info: profiles,
            };
            events.push(result);
        }

        Ok(events)
    }

    /// Search the index and return events matching a search term.
    /// This is just a helper function that gets a searcher and performs a
    /// search on it immediately.
    /// # Arguments
    ///
    /// * `term` - The search term that should be used to search the index.
    pub fn search(
        &self,
        term: &str,
        limit: usize,
        before_limit: usize,
        after_limit: usize,
        order_by_recent: bool,
        room_id: Option<&RoomId>,
    ) -> Vec<SearchResult> {
        let searcher = self.get_searcher();
        searcher.search(
            term,
            limit,
            before_limit,
            after_limit,
            order_by_recent,
            room_id,
        )
    }

    /// Get a searcher that can be used to perform a search.
    pub fn get_searcher(&self) -> Searcher {
        let index_searcher = self.index.get_searcher();
        Searcher {
            inner: index_searcher,
            database: self.connection.clone(),
        }
    }

    pub(crate) fn replace_backlog_checkpoint(
        connection: &rusqlite::Connection,
        new: Option<&BacklogCheckpoint>,
        old: Option<&BacklogCheckpoint>,
    ) -> Result<()> {
        if let Some(checkpoint) = new {
            connection.execute(
                "INSERT OR IGNORE INTO backlogcheckpoints (room_id, token)
                VALUES(?1, ?2)",
                &[&checkpoint.room_id, &checkpoint.token],
            )?;
        }

        if let Some(checkpoint) = old {
            connection.execute(
                "DELETE FROM backlogcheckpoints
                WHERE (room_id=?1 AND token=?2)",
                &[&checkpoint.room_id, &checkpoint.token],
            )?;
        }

        Ok(())
    }

    /// Get a database connection.
    /// Note that this connection should only be used for reading.
    pub fn get_connection(&mut self) -> Result<Connection> {
        let connection = self.pool.get()?;
        Ok(Connection(connection))
    }
}

#[test]
fn create_event_db() {
    let tmpdir = tempdir().unwrap();
    let _db = Database::new(tmpdir).unwrap();
}

#[test]
fn store_profile() {
    let tmpdir = tempdir().unwrap();
    let db = Database::new(&tmpdir).unwrap();

    let profile = Profile::new("Alice", "");

    let id = Database::save_profile(&db.connection, "@alice.example.org", &profile);
    assert_eq!(id.unwrap(), 1);

    let id = Database::save_profile(&db.connection, "@alice.example.org", &profile);
    assert_eq!(id.unwrap(), 1);

    let profile_new = Profile::new("Alice", "mxc://some_url");

    let id = Database::save_profile(&db.connection, "@alice.example.org", &profile_new);
    assert_eq!(id.unwrap(), 2);
}

#[test]
fn store_empty_profile() {
    let tmpdir = tempdir().unwrap();
    let db = Database::new(&tmpdir).unwrap();

    let profile = Profile {
        display_name: None,
        avatar_url: None,
    };
    let id = Database::save_profile(&db.connection, "@alice.example.org", &profile);
    assert_eq!(id.unwrap(), 1);
}

#[test]
fn store_event() {
    let tmpdir = tempdir().unwrap();
    let db = Database::new(&tmpdir).unwrap();
    let profile = Profile::new("Alice", "");
    let id = Database::save_profile(&db.connection, "@alice.example.org", &profile).unwrap();

    Database::save_event_helper(&db.connection, &EVENT, id).unwrap();
}

#[test]
fn store_event_and_profile() {
    let tmpdir = tempdir().unwrap();
    let db = Database::new(&tmpdir).unwrap();
    let profile = Profile::new("Alice", "");
    Database::save_event(&db.connection, &EVENT, &profile).unwrap();
}

#[test]
fn load_event() {
    let tmpdir = tempdir().unwrap();
    let db = Database::new(&tmpdir).unwrap();
    let profile = Profile::new("Alice", "");

    Database::save_event(&db.connection, &EVENT, &profile).unwrap();
    let events = Database::load_events(
        &db.connection,
        &[
            (1.0, "$15163622445EBvZJ:localhost".to_string()),
            (0.3, "$FAKE".to_string()),
        ],
        0,
        0,
    )
    .unwrap();

    assert_eq!(*EVENT.source, events[0].event_source)
}

#[test]
fn commit_a_write() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(&tmpdir).unwrap();
    let opstamp = db.commit();
    assert_eq!(opstamp, 1);
    let opstamp = db.commit();
    assert_eq!(opstamp, 2);
}

#[test]
fn save_the_event_multithreaded() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(&tmpdir).unwrap();
    let profile = Profile::new("Alice", "");

    db.add_event(EVENT.clone(), profile);
    db.commit();
    db.reload().unwrap();

    let events = Database::load_events(
        &db.connection,
        &[
            (1.0, "$15163622445EBvZJ:localhost".to_string()),
            (0.3, "$FAKE".to_string()),
        ],
        0,
        0,
    )
    .unwrap();

    assert_eq!(*EVENT.source, events[0].event_source)
}

#[test]
fn save_and_search() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(&tmpdir).unwrap();
    let profile = Profile::new("Alice", "");

    db.add_event(EVENT.clone(), profile);
    let opstamp = db.commit();
    db.reload().unwrap();

    assert_eq!(opstamp, 1);

    let result = db.search("Test", 10, 0, 0, false, None);
    assert!(!result.is_empty());
    assert_eq!(result[0].event_source, EVENT.source);
}

#[test]
fn duplicate_empty_profiles() {
    let tmpdir = tempdir().unwrap();
    let db = Database::new(&tmpdir).unwrap();
    let profile = Profile {
        display_name: None,
        avatar_url: None,
    };
    let user_id = "@alice.example.org";

    let first_id = Database::save_profile(&db.connection, user_id, &profile).unwrap();
    let second_id = Database::save_profile(&db.connection, user_id, &profile).unwrap();

    assert_eq!(first_id, second_id);

    let mut stmt = db
        .connection
        .prepare("SELECT id FROM profiles WHERE user_id=?1")
        .unwrap();

    let profile_ids = stmt.query_map(&[user_id], |row| row.get(0)).unwrap();

    let mut id_count = 0;

    for row in profile_ids {
        let _profile_id: i64 = row.unwrap();
        id_count += 1;
    }

    assert_eq!(id_count, 1);
}

#[test]
fn load_a_profile() {
    let tmpdir = tempdir().unwrap();
    let db = Database::new(&tmpdir).unwrap();

    let profile = Profile::new("Alice", "");
    let user_id = "@alice.example.org";
    let profile_id = Database::save_profile(&db.connection, user_id, &profile).unwrap();

    let loaded_profile = Database::load_profile(&db.connection, profile_id).unwrap();

    assert_eq!(profile, loaded_profile);
}

#[test]
fn duplicate_events() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(&tmpdir).unwrap();
    let profile = Profile::new("Alice", "");

    db.add_event(EVENT.clone(), profile.clone());
    db.add_event(EVENT.clone(), profile.clone());

    db.commit();
    db.reload().unwrap();

    let searcher = db.index.get_searcher();
    let result = searcher.search("Test", 10, false, None);
    assert_eq!(result.len(), 1);
}

#[test]
fn load_event_context() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(&tmpdir).unwrap();
    let profile = Profile::new("Alice", "");

    db.add_event(EVENT.clone(), profile.clone());

    let mut before_event = None;

    for i in 1..6 {
        let mut event: Event = Faker.fake();
        event.server_ts = EVENT.server_ts - i;
        event.source = format!("Hello before event {}", i);

        if before_event.is_none() {
            before_event = Some(event.clone());
        }

        db.add_event(event, profile.clone());
    }

    let mut after_event = None;

    for i in 1..6 {
        let mut event: Event = Faker.fake();
        event.server_ts = EVENT.server_ts + i;
        event.source = format!("Hello after event {}", i);

        if after_event.is_none() {
            after_event = Some(event.clone());
        }

        db.add_event(event, profile.clone());
    }

    db.commit();
    db.reload().unwrap();
    db.reload().unwrap();

    let (before, after, _) = Database::load_event_context(&db.connection, &EVENT, 1, 1);

    assert_eq!(before.len(), 1);
    assert_eq!(before[0], before_event.as_ref().unwrap().source);
    assert_eq!(after.len(), 1);
    assert_eq!(after[0], after_event.as_ref().unwrap().source);
}

#[test]
fn save_and_load_checkpoints() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(&tmpdir).unwrap();

    let checkpoint = BacklogCheckpoint {
        room_id: "!test:room".to_string(),
        token: "1234".to_string(),
    };

    let mut connection = db.get_connection().unwrap();
    let transaction = connection.transaction().unwrap();

    Database::replace_backlog_checkpoint(&transaction, Some(&checkpoint), None).unwrap();
    transaction.commit().unwrap();

    let checkpoints = connection.load_checkpoints().unwrap();

    assert!(checkpoints.contains(&checkpoint));

    let new_checkpoint = BacklogCheckpoint {
        room_id: "!test:room".to_string(),
        token: "12345".to_string(),
    };

    Database::replace_backlog_checkpoint(&connection, Some(&new_checkpoint), Some(&checkpoint))
        .unwrap();

    let checkpoints = connection.load_checkpoints().unwrap();

    assert!(!checkpoints.contains(&checkpoint));
    assert!(checkpoints.contains(&new_checkpoint));
}

#[test]
fn save_and_search_backlog_events() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(&tmpdir).unwrap();
    let profile = Profile::new("Alice", "");

    let mut events = Vec::new();

    for i in 1..6 {
        let mut event: Event = Faker.fake();
        event.server_ts = EVENT.server_ts - i;
        event.source = format!("Hello before event {}", i);
        events.push((event, profile.clone()));
    }

    let checkpoint = BacklogCheckpoint {
        room_id: "!test:room".to_string(),
        token: "1234".to_string(),
    };

    let receiver = db.add_backlog_events(events, Some(checkpoint.clone()), None);
    let ret = receiver.recv().unwrap();
    assert!(ret.is_ok());
    let connection = db.get_connection().unwrap();

    let checkpoints = connection.load_checkpoints().unwrap();
    assert!(checkpoints.contains(&checkpoint));
}
