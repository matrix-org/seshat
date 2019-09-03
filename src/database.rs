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
use rusqlite::{Connection, ToSql, NO_PARAMS};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;
use tantivy;

#[cfg(test)]
use tempfile::tempdir;

#[cfg(test)]
use fake::{Faker, Dummy, Fake};

#[cfg(test)]
use fake::faker::name::raw::*;
#[cfg(test)]
use fake::faker::internet::raw::*;
#[cfg(test)]
use fake::locales::*;


use crate::index::{Index, Writer, IndexSearcher};

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

#[cfg(test)]
impl<T> Dummy<T> for Event {
    fn dummy_with_rng<R: ?Sized>(config: &T, rng: &mut R) -> Self {
        let domain: String = FreeEmailProvider(EN).fake();
        Event::new(
            "Hello world",
            &format!("${}:{}", (0..10).fake::<u8>(), &domain),
            &format!("@{}:{}", Username(EN).fake::<String>(), FreeEmailProvider(EN).fake::<String>()),
            151636_2244026,
            "!test_room:localhost",
            EVENT_SOURCE
        )
    }
}

enum ThreadMessage {
    Event((Event, Profile)),
    Write,
}

#[derive(Debug)]
pub enum Error {
    PoolError(r2d2::Error),
    DatabaseError(rusqlite::Error),
    IndexError(tantivy::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

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
    pub fn search(&self, term: &str) -> Vec<(f32, String, i64)> {
        let search_result = self.inner.search(term);

        if search_result.is_empty() {
            return vec![]
        }

        // TODO load the profile information as well.
        match Database::load_events(&self.database, &search_result) {
            Ok(result) => result,
            Err(_e) => vec![],
        }
    }
}

unsafe impl Send for Searcher {}

/// The Seshat database.
pub struct Database {
    connection: Arc<PooledConnection<SqliteConnectionManager>>,
    _pool: r2d2::Pool<SqliteConnectionManager>,
    _write_thread: JoinHandle<()>,
    tx: Sender<ThreadMessage>,
    condvar: Arc<(Mutex<AtomicUsize>, Condvar)>,
    last_opstamp: usize,
    index: Index,
}

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
            _pool: pool,
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

    fn spawn_writer(
        connection: PooledConnection<SqliteConnectionManager>,
        mut index_writer: Writer,
    ) -> (
        JoinHandle<()>,
        Sender<ThreadMessage>,
        Arc<(Mutex<AtomicUsize>, Condvar)>,
    ) {
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
                                // Write the events
                                // TODO all of this should be a single sqlite transaction
                                for (e, p) in &events {
                                    if Database::event_in_store(&connection, &e) {
                                        continue
                                    }
                                    Database::save_event(&connection, e, p).unwrap();
                                    index_writer.add_event(&e.body, &e.event_id);
                                }

                                // Clear the event queue.
                                events.clear();
                                // TODO remove the unwrap.
                                index_writer.commit().unwrap();

                                // Notify that we are done with the write.
                                opstamp.fetch_add(1, Ordering::SeqCst);
                                cvar.notify_all();
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

    fn create_tables(conn: &Connection) -> Result<()> {
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
                source TEXT NOT NULL,
                profile_id INTEGER NOT NULL,
                FOREIGN KEY (profile_id) REFERENCES profile (id),
                UNIQUE(event_id, room_id)
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
        connection: &PooledConnection<SqliteConnectionManager>,
        user_id: &str,
        profile: &Profile,
    ) -> Result<i64> {
        let display_name = profile.display_name.as_ref();
        let avatar_url = profile.avatar_url.as_ref();

        // unwrap_or_default doesn't work on references sadly.
        let display_name = if let Some(d) = display_name {
            d
        } else {
            ""
        };

        let avatar_url = if let Some(a) = avatar_url {
            a
        } else {
            ""
        };

        connection.execute(
            "
            INSERT OR IGNORE INTO profiles (
                user_id, display_name, avatar_url
            ) VALUES(?1, ?2, ?3)",
            &[
                user_id,
                display_name,
                avatar_url,
            ],
        )?;

        let profile_id: i64 = connection.query_row(
            "
            SELECT id FROM profiles WHERE (
                user_id=?1
                and display_name=?2
                and avatar_url=?3)",
            &[
                user_id,
                display_name,
                avatar_url
            ],
            |row| row.get(0),
        )?;

        Ok(profile_id)
    }

    pub(crate) fn load_profile(
        connection: &PooledConnection<SqliteConnectionManager>,
        profile_id: i64,
    ) -> Result<Profile> {
        let profile = connection.query_row(
            "SELECT display_name, avatar_url FROM profiles WHERE id=?1",
            &[profile_id],
            |row| Ok(Profile {display_name: row.get(0)?, avatar_url: row.get(1)?}),
        )?;

        Ok(profile)
    }

    pub(crate) fn save_event_helper(
        connection: &PooledConnection<SqliteConnectionManager>,
        event: &Event,
        profile_id: i64,
    ) -> Result<()> {
        connection.execute(
            "
            INSERT OR IGNORE INTO events (
                event_id, sender, server_ts, room_id, source, profile_id
            ) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            &[
                &event.event_id,
                &event.sender,
                &event.server_ts.to_string(),
                &event.room_id,
                &event.source,
                &profile_id.to_string(),
            ],
        )?;

        Ok(())
    }

    pub(crate) fn save_event(
        connection: &PooledConnection<SqliteConnectionManager>,
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

    pub(crate) fn event_in_store(
        connection: &PooledConnection<SqliteConnectionManager>,
        event: &Event
    ) -> bool {
        let ret: std::result::Result<i64, rusqlite::Error> = connection.query_row(
            "
            SELECT id FROM events WHERE (
                event_id=?1
                and room_id=?2)",
            &[
                &event.event_id,
                &event.room_id,
            ],
            |row| row.get(0),
        );

        match ret {
            Ok(_event_id) => true,
            Err(_e) => false
        }
    }

    /// Load events surounding the given event.
    pub fn load_event_context(
        connection: &PooledConnection<SqliteConnectionManager>,
        event: &Event,
        before_limit: usize,
        after_limit: usize
    ) -> (Vec<String>, Vec<String>) {
        let before = if before_limit == 0 {
            vec![]
        } else {
            let mut stmt = connection.prepare(
                "SELECT source
                 FROM events WHERE (
                     (event_id != ?1) &
                     (room_id == ?2) &
                     (server_ts <= ?3)
                 ) ORDER BY server_ts DESC LIMIT ?4
                 ",
            ).unwrap();
            let context = stmt.query_map(
                &vec![
                    &event.event_id as &dyn ToSql,
                    &event.room_id,
                    &event.server_ts,
                    &(before_limit as i64)
                ],
                |row| {row.get(0)}
            ).unwrap();
            let mut ret: Vec<String> = Vec::new();

            for row in context {
                let event_source: String = row.unwrap();
                ret.push(event_source)
            }

            ret
        };

        let after = if after_limit == 0 {
            vec![]
        } else {
            let mut stmt = connection.prepare(
                "SELECT source
                 FROM events WHERE (
                     (event_id != ?1) &
                     (room_id == ?2) &
                     (server_ts >= ?3)
                 ) ORDER BY server_ts ASC LIMIT ?4
                 ",
            ).unwrap();
            let context = stmt.query_map(
                &vec![
                    &event.event_id as &dyn ToSql,
                    &event.room_id,
                    &event.server_ts,
                    &(after_limit as i64)
                ],
                |row| {row.get(0)}
            ).unwrap();
            let mut ret: Vec<String> = Vec::new();

            for row in context {
                let event_source: String = row.unwrap();
                ret.push(event_source)
            }

            ret
        };

        (before, after)
    }

    pub(crate) fn load_events(
        connection: &PooledConnection<SqliteConnectionManager>,
        search_result: &[(f32, String)],
    ) -> rusqlite::Result<Vec<(f32, String, i64)>> {
        if search_result.is_empty() {
            return Ok(vec![]);
        }

        let event_num = search_result.len();
        let parameter_str = std::iter::repeat(", ?")
            .take(event_num - 1)
            .collect::<String>();

        let mut stmt = connection.prepare(&format!(
            "SELECT source, profile_id
             FROM events WHERE event_id IN (?{})
             ",
            &parameter_str
        ))?;
        let (scores, event_ids): (Vec<f32>, Vec<String>) = search_result.iter().cloned().unzip();
        let db_events = stmt.query_map(event_ids, |row| Ok((row.get(0)?, row.get(1)?)))?;

        let mut events = Vec::new();
        let i = 0;

        for row in db_events {
            let (e, p_id): (String, i64) = row?;
            events.push((scores[i], e, p_id));
        }

        Ok(events)
    }

    /// Search the index and return events matching a search term.
    /// This is just a helper function that gets a searcher and performs a
    /// search on it immediately.
    /// # Arguments
    ///
    /// * `term` - The search term that should be used to search the index.
    pub fn search(&self, term: &str) -> Vec<(f32, String, i64)> {
        let searcher = self.get_searcher();
        searcher.search(term)
    }

    /// Get a searcher that can be used to perform a search.
    pub fn get_searcher(&self) -> Searcher {
        let index_searcher = self.index.get_searcher();
        Searcher { inner: index_searcher, database: self.connection.clone() }
    }
}

#[cfg(test)]
static EVENT_SOURCE: &str = "{
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
    static ref EVENT: Event = Event::new(
        "Test message",
        "$15163622445EBvZJ:localhost",
        "@example2:localhost",
        151636_2244026,
        "!test_room:localhost",
        EVENT_SOURCE
    );
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
    let events = Database::load_events(&db.connection, &[
            (1.0, "$15163622445EBvZJ:localhost".to_string()),
            (0.3, "$FAKE".to_string()),
        ])
        .unwrap();

    assert_eq!(*EVENT.source, events[0].1)
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

    let events = Database::load_events(&db.connection, &[
            (1.0, "$15163622445EBvZJ:localhost".to_string()),
            (0.3, "$FAKE".to_string()),
        ])
        .unwrap();

    assert_eq!(*EVENT.source, events[0].1)
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

    let result = db.search("Test");
    assert!(!result.is_empty());
    assert_eq!(result[0].1, EVENT.source);
}

#[test]
fn duplicate_empty_profiles() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(&tmpdir).unwrap();
    let profile = Profile {display_name: None, avatar_url: None};
    let user_id = "@alice.example.org";

    let first_id = Database::save_profile(&db.connection, user_id, &profile).unwrap();
    let second_id = Database::save_profile(&db.connection, user_id, &profile).unwrap();

    assert_eq!(first_id, second_id);

    let mut stmt = db.connection.prepare("SELECT id FROM profiles WHERE user_id=?1").unwrap();

    let profile_ids = stmt.query_map(&[user_id], |row| row.get(0)).unwrap();

    let mut id_count = 0;

    for row in profile_ids {
        let (profile_id): i64 = row.unwrap();
        id_count += 1;
    }

    assert_eq!(id_count, 1);
}

#[test]
fn load_a_profile() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(&tmpdir).unwrap();

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
    let result = searcher.search("Test");
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
    };

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

    let (before, after) = Database::load_event_context(
        &db.connection,
        &EVENT,
        1,
        1
    );

    assert_eq!(before.len(), 1);
    assert_eq!(before[0], before_event.as_ref().unwrap().source);
    assert_eq!(after.len(), 1);
    assert_eq!(after[0], after_event.as_ref().unwrap().source);
}
