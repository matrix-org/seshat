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
use rusqlite::{Connection, NO_PARAMS};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;
use tantivy;

#[cfg(test)]
use tempfile::tempdir;

use crate::index::{Index, Writer};

#[derive(Debug, PartialEq, Default, Clone)]
pub struct Event {
    pub body: String,
    pub event_id: String,
    pub sender: String,
    pub server_ts: i64,
    pub room_id: String,
    pub source: String,
}

enum ThreadMessage {
    Event((Event, Profile)),
    Write,
}

#[derive(Debug)]
pub enum Error {
    PoolError(r2d2::Error),
    DatabaseError(rusqlite::Error),
    IndexError(tantivy::Error)
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
    pub fn new(
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

pub struct Profile {
    pub(crate) display_name: String,
    pub(crate) avatar_url: String,
}

impl Profile {
    pub fn new(display_name: &str, avatar_url: &str) -> Profile {
        Profile {
            display_name: display_name.to_string(),
            avatar_url: avatar_url.to_string(),
        }
    }
}

pub struct Database {
    connection: PooledConnection<SqliteConnectionManager>,
    _pool: r2d2::Pool<SqliteConnectionManager>,
    _write_thread: JoinHandle<()>,
    tx: Sender<ThreadMessage>,
    condvar: Arc<(Mutex<AtomicUsize>, Condvar)>,
    last_opstamp: usize,
    index: Index,
}

impl Database {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Database> {
        let db_path = path.as_ref().join("events.db");
        let manager = SqliteConnectionManager::file(&db_path);
        let pool = r2d2::Pool::new(manager)?;

        let connection = pool.get()?;

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
            index
        })
    }

    fn create_index<P: AsRef<Path>>(path: &P) -> Result<Index> {
        Ok(Index::new(path)?)
    }

    fn spawn_writer(
        connection: PooledConnection<SqliteConnectionManager>,
        mut index_writer: Writer
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

            while let Ok(message) = rx.recv() {
                let opstamp = lock.lock().unwrap();

                match message {
                    ThreadMessage::Event(e) => events.push(e),
                    ThreadMessage::Write => {
                        // Write the events
                        // TODO all of this should be a single sqlite transaction
                        for (e, p) in &events {
                            // TODO check if the event was already stored
                            Database::save_event(&connection, e, p).unwrap();
                            index_writer.add_event(&e.body, &e.event_id);
                        }
                        // Clear the event queue.
                        events.clear();
                        // TODO remove the unwrap.
                        index_writer.commit().unwrap();

                        // Notify that we are done with the write.
                        opstamp.fetch_add(1, Ordering::SeqCst);
                        cvar.notify_one();
                    }
                };
            }
        });

        (t_handle, tx, pair2)
    }

    pub fn add_event(&self, event: Event, profile: Profile) {
        let message = ThreadMessage::Event((event, profile));
        self.tx.send(message).unwrap();
    }

    pub fn commit(&mut self) -> usize {
        self.tx.send(ThreadMessage::Write).unwrap();

        let &(ref lock, ref cvar) = &*self.condvar;
        let mut opstamp = lock.lock().unwrap();
        while *opstamp.get_mut() == self.last_opstamp {
            opstamp = cvar.wait(opstamp).unwrap();
        }

        self.last_opstamp = *opstamp.get_mut();
        self.last_opstamp
    }

    pub fn commit_no_wait(&mut self) {
        self.tx.send(ThreadMessage::Write).unwrap();
    }

    fn create_tables(conn: &Connection) -> Result<()> {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS profiles (
                id INTEGER NOT NULL PRIMARY KEY,
                user_id TEXT NOT NULL,
                display_name TEXT,
                avatar_url TEXT,
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
            "CREATE INDEX event_profile_id ON events (profile_id)",
            NO_PARAMS,
        )?;

        Ok(())
    }

    pub(crate) fn save_profile(
        connection: &PooledConnection<SqliteConnectionManager>,
        user_id: &str,
        profile: &Profile,
    ) -> Result<i64> {
        connection.execute(
            "
            INSERT OR IGNORE INTO profiles (
                user_id, display_name, avatar_url
            ) VALUES(?1, ?2, ?3)",
            &[user_id, &profile.display_name, &profile.avatar_url],
        )?;

        let profile_id = connection.query_row(
            "
            SELECT id FROM profiles WHERE (
                user_id=?1
                and display_name=?2
                and avatar_url=?3)",
            &[user_id, &profile.display_name, &profile.avatar_url],
            |row| row.get(0),
        )?;

        Ok(profile_id)
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
        let profile_id = Database::save_profile(connection, &event.sender, profile)?;
        Database::save_event_helper(connection, event, profile_id)?;

        Ok(())
    }

    pub(crate) fn event_in_store(&self) -> bool {
        false
    }

    pub(crate) fn load_events(&self, search_result: &[(f32, String)]) -> rusqlite::Result<Vec<(f32, String, i64)>> {
        let event_num = search_result.len();
        let parameter_str = std::iter::repeat(", ?")
            .take(event_num - 1)
            .collect::<String>();

        let mut stmt = self.connection.prepare(&format!(
            "SELECT source, profile_id
             FROM events WHERE event_id IN (?{})
             ",
            &parameter_str
        ))?;
        let (scores, event_ids): (Vec<f32>, Vec<String>) = search_result.iter().cloned().unzip();
        let db_events = stmt.query_map(event_ids, |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
            ))
        })?;

        let mut events = Vec::new();
        let i = 0;

        for row in db_events {
            let (e, p_id): (String, i64) = row?;
            events.push((scores[i], e, p_id));
        }

        Ok(events)
    }

    pub fn search(&self, term: &str) -> Vec<(f32, String, i64)> {
        let search_result = self.index.search(term);
        match self.load_events(&search_result) {
            Ok(result) => result,
            Err(_e) => vec!()
        }
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
    let events = db
        .load_events(&[(1.0, "$15163622445EBvZJ:localhost".to_string()), (0.3, "$FAKE".to_string())])
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

    let events = db
        .load_events(&[(1.0, "$15163622445EBvZJ:localhost".to_string()), (0.3, "$FAKE".to_string())])
        .unwrap();

    assert_eq!(*EVENT.source, events[0].1)
}

#[test]
fn save_and_search() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(&tmpdir).unwrap();
    let profile = Profile::new("Alice", "");

    db.add_event(EVENT.clone(), profile);
    db.commit();
    db.commit();

    let result = db.search("Test");
    assert_eq!(result[0].1, EVENT.source);
}
