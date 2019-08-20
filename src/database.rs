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
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::thread::JoinHandle;
use tempdir::TempDir;

#[derive(Debug, PartialEq, Default)]
pub(crate) struct Event {
    pub(crate) event_id: String,
    pub(crate) sender: String,
    pub(crate) server_ts: i64,
    pub(crate) room_id: String,
    pub(crate) source: String,
}

enum ThreadMessage {
    Event(Event),
    Write,
    ShutDown,
}

#[derive(Debug)]
pub enum Error {
    PoolError(r2d2::Error),
    DatabaseError(rusqlite::Error),
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

impl Event {
    pub(crate) fn new(
        event_id: &str,
        sender: &str,
        server_ts: i64,
        room_id: &str,
        source: &str,
    ) -> Event {
        Event {
            event_id: event_id.to_string(),
            sender: sender.to_string(),
            server_ts,
            room_id: room_id.to_string(),
            source: source.to_string(),
        }
    }
}

pub(crate) struct Profile {
    pub(crate) display_name: String,
    pub(crate) avatar_url: String,
}

impl Profile {
    pub(crate) fn new(display_name: &str, avatar_url: &str) -> Profile {
        Profile {
            display_name: display_name.to_string(),
            avatar_url: avatar_url.to_string(),
        }
    }
}

pub(crate) struct Database {
    connection: PooledConnection<SqliteConnectionManager>,
    pool: r2d2::Pool<SqliteConnectionManager>,
    write_thread: JoinHandle<()>,
    tx: Sender<ThreadMessage>,
}

impl Database {
    pub(crate) fn new<P: AsRef<Path>>(path: P, db_name: &str) -> Result<Database> {
        let db_path = path.as_ref().join(db_name);
        let manager = SqliteConnectionManager::file(db_path);
        let pool = r2d2::Pool::new(manager)?;

        let connection = pool.get()?;

        Database::create_tables(&connection)?;

        let (t_handle, tx) = Database::spawn_writer(pool.get()?);

        Ok(Database {
            connection,
            pool,
            write_thread: t_handle,
            tx,
        })
    }

    fn spawn_writer(
        connection: PooledConnection<SqliteConnectionManager>,
    ) -> (JoinHandle<()>, Sender<ThreadMessage>) {
        let (tx, rx): (_, Receiver<ThreadMessage>) = channel();

        let t_handle = thread::spawn(move || loop {
            let mut events: Vec<Event> = Vec::new();
            let message = rx.recv();

            match message {
                Ok(m) => match m {
                    ThreadMessage::Event(e) => events.push(e),
                    ThreadMessage::ShutDown => return,
                    ThreadMessage::Write => continue,
                },
                Err(_e) => return,
            };
        });

        (t_handle, tx)
    }

    pub(crate) fn new_memory_db() -> Result<Database> {
        let manager = SqliteConnectionManager::memory();
        let pool = r2d2::Pool::new(manager)?;

        let connection = pool.get()?;
        Database::create_tables(&connection)?;
        let (t_handle, tx) = Database::spawn_writer(pool.get()?);

        Ok(Database {
            connection,
            pool,
            write_thread: t_handle,
            tx,
        })
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
                UNIQUE(event_id, room_id, sender, profile_id)
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

    pub(crate) fn save_event(&self, event: &Event, profile: &Profile) -> Result<()> {
        let profile_id = Database::save_profile(&self.connection, &event.sender, profile)?;
        Database::save_event_helper(&self.connection, event, profile_id)?;

        Ok(())
    }

    pub(crate) fn event_in_store(&self) -> bool {
        false
    }

    pub(crate) fn load_events(&self, event_ids: &[&str]) -> rusqlite::Result<Vec<Event>> {
        let event_num = event_ids.len();
        let parameter_str = std::iter::repeat(", ?")
            .take(event_num - 1)
            .collect::<String>();

        let mut stmt = self.connection.prepare(&format!(
            "SELECT event_id, sender, server_ts, room_id, source, profile_id
             FROM events WHERE event_id IN (?{})
             ",
            &parameter_str
        ))?;
        let db_events = stmt.query_map(event_ids, |row| {
            Ok((
                Event {
                    event_id: row.get(0)?,
                    sender: row.get(1)?,
                    server_ts: row.get(2)?,
                    room_id: row.get(3)?,
                    source: row.get(4)?,
                },
                row.get(5)?,
            ))
        })?;

        let mut events = Vec::new();

        for row in db_events {
            let (e, p_id): (Event, i64) = row?;
            events.push(e);
        }

        Ok(events)
    }
}

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

lazy_static! {
    static ref EVENT: Event = Event::new(
        "$15163622445EBvZJ:localhost",
        "@example2:localhost",
        1516362244026,
        "!test_room:localhost",
        EVENT_SOURCE
    );
}

#[test]
fn create_event_db() {
    let tmpdir = TempDir::new("matrix-search").unwrap();
    let _db = Database::new(tmpdir, "events.db").unwrap();
}

#[test]
fn store_profile() {
    let db = Database::new_memory_db().unwrap();

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
    let db = Database::new_memory_db().unwrap();
    let profile = Profile::new("Alice", "");
    let id = Database::save_profile(&db.connection, "@alice.example.org", &profile).unwrap();

    Database::save_event_helper(&db.connection, &EVENT, id).unwrap();
}

#[test]
fn store_event_and_profile() {
    let db = Database::new_memory_db().unwrap();
    let profile = Profile::new("Alice", "");
    db.save_event(&EVENT, &profile).unwrap();
}

#[test]
fn load_event() {
    let db = Database::new_memory_db().unwrap();
    let profile = Profile::new("Alice", "");

    db.save_event(&EVENT, &profile).unwrap();
    let events = db
        .load_events(&["$15163622445EBvZJ:localhost", "$FAKE"])
        .unwrap();

    assert_eq!(*EVENT, events[0])
}
