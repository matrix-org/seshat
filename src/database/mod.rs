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

use fs_extra::dir;
use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{ToSql, NO_PARAMS};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use zeroize::Zeroizing;

use crate::config::{Config, LoadConfig, LoadDirection, SearchConfig};
use crate::error::{Error, Result};
use crate::events::{
    CrawlerCheckpoint, Event, EventContext, EventId, HistoricEventsT, MxId, Profile,
    SerializedEvent,
};
use crate::index::{Index, IndexSearcher, Writer as IndexWriter};

#[cfg(test)]
use fake::{Fake, Faker};
#[cfg(test)]
use std::time;
#[cfg(test)]
use tempfile::tempdir;

#[cfg(test)]
use crate::events::CheckpointDirection;
#[cfg(test)]
use crate::EVENT;

const DATABASE_VERSION: i64 = 1;
const FILE_EVENT_TYPES: &str = "'m.image', 'm.file', 'm.audio', 'm.video'";

pub(crate) enum ThreadMessage {
    Event((Event, Profile)),
    HistoricEvents(HistoricEventsT),
    Write(Sender<Result<()>>, bool),
}

#[derive(Debug, PartialEq, Default, Clone)]
/// A search result
pub struct SearchResult {
    /// The score that the full text search assigned to this event.
    pub score: f32,
    /// The serialized source of the event that matched a search.
    pub event_source: SerializedEvent,
    /// Events that happened before our matched event.
    pub events_before: Vec<SerializedEvent>,
    /// Events that happened after our matched event.
    pub events_after: Vec<SerializedEvent>,
    /// The profile of the sender of the matched event.
    pub profile_info: HashMap<MxId, Profile>,
}

/// Statistical information about the database.
pub struct DatabaseStats {
    /// The number number of bytes the database is using on disk.
    pub size: u64,
    /// The number of events that the database knows about.
    pub event_count: u64,
    /// The number of rooms that the database knows about.
    pub room_count: u64,
}

/// The main entry point to the index and database.
pub struct Searcher {
    inner: IndexSearcher,
    database: Arc<PooledConnection<SqliteConnectionManager>>,
}

struct Writer {
    inner: IndexWriter,
    connection: r2d2::PooledConnection<SqliteConnectionManager>,
    events: Vec<(Event, Profile)>,
    uncommitted_events: Vec<i64>,
}

impl Writer {
    fn new(
        connection: r2d2::PooledConnection<SqliteConnectionManager>,
        index_writer: IndexWriter,
    ) -> Self {
        Writer {
            inner: index_writer,
            connection,
            events: Vec::new(),
            uncommitted_events: Vec::new(),
        }
    }

    fn add_event(&mut self, event: Event, profile: Profile) {
        self.events.push((event, profile));
    }

    fn write_queued_events(&mut self, force_commit: bool) -> Result<()> {
        Database::write_events(
            &mut self.connection,
            &mut self.inner,
            (None, None, &mut self.events),
            force_commit,
            &mut self.uncommitted_events,
        )?;

        Ok(())
    }

    fn write_historic_events(
        &mut self,
        checkpoint: Option<CrawlerCheckpoint>,
        old_checkpoint: Option<CrawlerCheckpoint>,
        mut events: Vec<(Event, Profile)>,
        force_commit: bool,
    ) -> Result<bool> {
        Database::write_events(
            &mut self.connection,
            &mut self.inner,
            (checkpoint, old_checkpoint, &mut events),
            force_commit,
            &mut self.uncommitted_events,
        )
    }

    fn load_uncommitted_events(&mut self) -> Result<()> {
        let mut ret = Database::load_uncommitted_events(&self.connection)?;

        for (id, event) in ret.drain(..) {
            self.uncommitted_events.push(id);
            self.inner.add_event(&event);
        }

        Ok(())
    }
}

impl Searcher {
    /// Search the index and return events matching a search term.
    /// # Arguments
    ///
    /// * `term` - The search term that should be used to search the index.
    /// * `config` - A SearchConfig that will modify what the search result
    /// should contain.
    ///
    /// Returns a list of `SearchResult`.
    pub fn search(&self, term: &str, config: &SearchConfig) -> Result<Vec<SearchResult>> {
        let search_result = self.inner.search(term, config)?;

        if search_result.is_empty() {
            return Ok(vec![]);
        }

        Ok(Database::load_events(
            &self.database,
            &search_result,
            config.before_limit,
            config.after_limit,
            config.order_by_recency,
        )?)
    }
}

unsafe impl Send for Searcher {}

/// The Seshat database.
pub struct Database {
    path: PathBuf,
    connection: Arc<PooledConnection<SqliteConnectionManager>>,
    pool: r2d2::Pool<SqliteConnectionManager>,
    _write_thread: JoinHandle<()>,
    tx: Sender<ThreadMessage>,
    index: Index,
    passphrase: Option<Zeroizing<String>>,
}

/// A Seshat database connection.
/// The connection can be used to read data out of the database using a
/// separate thread.
pub struct Connection {
    inner: PooledConnection<SqliteConnectionManager>,
    path: PathBuf,
}

impl Connection {
    /// Load all the previously stored crawler checkpoints from the database.
    /// # Arguments
    pub fn load_checkpoints(&self) -> Result<Vec<CrawlerCheckpoint>> {
        let mut stmt = self.prepare(
            "SELECT room_id, token, full_crawl, direction
                                    FROM crawlercheckpoints",
        )?;

        let rows = stmt.query_map(NO_PARAMS, |row| {
            Ok(CrawlerCheckpoint {
                room_id: row.get(0)?,
                token: row.get(1)?,
                full_crawl: row.get(2)?,
                direction: row.get(3)?,
            })
        })?;

        let mut checkpoints = Vec::new();

        for row in rows {
            let checkpoint: CrawlerCheckpoint = row?;
            checkpoints.push(checkpoint);
        }
        Ok(checkpoints)
    }

    /// Is the database empty.
    /// Returns true if the database is empty, false otherwise.
    pub fn is_empty(&self) -> Result<bool> {
        let event_count: i64 = Database::get_event_count(&self.inner)?;
        let checkpoint_count: i64 = self.query_row(
            "SELECT COUNT(*) FROM crawlercheckpoints",
            NO_PARAMS,
            |row| row.get(0),
        )?;

        Ok(event_count == 0 && checkpoint_count == 0)
    }

    /// Get statistical information of the database.
    pub fn get_stats(&self) -> Result<DatabaseStats> {
        let event_count = Database::get_event_count(&self.inner)? as u64;
        let room_count = Database::get_room_count(&self.inner)? as u64;
        let size = dir::get_size(&self.path)?;
        Ok(DatabaseStats {
            size,
            event_count,
            room_count,
        })
    }

    /// Load events that contain an mxc URL to a file.
    /// # Arguments
    ///
    /// * `load_config` - Configuration deciding which events and how many of
    /// them should be loaded.
    ///
    /// # Examples
    ///
    /// ```noexecute
    /// let config = LoadConfig::new("!testroom:localhost").limit(10);
    /// let result = connection.load_file_events(&config);
    /// ```
    ///
    /// Returns a list of tuples containing the serialized events and the
    /// profile of the sender at the time when the event was sent.
    pub fn load_file_events(
        &self,
        load_config: &LoadConfig,
    ) -> Result<Vec<(SerializedEvent, Profile)>> {
        Ok(Database::load_file_events(
            self,
            &load_config.room_id,
            load_config.limit,
            load_config.from_event.as_ref().map(|x| &**x),
            &load_config.direction,
        )?)
    }
}

impl Deref for Connection {
    type Target = PooledConnection<SqliteConnectionManager>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Connection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

type WriterRet = (JoinHandle<()>, Sender<ThreadMessage>);

impl Database {
    /// Create a new Seshat database or open an existing one.
    /// # Arguments
    ///
    /// * `path` - The directory where the database will be stored in. This
    /// should be an empty directory if a new database should be created.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Database>
    where
        PathBuf: std::convert::From<P>,
    {
        Database::new_with_config(path, &Config::new())
    }

    /// Create a new Seshat database or open an existing one with the given
    /// configuration.
    /// # Arguments
    ///
    /// * `path` - The directory where the database will be stored in. This
    /// should be an empty directory if a new database should be created.
    /// * `config` - Configuration that changes the behaviour of the database.
    pub fn new_with_config<P: AsRef<Path>>(path: P, config: &Config) -> Result<Database>
    where
        PathBuf: std::convert::From<P>,
    {
        let db_path = path.as_ref().join("events.db");
        let manager = SqliteConnectionManager::file(&db_path);
        let pool = r2d2::Pool::new(manager)?;

        let connection = Arc::new(pool.get()?);

        if let Some(ref p) = config.passphrase {
            Database::unlock(&connection, p)?;
        }

        Database::create_tables(&connection)?;

        let version = match Database::get_version(&connection) {
            Ok(v) => v,
            Err(e) => return Err(Error::DatabaseOpenError(e.to_string())),
        };

        if version != DATABASE_VERSION {
            return Err(Error::DatabaseVersionError);
        }

        let index = Database::create_index(&path, &config)?;
        let writer = index.get_writer()?;

        // Warning: Do not open a new db connection before we write the tables
        // to the DB, otherwise sqlcipher might think that we are initializing
        // a new database and we'll end up with two connections using differing
        // keys and writes/reads to one of the connections might fail.
        let writer_connection = pool.get()?;
        if let Some(ref p) = config.passphrase {
            Database::unlock(&writer_connection, p)?;
        }

        let (t_handle, tx) = Database::spawn_writer(writer_connection, writer)?;

        Ok(Database {
            path: path.into(),
            connection,
            pool,
            _write_thread: t_handle,
            tx,
            index,
            passphrase: config.passphrase.clone(),
        })
    }

    /// Change the passphrase of the Seshat database.
    ///
    /// Note that this consumes the database object and any searcher objects
    /// can't be used anymore. A new database will have to be opened and new
    /// searcher objects as well.
    ///
    /// # Arguments
    ///
    /// * `path` - The directory where the database will be stored in. This
    /// should be an empty directory if a new database should be created.
    /// * `new_passphrase` - The passphrase that should be used instead of the
    /// current one.
    pub fn change_passphrase(self, new_passphrase: &str) -> Result<()> {
        match self.passphrase {
            Some(p) => {
                Index::change_passphrase(&self.path, &p, new_passphrase)?;
                self.connection
                    .pragma_update(None, "rekey", &new_passphrase as &dyn ToSql)?;
            }
            None => panic!("Database isn't encrypted"),
        }
        Ok(())
    }

    fn unlock(connection: &rusqlite::Connection, passphrase: &str) -> Result<()> {
        let mut statement = connection.prepare("PRAGMA cipher_version")?;
        let results = statement.query_map(NO_PARAMS, |row| row.get::<usize, String>(0))?;

        if results.count() != 1 {
            return Err(Error::SqlCipherError(
                "Sqlcipher support is missing".to_string(),
            ));
        }

        connection.pragma_update(None, "key", &passphrase as &dyn ToSql)?;

        let count: std::result::Result<i64, rusqlite::Error> =
            connection.query_row("SELECT COUNT(*) FROM sqlite_master", NO_PARAMS, |row| {
                row.get(0)
            });

        match count {
            Ok(_) => Ok(()),
            Err(_) => Err(Error::DatabaseUnlockError("Invalid passphrase".to_owned())),
        }
    }

    /// Get the size of the database.
    /// This returns the number of bytes the database is using on disk.
    pub fn get_size(&self) -> Result<u64> {
        Ok(dir::get_size(self.get_path())?)
    }

    /// Get the path of the directory where the Seshat database lives in.
    pub fn get_path(&self) -> &Path {
        self.path.as_path()
    }

    fn create_index<P: AsRef<Path>>(path: &P, config: &Config) -> Result<Index> {
        Ok(Index::new(path, &config)?)
    }

    /// Write the events to the database.
    /// Returns a tuple containing a boolean and an array if integers. The
    /// boolean notifies us if all the events were already added to the
    /// database, the integers are the database ids of our events.
    fn write_events_helper(
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
    fn mark_events_as_indexed(
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

    fn write_events(
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

    fn spawn_writer(
        connection: PooledConnection<SqliteConnectionManager>,
        index_writer: IndexWriter,
    ) -> Result<WriterRet> {
        let (tx, rx): (_, Receiver<ThreadMessage>) = channel();

        let t_handle = thread::spawn(move || {
            let mut writer = Writer::new(connection, index_writer);
            let mut loaded_uncommitted = false;

            while let Ok(message) = rx.recv() {
                match message {
                    ThreadMessage::Event((event, profile)) => writer.add_event(event, profile),
                    ThreadMessage::Write(sender, force_commit) => {
                        // We may have events that aren't committed to the index
                        // but are stored in the db, let us load them from the
                        // db and commit them to the index now. They will later
                        // be marked as committed in the database as part of a
                        // normal write.
                        if !loaded_uncommitted {
                            let ret = writer.load_uncommitted_events();

                            loaded_uncommitted = true;

                            if ret.is_err() {
                                sender.send(ret).unwrap_or(());
                                continue;
                            }
                        }
                        let ret = writer.write_queued_events(force_commit);
                        // Notify that we are done with the write.
                        sender.send(ret).unwrap_or(());
                    }
                    ThreadMessage::HistoricEvents(m) => {
                        let (check, old_check, events, sender) = m;
                        let ret = writer.write_historic_events(check, old_check, events, true);
                        sender.send(ret).unwrap_or(());
                    }
                };
            }
        });

        Ok((t_handle, tx))
    }

    /// Add an event with the given profile to the database.
    /// # Arguments
    ///
    /// * `event` - The directory where the database will be stored in. This
    /// * `profile` - The directory where the database will be stored in. This
    ///
    /// This is a fast non-blocking operation, it only queues up the event to be
    /// added to the database. The events will be committed to the database
    /// only when the user calls the `commit()` method.
    pub fn add_event(&self, event: Event, profile: Profile) {
        let message = ThreadMessage::Event((event, profile));
        self.tx.send(message).unwrap();
    }

    fn commit_helper(&mut self, force: bool) -> Receiver<Result<()>> {
        let (sender, receiver): (_, Receiver<Result<()>>) = channel();
        self.tx.send(ThreadMessage::Write(sender, force)).unwrap();
        receiver
    }

    /// Commit the currently queued up events. This method will block. A
    /// non-blocking version of this method exists in the `commit_no_wait()`
    /// method.
    pub fn commit(&mut self) -> Result<()> {
        self.commit_helper(false).recv().unwrap()
    }

    /// Commit the currently queued up events forcing the commit to the index.
    ///
    /// Commits are usually rate limited. This gets around the limit and forces
    /// the documents to be added to the index.
    ///
    /// This method will block. A non-blocking version of this method exists in
    /// the `force_commit_no_wait()` method.
    ///
    /// This should only be used for testing purposes.
    pub fn force_commit(&mut self) -> Result<()> {
        self.commit_helper(true).recv().unwrap()
    }

    /// Reload the database so that a search reflects the state of the last
    /// commit. Note that this happens automatically and this method should be
    /// used only in unit tests.
    pub fn reload(&mut self) -> Result<()> {
        self.index.reload()?;
        Ok(())
    }

    /// Commit the currently queued up events without waiting for confirmation
    /// that the operation is done.
    ///
    /// Returns a receiver that will receive an empty message once the commit is
    /// done.
    pub fn commit_no_wait(&mut self) -> Receiver<Result<()>> {
        self.commit_helper(false)
    }

    /// Commit the currently queued up events forcing the commit to the index.
    ///
    /// Commits are usually rate limited. This gets around the limit and forces
    /// the documents to be added to the index.
    ///
    /// This should only be used for testing purposes.
    ///
    /// Returns a receiver that will receive an empty message once the commit is
    /// done.
    pub fn force_commit_no_wait(&mut self) -> Receiver<Result<()>> {
        self.commit_helper(true)
    }

    /// Add the given events from the room history to the database.
    /// # Arguments
    ///
    /// * `events` - The events that will be added.
    /// * `new_checkpoint` - A checkpoint that states where we need to continue
    /// fetching events from the room history. This checkpoint will be
    /// persisted in the database.
    /// * `old_checkpoint` - The checkpoint that was used to fetch the given
    /// events. This checkpoint will be removed from the database.
    pub fn add_historic_events(
        &self,
        events: Vec<(Event, Profile)>,
        new_checkpoint: Option<CrawlerCheckpoint>,
        old_checkpoint: Option<CrawlerCheckpoint>,
    ) -> Receiver<Result<bool>> {
        let (sender, receiver): (_, Receiver<Result<bool>>) = channel();
        let payload = (new_checkpoint, old_checkpoint, events, sender);
        let message = ThreadMessage::HistoricEvents(payload);
        self.tx.send(message).unwrap();

        receiver
    }

    fn get_version(connection: &rusqlite::Connection) -> Result<i64> {
        connection.execute(
            "INSERT OR IGNORE INTO version ( version ) VALUES(?1)",
            &[DATABASE_VERSION],
        )?;

        let version: i64 =
            connection.query_row("SELECT version FROM version", NO_PARAMS, |row| row.get(0))?;

        // Do database migrations here before bumping the database version.

        Ok(version)
    }

    fn create_tables(conn: &rusqlite::Connection) -> Result<()> {
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
    fn load_event_context(
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

    /// Search the index and return events matching a search term.
    /// This is just a helper function that gets a searcher and performs a
    /// search on it immediately.
    /// # Arguments
    ///
    /// * `term` - The search term that should be used to search the index.
    pub fn search(&self, term: &str, config: &SearchConfig) -> Result<Vec<SearchResult>> {
        let searcher = self.get_searcher();
        searcher.search(term, config)
    }

    /// Get a searcher that can be used to perform a search.
    pub fn get_searcher(&self) -> Searcher {
        let index_searcher = self.index.get_searcher();
        Searcher {
            inner: index_searcher,
            database: self.connection.clone(),
        }
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

    /// Get a database connection.
    /// Note that this connection should only be used for reading.
    pub fn get_connection(&self) -> Result<Connection> {
        let connection = self.pool.get()?;

        if let Some(ref p) = self.passphrase {
            Database::unlock(&connection, p)?;
        }

        Ok(Connection {
            inner: connection,
            path: self.path.clone(),
        })
    }

    /// Delete the database.
    /// Warning: This will delete the whole path that was provided at the
    /// database creation time.
    pub fn delete(self) -> std::io::Result<()> {
        fs::remove_dir_all(self.path)?;
        Ok(())
    }
}

#[test]
fn create_event_db() {
    let tmpdir = tempdir().unwrap();
    let _db = Database::new(tmpdir.path()).unwrap();
}

#[test]
fn store_profile() {
    let tmpdir = tempdir().unwrap();
    let db = Database::new(tmpdir.path()).unwrap();

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
    let db = Database::new(tmpdir.path()).unwrap();

    let profile = Profile {
        displayname: None,
        avatar_url: None,
    };
    let id = Database::save_profile(&db.connection, "@alice.example.org", &profile);
    assert_eq!(id.unwrap(), 1);
}

#[test]
fn store_event() {
    let tmpdir = tempdir().unwrap();
    let db = Database::new(tmpdir.path()).unwrap();
    let profile = Profile::new("Alice", "");
    let id = Database::save_profile(&db.connection, "@alice.example.org", &profile).unwrap();

    let id = Database::save_event_helper(&db.connection, &EVENT, id).unwrap();
    assert_eq!(id, 1);
}

#[test]
fn store_event_and_profile() {
    let tmpdir = tempdir().unwrap();
    let db = Database::new(tmpdir.path()).unwrap();
    let profile = Profile::new("Alice", "");
    Database::save_event(&db.connection, &EVENT, &profile).unwrap();
}

#[test]
fn load_event() {
    let tmpdir = tempdir().unwrap();
    let db = Database::new(tmpdir.path()).unwrap();
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
        false,
    )
    .unwrap();

    assert_eq!(*EVENT.source, events[0].event_source)
}

#[test]
fn commit_a_write() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(tmpdir.path()).unwrap();
    db.commit().unwrap();
}

#[test]
fn save_the_event_multithreaded() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(tmpdir.path()).unwrap();
    let profile = Profile::new("Alice", "");

    db.add_event(EVENT.clone(), profile);
    db.commit().unwrap();
    db.reload().unwrap();

    let events = Database::load_events(
        &db.connection,
        &[
            (1.0, "$15163622445EBvZJ:localhost".to_string()),
            (0.3, "$FAKE".to_string()),
        ],
        0,
        0,
        false,
    )
    .unwrap();

    assert_eq!(*EVENT.source, events[0].event_source)
}

#[test]
fn load_a_profile() {
    let tmpdir = tempdir().unwrap();
    let db = Database::new(tmpdir.path()).unwrap();

    let profile = Profile::new("Alice", "");
    let user_id = "@alice.example.org";
    let profile_id = Database::save_profile(&db.connection, user_id, &profile).unwrap();

    let loaded_profile = Database::load_profile(&db.connection, profile_id).unwrap();

    assert_eq!(profile, loaded_profile);
}

#[test]
fn load_event_context() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(tmpdir.path()).unwrap();
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

    db.commit().unwrap();

    for i in 1..5 {
        let (before, after, _) =
            Database::load_event_context(&db.connection, &EVENT, 1, 1).unwrap();

        if (before.len() != 1
            || after.len() != 1
            || before[0] != before_event.as_ref().unwrap().source
            || after[0] != after_event.as_ref().unwrap().source)
            && i != 10
        {
            thread::sleep(time::Duration::from_millis(10));
            continue;
        }

        assert_eq!(before.len(), 1);
        assert_eq!(before[0], before_event.as_ref().unwrap().source);
        assert_eq!(after.len(), 1);
        assert_eq!(after[0], after_event.as_ref().unwrap().source);

        return;
    }
}

#[test]
fn save_and_load_checkpoints() {
    let tmpdir = tempdir().unwrap();
    let db = Database::new(tmpdir.path()).unwrap();

    let checkpoint = CrawlerCheckpoint {
        room_id: "!test:room".to_string(),
        token: "1234".to_string(),
        full_crawl: false,
        direction: CheckpointDirection::Backwards,
    };

    let mut connection = db.get_connection().unwrap();
    let transaction = connection.transaction().unwrap();

    Database::replace_crawler_checkpoint(&transaction, Some(&checkpoint), None).unwrap();
    transaction.commit().unwrap();

    let checkpoints = connection.load_checkpoints().unwrap();

    println!("{:?}", checkpoints);

    assert!(checkpoints.contains(&checkpoint));

    let new_checkpoint = CrawlerCheckpoint {
        room_id: "!test:room".to_string(),
        token: "12345".to_string(),
        full_crawl: false,
        direction: CheckpointDirection::Backwards,
    };

    Database::replace_crawler_checkpoint(&connection, Some(&new_checkpoint), Some(&checkpoint))
        .unwrap();

    let checkpoints = connection.load_checkpoints().unwrap();

    assert!(!checkpoints.contains(&checkpoint));
    assert!(checkpoints.contains(&new_checkpoint));
}

#[test]
fn duplicate_empty_profiles() {
    let tmpdir = tempdir().unwrap();
    let db = Database::new(tmpdir.path()).unwrap();
    let profile = Profile {
        displayname: None,
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
fn is_empty() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(tmpdir.path()).unwrap();
    let connection = db.get_connection().unwrap();
    assert!(connection.is_empty().unwrap());

    let profile = Profile::new("Alice", "");
    db.add_event(EVENT.clone(), profile);
    db.commit().unwrap();
    assert!(!connection.is_empty().unwrap());
}

#[test]
fn encrypted_db() {
    let tmpdir = tempdir().unwrap();
    let db_config = Config::new().set_passphrase("test");
    let mut db = match Database::new_with_config(tmpdir.path(), &db_config) {
        Ok(db) => db,
        Err(e) => panic!("Coulnd't open encrypted database {}", e),
    };

    let connection = match db.get_connection() {
        Ok(c) => c,
        Err(e) => panic!("Could not get database connection {}", e),
    };

    assert!(
        connection.is_empty().unwrap(),
        "New database should be empty"
    );

    let profile = Profile::new("Alice", "");
    db.add_event(EVENT.clone(), profile);

    match db.commit() {
        Ok(_) => (),
        Err(e) => panic!("Could not commit events to database {}", e),
    }
    assert!(
        !connection.is_empty().unwrap(),
        "Database shouldn't be empty anymore"
    );

    drop(db);

    let db = Database::new(tmpdir.path());
    assert!(
        db.is_err(),
        "opening the database without a passphrase should fail"
    );
}

#[test]
fn change_passphrase() {
    let tmpdir = tempdir().unwrap();
    let db_config = Config::new().set_passphrase("test");
    let mut db = match Database::new_with_config(tmpdir.path(), &db_config) {
        Ok(db) => db,
        Err(e) => panic!("Coulnd't open encrypted database {}", e),
    };

    let connection = db
        .get_connection()
        .expect("Could not get database connection");
    assert!(
        connection.is_empty().unwrap(),
        "New database should be empty"
    );

    let profile = Profile::new("Alice", "");
    db.add_event(EVENT.clone(), profile);

    db.commit().expect("Could not commit events to database");
    db.change_passphrase("wordpass")
        .expect("Could not change the database passphrase");

    let db_config = Config::new().set_passphrase("wordpass");
    let db = Database::new_with_config(tmpdir.path(), &db_config)
        .expect("Could not open database with the new passphrase");
    let connection = db
        .get_connection()
        .expect("Could not get database connection");
    assert!(
        !connection.is_empty().unwrap(),
        "Database shouldn't be empty anymore"
    );
    drop(db);

    let db_config = Config::new().set_passphrase("test");
    let db = Database::new_with_config(tmpdir.path(), &db_config);
    assert!(
        db.is_err(),
        "opening the database without a passphrase should fail"
    );
}

#[test]
fn resume_committing() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(tmpdir.path()).unwrap();
    let profile = Profile::new("Alice", "");

    // Check that we don't have any uncommitted events.
    assert!(Database::load_uncommitted_events(&db.connection)
        .unwrap()
        .is_empty());

    db.add_event(EVENT.clone(), profile);
    db.commit().unwrap();
    db.reload().unwrap();

    // Now we do have uncommitted events.
    assert!(!Database::load_uncommitted_events(&db.connection)
        .unwrap()
        .is_empty());

    // Since the event wasn't committed to the index the search should fail.
    assert!(db.search("test", &SearchConfig::new()).unwrap().is_empty());

    // Let us drop the DB to check if we're loading the uncommitted events
    // correctly.
    drop(db);
    let mut counter = 0;
    let mut db = Database::new(tmpdir.path());

    // Tantivy might still be in the process of being shut down
    // and hold on to the write lock. Meaning that opening the database might
    // not succeed immediately. Retry a couple of times before giving up.
    while db.is_err() {
        counter += 1;
        if counter > 10 {
            break;
        }
        thread::sleep(time::Duration::from_millis(10));
        db = Database::new(tmpdir.path())
    }

    let mut db = db.unwrap();

    // We still have uncommitted events.
    assert_eq!(
        Database::load_uncommitted_events(&db.connection).unwrap()[0].1,
        *EVENT
    );

    db.force_commit().unwrap();
    db.reload().unwrap();

    // A forced commit gets rid of our uncommitted events.
    assert!(Database::load_uncommitted_events(&db.connection)
        .unwrap()
        .is_empty());

    let result = db.search("test", &SearchConfig::new()).unwrap();

    // The search is now successful.
    assert!(!result.is_empty());
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].event_source, EVENT.source);
}

#[test]
fn delete_uncommitted() {
    let tmpdir = tempdir().unwrap();
    let db_config = Config::new().set_passphrase("test");
    let mut db = Database::new_with_config(tmpdir.path(), &db_config).unwrap();
    let profile = Profile::new("Alice", "");

    for i in 1..1000 {
        let mut event: Event = Faker.fake();
        event.server_ts += i;
        db.add_event(event, profile.clone());

        if i % 100 == 0 {
            db.commit().unwrap();
        }
    }

    db.force_commit().unwrap();
    assert!(Database::load_uncommitted_events(&db.connection)
        .unwrap()
        .is_empty());
}

#[test]
fn stats_getting() {
    let tmpdir = tempdir().unwrap();
    let db_config = Config::new().set_passphrase("test");
    let mut db = Database::new_with_config(tmpdir.path(), &db_config).unwrap();
    let profile = Profile::new("Alice", "");

    for i in 0..1000 {
        let mut event: Event = Faker.fake();
        event.server_ts += i;
        db.add_event(event, profile.clone());
    }

    db.commit().unwrap();

    let connection = db.get_connection().unwrap();

    let stats = connection.get_stats().unwrap();

    assert_eq!(stats.event_count, 1000);
    assert_eq!(stats.room_count, 1);
    assert!(stats.size > 0);
}
