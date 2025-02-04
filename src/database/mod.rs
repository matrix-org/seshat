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

mod connection;
mod recovery;
mod searcher;
mod static_methods;
mod writer;

use fs_extra::dir;
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::ToSql;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        mpsc::{channel, Receiver, Sender},
        Arc, Mutex,
    },
    thread,
    thread::JoinHandle,
};

pub use crate::database::{
    connection::{Connection, DatabaseStats},
    recovery::{RecoveryDatabase, RecoveryInfo},
    searcher::{SearchBatch, SearchResult, Searcher},
};
use crate::{
    config::{Config, SearchConfig},
    database::writer::Writer,
    error::{Error, Result},
    events::{CrawlerCheckpoint, Event, EventId, HistoricEventsT, Profile},
    index::{Index, Writer as IndexWriter},
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

const DATABASE_VERSION: i64 = 4;
const EVENTS_DB_NAME: &str = "events.db";

pub(crate) enum ThreadMessage {
    Event((Event, Profile)),
    HistoricEvents(HistoricEventsT),
    Write(Sender<Result<()>>, bool),
    Delete(Sender<Result<bool>>, EventId),
    ShutDown(Sender<Result<()>>),
}

/// The Seshat database.
pub struct Database {
    path: PathBuf,
    connection: Arc<Mutex<PooledConnection<SqliteConnectionManager>>>,
    pool: r2d2::Pool<SqliteConnectionManager>,
    _write_thread: JoinHandle<()>,
    tx: Sender<ThreadMessage>,
    index: Index,
    config: Config,
}

type WriterRet = (JoinHandle<()>, Sender<ThreadMessage>);

impl Database {
    /// Create a new Seshat database or open an existing one.
    /// # Arguments
    ///
    /// * `path` - The directory where the database will be stored in. This
    ///   should be an empty directory if a new database should be created.
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
    ///   should be an empty directory if a new database should be created.
    /// * `config` - Configuration that changes the behaviour of the database.
    pub fn new_with_config<P: AsRef<Path>>(path: P, config: &Config) -> Result<Database>
    where
        PathBuf: std::convert::From<P>,
    {
        let db_path = path.as_ref().join(EVENTS_DB_NAME);
        let pool = Self::get_pool(&db_path, config)?;

        let mut connection = pool.get()?;

        Database::unlock(&connection, config)?;
        Database::set_pragmas(&connection)?;

        let (version, reindex_needed) = match Database::get_version(&mut connection) {
            Ok(ret) => ret,
            Err(e) => return Err(Error::DatabaseOpenError(e.to_string())),
        };

        Database::create_tables(&connection)?;

        if version != DATABASE_VERSION {
            return Err(Error::DatabaseVersionError);
        }

        if reindex_needed {
            return Err(Error::ReindexError);
        }

        let index = Database::create_index(&path, config)?;
        let writer = index.get_writer()?;

        // Warning: Do not open a new db connection before we write the tables
        // to the DB, otherwise sqlcipher might think that we are initializing
        // a new database and we'll end up with two connections using differing
        // keys and writes/reads to one of the connections might fail.
        let writer_connection = pool.get()?;
        Database::unlock(&writer_connection, config)?;
        Database::set_pragmas(&writer_connection)?;

        let (t_handle, tx) = Database::spawn_writer(writer_connection, writer);

        Ok(Database {
            path: path.into(),
            connection: Arc::new(Mutex::new(connection)),
            pool,
            _write_thread: t_handle,
            tx,
            index,
            config: config.clone(),
        })
    }

    fn get_pool(db_path: &PathBuf, config: &Config) -> Result<Pool<SqliteConnectionManager>> {
        let manager = SqliteConnectionManager::file(db_path);
        let pool = r2d2::Pool::new(manager)?;
        let connection = pool.get()?;

        // Try to unlock a single connection.
        match Database::unlock(&connection, config) {
            // We're fine, the connection was returned successfully, we can return the pool.
            Ok(_) => Ok(pool),
            Err(_) => {
                let Some(passphrase) = &config.passphrase else {
                    // No passphrase was provided, and we failed to unlock a connection, return an
                    // error.
                    return Err(Error::DatabaseUnlockError("Invalid passphrase".to_owned()));
                };

                // Ok, let's see if the unlock of the connection failed because of new default
                // settings for the cipher settings, let's see if we can migrate the cipher settings.
                // Take a look at the documentation of cipher_migrate[1] for more info.
                //
                // [1]: https://www.zetetic.net/sqlcipher/sqlcipher-api/#cipher_migrate
                let connection = pool.get()?;
                connection.pragma_update(None, "key", &passphrase.as_str() as &dyn ToSql)?;

                let mut statement = connection.prepare("PRAGMA cipher_migrate")?;
                let result = statement.query_row([], |row| row.get::<usize, String>(0))?;

                // The cipher_migrate pragma returns a single row/column with the value set to `0`
                // if we succeeded.
                if result == "0" {
                    // In this case the migration was successful and we can now recreate the pool
                    // so the new settings come into play.
                    let manager = SqliteConnectionManager::file(db_path);
                    let pool = r2d2::Pool::new(manager)?;

                    Ok(pool)
                } else {
                    Err(Error::DatabaseUnlockError("Invalid passphrase".to_owned()))
                }
            }
        }
    }

    fn set_pragmas(connection: &rusqlite::Connection) -> Result<()> {
        connection.pragma_update(None, "foreign_keys", &1 as &dyn ToSql)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        Ok(())
    }

    /// Change the passphrase of the Seshat database.
    ///
    /// Note that this consumes the database object and any searcher objects
    ///   can't be used anymore. A new database will have to be opened and new
    ///   searcher objects as well.
    ///
    /// # Arguments
    ///
    /// * `path` - The directory where the database will be stored in. This
    ///   should be an empty directory if a new database should be created.
    /// * `new_passphrase` - The passphrase that should be used instead of the
    ///   current one.
    #[cfg(feature = "encryption")]
    pub fn change_passphrase(self, new_passphrase: &str) -> Result<()> {
        match &self.config.passphrase {
            Some(p) => {
                Index::change_passphrase(&self.path, p, new_passphrase)?;
                self.connection.lock().unwrap().pragma_update(
                    None,
                    "rekey",
                    &new_passphrase as &dyn ToSql,
                )?;
            }
            None => panic!("Database isn't encrypted"),
        }

        let receiver = self.shutdown();
        receiver.recv().unwrap()?;

        Ok(())
    }

    #[cfg(feature = "encryption")]
    fn unlock(connection: &rusqlite::Connection, config: &Config) -> Result<()> {
        let passphrase: &String = if let Some(ref p) = config.passphrase {
            p
        } else {
            return Ok(());
        };

        let mut statement = connection.prepare("PRAGMA cipher_version")?;
        let results = statement.query_map([], |row| row.get::<usize, String>(0))?;

        if results.count() != 1 {
            return Err(Error::SqlCipherError(
                "Sqlcipher support is missing".to_string(),
            ));
        }

        connection.pragma_update(None, "key", passphrase as &dyn ToSql)?;

        let count: std::result::Result<i64, rusqlite::Error> =
            connection.query_row("SELECT COUNT(*) FROM sqlite_master", [], |row| row.get(0));

        match count {
            Ok(_) => Ok(()),
            Err(_) => Err(Error::DatabaseUnlockError("Invalid passphrase".to_owned())),
        }
    }

    #[cfg(not(feature = "encryption"))]
    fn unlock(_: &rusqlite::Connection, _: &Config) -> Result<()> {
        Ok(())
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
        Ok(Index::new(path, config)?)
    }

    fn spawn_writer(
        connection: PooledConnection<SqliteConnectionManager>,
        index_writer: IndexWriter,
    ) -> WriterRet {
        let (tx, rx): (_, Receiver<ThreadMessage>) = channel();

        let t_handle = thread::spawn(move || {
            let mut writer = Writer::new(connection, index_writer);
            let mut loaded_unprocessed = false;

            while let Ok(message) = rx.recv() {
                match message {
                    ThreadMessage::Event((event, profile)) => writer.add_event(event, profile),
                    ThreadMessage::Write(sender, force_commit) => {
                        // We may have events that aren't deleted or committed
                        // to the index but are stored in the db, let us load
                        // them from the db and commit them to the index now.
                        // They will later be marked as committed in the
                        // database as part of a normal write.
                        if !loaded_unprocessed {
                            let ret = writer.load_unprocessed_events();

                            loaded_unprocessed = true;

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
                    ThreadMessage::Delete(sender, event_id) => {
                        let ret = writer.delete_event(event_id);
                        sender.send(ret).unwrap_or(());
                    }
                    ThreadMessage::ShutDown(sender) => {
                        let ret = writer.shutdown();
                        sender.send(ret).unwrap_or(());
                        return;
                    }
                };
            }
        });

        (t_handle, tx)
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

    /// Delete an event from the database.
    ///
    /// # Arguments
    /// * `event_id` - The event id of the event that will be deleted.
    ///
    /// Note for the event to be completely removed a commit needs to be done.
    ///
    /// Returns a receiver that will receive an boolean once the event has
    /// been deleted. The boolean indicates if the event was deleted or if a
    /// commit will be needed.
    pub fn delete_event(&self, event_id: &str) -> Receiver<Result<bool>> {
        let (sender, receiver): (_, Receiver<Result<bool>>) = channel();
        let message = ThreadMessage::Delete(sender, event_id.to_owned());
        self.tx.send(message).unwrap();
        receiver
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
    ///   fetching events from the room history. This checkpoint will be
    ///   persisted in the database.
    /// * `old_checkpoint` - The checkpoint that was used to fetch the given
    ///   events. This checkpoint will be removed from the database.
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

    /// Search the index and return events matching a search term.
    /// This is just a helper function that gets a searcher and performs a
    /// search on it immediately.
    /// # Arguments
    ///
    /// * `term` - The search term that should be used to search the index.
    pub fn search(&self, term: &str, config: &SearchConfig) -> Result<SearchBatch> {
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

    /// Get a database connection.
    /// Note that this connection should only be used for reading.
    pub fn get_connection(&self) -> Result<Connection> {
        let connection = self.pool.get()?;
        Database::unlock(&connection, &self.config)?;
        Database::set_pragmas(&connection)?;

        Ok(Connection {
            inner: connection,
            path: self.path.clone(),
        })
    }

    /// Shut the database down.
    ///
    /// This will terminate the writer thread making sure that no writes will
    /// happen after this operation.
    pub fn shutdown(self) -> Receiver<Result<()>> {
        let (sender, receiver): (_, Receiver<Result<()>>) = channel();
        let message = ThreadMessage::ShutDown(sender);
        self.tx.send(message).unwrap();
        receiver
    }

    /// Delete the database.
    /// Warning: This will delete the whole path that was provided at the
    /// database creation time.
    pub fn delete(self) -> Result<()> {
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

    let id = Database::save_profile(
        &db.connection.lock().unwrap(),
        "@alice.example.org",
        &profile,
    );
    assert_eq!(id.unwrap(), 1);

    let id = Database::save_profile(
        &db.connection.lock().unwrap(),
        "@alice.example.org",
        &profile,
    );
    assert_eq!(id.unwrap(), 1);

    let profile_new = Profile::new("Alice", "mxc://some_url");

    let id = Database::save_profile(
        &db.connection.lock().unwrap(),
        "@alice.example.org",
        &profile_new,
    );
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
    let id = Database::save_profile(
        &db.connection.lock().unwrap(),
        "@alice.example.org",
        &profile,
    );
    assert_eq!(id.unwrap(), 1);
}

#[test]
fn store_event() {
    let tmpdir = tempdir().unwrap();
    let db = Database::new(tmpdir.path()).unwrap();
    let profile = Profile::new("Alice", "");
    let id = Database::save_profile(
        &db.connection.lock().unwrap(),
        "@alice.example.org",
        &profile,
    )
    .unwrap();

    let mut event = EVENT.clone();
    let id = Database::save_event_helper(&db.connection.lock().unwrap(), &mut event, id).unwrap();
    assert_eq!(id, 1);
}

#[test]
fn store_event_and_profile() {
    let tmpdir = tempdir().unwrap();
    let db = Database::new(tmpdir.path()).unwrap();
    let mut profile = Profile::new("Alice", "");
    let mut event = EVENT.clone();
    Database::save_event(&db.connection.lock().unwrap(), &mut event, &mut profile).unwrap();
}

#[test]
fn load_event() {
    let tmpdir = tempdir().unwrap();
    let db = Database::new(tmpdir.path()).unwrap();
    let mut profile = Profile::new("Alice", "");

    let mut event = EVENT.clone();
    Database::save_event(&db.connection.lock().unwrap(), &mut event, &mut profile).unwrap();
    let events = Database::load_events(
        &db.connection.lock().unwrap(),
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
        &db.connection.lock().unwrap(),
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
    let profile_id =
        Database::save_profile(&db.connection.lock().unwrap(), user_id, &profile).unwrap();

    let loaded_profile =
        Database::load_profile(&db.connection.lock().unwrap(), profile_id).unwrap();

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
            Database::load_event_context(&db.connection.lock().unwrap(), &EVENT, 1, 1).unwrap();

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

    let first_id =
        Database::save_profile(&db.connection.lock().unwrap(), user_id, &profile).unwrap();
    let second_id =
        Database::save_profile(&db.connection.lock().unwrap(), user_id, &profile).unwrap();

    assert_eq!(first_id, second_id);

    let connection = db.connection.lock().unwrap();

    let mut stmt = connection
        .prepare("SELECT id FROM profile WHERE user_id=?1")
        .unwrap();

    let profile_ids = stmt.query_map([user_id], |row| row.get(0)).unwrap();

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

#[cfg(feature = "encryption")]
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

#[cfg(feature = "encryption")]
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
    assert!(
        Database::load_uncommitted_events(&db.connection.lock().unwrap())
            .unwrap()
            .is_empty()
    );

    db.add_event(EVENT.clone(), profile);
    db.commit().unwrap();
    db.reload().unwrap();

    // Now we do have uncommitted events.
    assert!(
        !Database::load_uncommitted_events(&db.connection.lock().unwrap())
            .unwrap()
            .is_empty()
    );

    // Since the event wasn't committed to the index the search should fail.
    assert!(db
        .search("test", &SearchConfig::new())
        .unwrap()
        .results
        .is_empty());

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
        thread::sleep(time::Duration::from_millis(100));
        db = Database::new(tmpdir.path())
    }

    let mut db = db.unwrap();

    // We still have uncommitted events.
    assert_eq!(
        Database::load_uncommitted_events(&db.connection.lock().unwrap()).unwrap()[0].1,
        *EVENT
    );

    db.force_commit().unwrap();
    db.reload().unwrap();

    // A forced commit gets rid of our uncommitted events.
    assert!(
        Database::load_uncommitted_events(&db.connection.lock().unwrap())
            .unwrap()
            .is_empty()
    );

    let result = db.search("test", &SearchConfig::new()).unwrap().results;

    // The search is now successful.
    assert!(!result.is_empty());
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].event_source, EVENT.source);
}

#[test]
fn delete_uncommitted() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(tmpdir.path()).unwrap();
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
    assert!(
        Database::load_uncommitted_events(&db.connection.lock().unwrap())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn stats_getting() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(tmpdir.path()).unwrap();
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

#[test]
fn database_upgrade_v1() {
    let mut path = PathBuf::from(file!());
    path.pop();
    path.pop();
    path.pop();
    path.push("data/database/v1");
    let db = Database::new(path);

    // Sadly the v1 database has invalid json in the source field, reindexing it
    // won't be possible. Let's check that it's marked for a reindex.
    match db {
        Ok(_) => panic!("Database doesn't need a reindex."),
        Err(e) => match e {
            Error::ReindexError => (),
            e => panic!("Database doesn't need a reindex: {}", e),
        },
    }
}

#[cfg(test)]
use crate::database::recovery::test::reindex_loop;

#[test]
fn database_upgrade_v1_2() {
    let mut path = PathBuf::from(file!());
    path.pop();
    path.pop();
    path.pop();
    path.push("data/database/v1_2");
    let db = Database::new(&path);
    match db {
        Ok(_) => panic!("Database doesn't need a reindex."),
        Err(e) => match e {
            Error::ReindexError => (),
            e => panic!("Database doesn't need a reindex: {}", e),
        },
    }

    let mut recovery_db = RecoveryDatabase::new(&path).expect("Can't open recovery db");

    recovery_db.delete_the_index().unwrap();
    recovery_db.open_index().unwrap();

    let events = recovery_db.load_events_deserialized(100, None).unwrap();

    recovery_db.index_events(&events).unwrap();
    reindex_loop(&mut recovery_db, events).unwrap();
    recovery_db.commit_and_close().unwrap();

    let db = Database::new(&path).expect("Can't open the db event after a reindex");

    let mut connection = db.get_connection().unwrap();
    let (version, _) = Database::get_version(&mut connection).unwrap();
    assert_eq!(version, DATABASE_VERSION);

    let result = db.search("Hello", &SearchConfig::new()).unwrap().results;
    assert!(!result.is_empty())
}

#[test]
fn delete_an_event() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(tmpdir.path()).unwrap();
    let profile = Profile::new("Alice", "");

    db.add_event(EVENT.clone(), profile.clone());
    db.add_event(TOPIC_EVENT.clone(), profile);

    db.force_commit().unwrap();

    assert!(
        Database::load_pending_deletion_events(&db.connection.lock().unwrap())
            .unwrap()
            .is_empty()
    );

    let recv = db.delete_event(&EVENT.event_id);
    recv.recv().unwrap().unwrap();

    assert_eq!(
        Database::load_pending_deletion_events(&db.connection.lock().unwrap())
            .unwrap()
            .len(),
        1
    );

    drop(db);

    let mut db = Database::new(tmpdir.path()).unwrap();
    assert_eq!(
        Database::load_pending_deletion_events(&db.connection.lock().unwrap())
            .unwrap()
            .len(),
        1
    );

    db.force_commit().unwrap();
    assert_eq!(
        Database::load_pending_deletion_events(&db.connection.lock().unwrap())
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn add_events_with_null_byte() {
    let event_source: &str = r#"{
        "content": {
            "body": "\u00000",
            "msgtype": "m.text"
        },
        "event_id": "$15163622448EBvZJ:localhost",
        "origin_server_ts": 1516362244050,
        "sender": "@example2:localhost",
        "type": "m.room.message",
        "unsigned": {"age": 43289803098},
        "user_id": "@example2:localhost",
        "age": 43289803098,
        "room_id": "!test:example.org"
    }"#;

    let event = RecoveryDatabase::event_from_json(event_source).unwrap();

    let tmpdir = tempdir().unwrap();
    let db = Database::new(tmpdir.path()).unwrap();
    let profile = Profile::new("Alice", &event.content_value);

    let events = vec![(event, profile)];
    db.add_historic_events(events, None, None)
        .recv()
        .unwrap()
        .expect("Event should be added");
}

#[test]
fn is_room_indexed() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(tmpdir.path()).unwrap();

    let connection = db.get_connection().unwrap();

    assert!(connection.is_empty().unwrap());
    assert!(!connection.is_room_indexed("!test_room:localhost").unwrap());

    let profile = Profile::new("Alice", "");
    db.add_event(EVENT.clone(), profile);
    db.force_commit().unwrap();

    assert!(connection.is_room_indexed("!test_room:localhost").unwrap());
    assert!(!connection.is_room_indexed("!test_room2:localhost").unwrap());
}

#[test]
fn user_version() {
    let tmpdir = tempdir().unwrap();
    let db = Database::new(tmpdir.path()).unwrap();
    let connection = db.get_connection().unwrap();

    assert_eq!(connection.get_user_version().unwrap(), 0);
    connection.set_user_version(10).unwrap();
    assert_eq!(connection.get_user_version().unwrap(), 10);
}

#[test]
#[cfg(feature = "encryption")]
fn sqlcipher_cipher_settings_update() {
    let mut path = PathBuf::from(file!());
    path.pop();
    path.pop();
    path.pop();
    path.push("data/database/sqlcipher-v3");

    let config = Config::new().set_passphrase("qR17RdpWurSh2pQRSc/EnsaO9V041kOwsZk0iSdUY/g");
    let _db =
        Database::new_with_config(&path, &config).expect("We should be able to open the database");
}
