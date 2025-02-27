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
mod event_database;
mod writer;

use fs_extra::dir;
use rusqlite::Connection;
// use r2d2::{Pool, PooledConnection};
// use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::trace::{config_log, TraceEvent, TraceEventCodes};
use rusqlite::ToSql;
use event_database::EventDatabase;
// use sqlite_wasm_rs::export::{self as ffi, install_opfs_sahpool, OpfsSAHPoolCfgBuilder};
use std::ffi::c_int;
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
use web_sys::console;

pub use crate::database::{
    // connection::{Connection, DatabaseStats},
    // recovery::{RecoveryDatabase, RecoveryInfo},
    searcher::{SearchBatch, SearchResult, Searcher},
};
use crate::{
    config::{Config, SearchConfig},
    database::writer::Writer,
    error::{Error, Result},
    events::{CrawlerCheckpoint, Event, EventId, HistoricEventsT, Profile},
    index::{Index, Writer as IndexWriter},
};

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
    event_db: Arc<EventDatabase>,
    _write_thread: Receiver<()>,
    tx: Sender<ThreadMessage>,
    index: Index,
    config: Config,
}

type WriterRet = (Receiver<()>, Sender<ThreadMessage>);

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
    ///
    pub fn new_with_config<P: AsRef<Path>>(path: P, config: &Config) -> Result<Database>
    where
        PathBuf: std::convert::From<P>,
    {
        console::log_1(&"setting rusqlite_log".into());
        let db_path = path.as_ref().to_path_buf();
        let events_db = Arc::new(EventDatabase::new(db_path)?);
        let index = Database::create_index(&path, config)?;
        let writer = index.get_writer()?;

        let (t_handle, tx) = Database::spawn_writer(events_db.clone(), writer);

        Ok(Database {
            path: path.into(),
            event_db: events_db,
            _write_thread: t_handle,
            tx,
            index,
            config: config.clone(),
        })
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
                // Index::change_passphrase(&self.path, p, new_passphrase)?;
                self.event_db.change_passphrase(new_passphrase);
            }
            None => panic!("Database isn't encrypted"),
        }

        let receiver = self.shutdown();
        receiver.recv().unwrap()?;

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

    fn spawn_writer(events_db: Arc<EventDatabase>, index_writer: IndexWriter) -> WriterRet {
        console::log_1(&"spawn_writer".into());
        let (tx, rx): (_, Receiver<ThreadMessage>) = channel();

        let (shutdownSender, shutdownReceiver): (_, Receiver<()>) = channel();
        // let t_handle = thread::spawn(move || {
        rayon::spawn(move || {
            let mut writer = Writer::new(events_db.clone(), index_writer);
            let mut loaded_unprocessed = false;

            while let Ok(message) = rx.recv() {
                match message {
                    ThreadMessage::Event((event, profile)) => writer.add_event(event, profile),
                    ThreadMessage::Write(sender, force_commit) => {
                        console::log_1(&"spawn_writer Write".into());
                        // We may have events that aren't deleted or committed
                        // to the index but are stored in the db, let us load
                        // them from the db and commit them to the index now.
                        // They will later be marked as committed in the
                        // database as part of a normal write.
                        if !loaded_unprocessed {
                            console::log_1(&"!loaded_unprocessed".into());
                            let ret = writer.load_unprocessed_events();
                            console::log_1(&"!loaded_unprocessed ret".into());
                            loaded_unprocessed = true;

                            if ret.is_err() {
                                console::log_1(&"!loaded_unprocessed is_err".into());
                                sender.send(ret).unwrap_or(());
                                continue;
                            }
                        }
                        console::log_1(&"!write_queued_events".into());
                        let ret = writer.write_queued_events(force_commit);
                        // Notify that we are done with the write.
                        console::log_1(&"!send ret".into());
                        sender.send(ret).unwrap_or(());
                    }
                    ThreadMessage::HistoricEvents(m) => {
                        console::log_1(&"spawn_writer HistoricEvents".into());
                        let (check, old_check, events, sender) = m;
                        let ret = writer.write_historic_events(check, old_check, events, true);
                        sender.send(ret).unwrap_or(());
                    }
                    ThreadMessage::Delete(sender, event_id) => {
                        console::log_1(&"spawn_writer Delete".into());
                        let ret = writer.delete_event(event_id);
                        sender.send(ret).unwrap_or(());
                    }
                    ThreadMessage::ShutDown(sender) => {
                        console::log_1(&"spawn_writer ShutDown".into());
                        let ret = writer.shutdown();
                        sender.send(ret).unwrap_or(());
                        let _ = shutdownSender.send(());
                        return;
                    }
                };
            }
        });

        (shutdownReceiver, tx)
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
            event_db: self.event_db.clone(),
        }
    }

    /// Get a database connection.
    /// Note that this connection should only be used for reading.
    // pub fn get_connection(&self) -> Connection {
    //     // let connection = self.pool.get()?;

    //     let connection: Connection = Connection::open(&self.path).unwrap();
    //     // let mut connection = self.connection.lock().unwrap().;

    //     // Database::unlock(&connection, &self.config);
    //     Database::set_pragmas(&connection);

    //     connection
    //     // Ok(Connection {
    //     //     inner: connection,
    //     //     path: self.path.clone(),
    //     // })
    // }

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

impl Database {
    /// Write the events to the database.
    /// Returns a tuple containing a boolean and an array if integers. The
    /// boolean notifies us if all the events were already added to the
    /// database, the integers are the database ids of our events.
    pub(crate) fn write_events_helper(
        event_db: Arc<EventDatabase>,
        index_writer: &mut IndexWriter,
        events: &mut Vec<(Event, Profile)>,
    ) -> Result<(bool, Vec<i64>)> {
        let mut ret = Vec::new();
        let mut event_ids = Vec::new();

        for (e, p) in events.drain(..) {
            let event_id = event_db.save_event(e.clone(), p)?;
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

    pub(crate) fn delete_event_helper(
        event_db: Arc<EventDatabase>,
        index_writer: &mut IndexWriter,
        event_id: EventId,
        pending_deletion_events: &mut Vec<EventId>,
    ) -> Result<bool> {
        event_db.pending_delete(event_id.clone())?;

        index_writer.delete_event(&event_id);
        pending_deletion_events.push(event_id);

        let committed = index_writer.commit()?;

        if committed {
            event_db.mark_events_as_deleted(pending_deletion_events.to_vec())?;
            pending_deletion_events.clear();
        }

        Ok(committed)
    }

    pub(crate) fn write_events(
        event_db: Arc<EventDatabase>,
        index_writer: &mut IndexWriter,
        message: (
            Option<CrawlerCheckpoint>,
            Option<CrawlerCheckpoint>,
            &mut Vec<(Event, Profile)>,
        ),
        force_commit: bool,
        uncommitted_events: &mut Vec<i64>,
    ) -> Result<(bool, bool)> {
        let (new_checkpoint, old_checkpoint, events) = message;
        let mut write_events_result: Option<(bool, Vec<i64>)> = None;
        let write_events = Box::new(|| {
            write_events_result = Some(
                Database::write_events_helper(event_db.clone(), index_writer, events).unwrap(),
            );
        });
        event_db.replace_crawler(new_checkpoint, old_checkpoint, Box::new(write_events))?;

        let (ret, event_ids) = write_events_result.unwrap();

        uncommitted_events.extend(event_ids);

        let committed = if force_commit {
            index_writer.force_commit()?;
            true
        } else {
            index_writer.commit()?
        };

        if committed {
            event_db.mark_events_as_indexed(uncommitted_events.to_vec())?;
            uncommitted_events.clear();
        }

        Ok((ret, committed))
    }
}
