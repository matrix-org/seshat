use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::ToSql;

use crate::config::Config;
use crate::database::{DATABASE_VERSION, EVENTS_DB_NAME};
use crate::error::{Error, Result};
use crate::events::{Event, SerializedEvent};
use crate::index::{Index, Writer};
use crate::Database;

pub struct RecoveryDatabase {
    path: PathBuf,
    connection: PooledConnection<SqliteConnectionManager>,
    #[used]
    pool: r2d2::Pool<SqliteConnectionManager>,
    config: Config,
    recovery_info: RecoveryInfo,
    index_deleted: bool,
    index: Option<Index>,
    index_writer: Option<Writer>,
}

#[derive(Debug, Clone)]
pub struct RecoveryInfo {
    total_event_count: u64,
    reindexed_events: Arc<AtomicU64>,
}

impl RecoveryInfo {
    pub fn total_events(&self) -> u64 {
        self.total_event_count
    }

    pub fn reindexed_events(&self) -> &AtomicU64 {
        &self.reindexed_events
    }
}

impl RecoveryDatabase {
    /// Open a read-only Seshat database.
    ///
    /// # Arguments
    ///
    /// * `path` - The directory where the database will be stored in. This
    /// should be an empty directory if a new database should be created.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self>
    where
        PathBuf: std::convert::From<P>,
    {
        Self::new_with_config(path, &Config::new())
    }

    /// Open a recovery Seshat database with the provided config.
    ///
    /// # Arguments
    ///
    /// * `path` - The directory where the database will be stored in. This
    /// should be an empty directory if a new database should be created.
    ///
    /// * `config` - Configuration that changes the behaviour of the database.
    pub fn new_with_config<P: AsRef<Path>>(path: P, config: &Config) -> Result<Self>
    where
        PathBuf: std::convert::From<P>,
    {
        let db_path = path.as_ref().join(EVENTS_DB_NAME);
        let manager = SqliteConnectionManager::file(&db_path);
        let pool = r2d2::Pool::new(manager)?;

        let mut connection = pool.get()?;
        connection.pragma_update(None, "foreign_keys", &1 as &dyn ToSql)?;

        Database::unlock(&connection, config)?;

        let (version, _) = match Database::get_version(&mut connection) {
            Ok(ret) => ret,
            Err(e) => return Err(Error::DatabaseOpenError(e.to_string())),
        };

        Database::create_tables(&connection)?;

        if version != DATABASE_VERSION {
            return Err(Error::DatabaseVersionError);
        }

        let event_count = Database::get_event_count(&connection)?;

        let info = RecoveryInfo {
            total_event_count: event_count as u64,
            reindexed_events: Arc::new(AtomicU64::new(0)),
        };

        Ok(Self {
            path: path.into(),
            connection,
            pool,
            config: config.clone(),
            recovery_info: info,
            index_deleted: false,
            index: None,
            index_writer: None,
        })
    }

    /// Delete the Seshat index, leaving only the events database.
    ///
    /// After this operation is done, the index can be rebuilt.
    pub fn delete_the_index(&mut self) -> std::io::Result<()> {
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            let path = entry.path();

            // Skip removing directories, we don't create subdirs in our
            // database dir.
            if path.is_dir() {
                continue;
            }

            if let Some(file_name) = path.file_name() {
                // Skip removing the events database, those will be needed for
                // reindexing.
                if file_name == EVENTS_DB_NAME {
                    continue;
                }

                fs::remove_file(path)?
            }
        }
        self.index_deleted = true;
        Ok(())
    }

    pub fn load_events(
        &self,
        limit: usize,
        from_event: Option<&Event>,
    ) -> Result<Vec<SerializedEvent>> {
        Ok(Database::load_all_events(
            &self.connection,
            limit,
            from_event,
        )?)
    }

    pub fn open_index(&mut self) -> Result<()> {
        let index = Index::new(&self.path, &self.config)?;
        let writer = index.get_writer()?;
        self.index = Some(index);
        self.index_writer = Some(writer);

        Ok(())
    }

    pub fn info(&self) -> &RecoveryInfo {
        &self.recovery_info
    }

    pub fn index_events(&mut self, events: &Vec<Event>) -> Result<()> {
        match self.index_writer.as_mut() {
            Some(writer) => events.iter().map(|e| writer.add_event(e)).collect(),
            None => panic!("Index wasn't deleted"),
        }

        self.recovery_info
            .reindexed_events
            .fetch_add(events.len() as u64, Ordering::SeqCst);

        Ok(())
    }

    /// Commit the recovery database and mark is as reindexed.
    pub fn commit(mut self) -> Result<()> {
        self.index_writer.as_mut().unwrap().force_commit()?;
        self.connection
            .execute("UPDATE reindex_needed SET reindex_needed = ?1", &[false])?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{Database, Error, Event, EventType, RecoveryDatabase};
    use serde_json::Value;
    use std::path::PathBuf;
    use std::sync::atomic::Ordering;

    fn event_from_json(event_source: &str) -> Event {
        let object: Value =
            serde_json::from_str(event_source).expect("Can't deserialize event source");
        let content = &object["content"];
        let event_type = &object["type"];

        let event_type = match event_type.as_str().unwrap() {
            "m.room.message" => EventType::Message,
            "m.room.name" => EventType::Name,
            "m.room.topic" => EventType::Topic,
            _ => panic!("Invalid event type"),
        };

        let (content_value, msgtype) = match event_type {
            EventType::Message => (content["body"].as_str().unwrap(), Some("m.text")),
            EventType::Topic => (content["topic"].as_str().unwrap(), None),
            EventType::Name => (content["name"].as_str().unwrap(), None),
        };

        Event::new(
            event_type,
            content_value,
            msgtype,
            object["event_id"].as_str().unwrap(),
            object["sender"].as_str().unwrap(),
            object["origin_server_ts"].as_u64().unwrap() as i64,
            object["room_id"].as_str().unwrap(),
            &event_source,
        )
    }

    #[test]
    fn test_recovery() {
        let mut path = PathBuf::from(file!());
        path.pop();
        path.pop();
        path.pop();
        path.push("data/database/v2");
        let db = Database::new(&path);

        match db {
            Ok(_) => panic!("Database doesn't need a reindex."),
            Err(e) => match e {
                Error::ReindexError => (),
                e => panic!("Database doesn't need a reindex: {}", e),
            },
        }

        let mut recovery_db = RecoveryDatabase::new(path).expect("Can't open recovery db");
        assert_ne!(recovery_db.info().total_events(), 0);
        recovery_db
            .delete_the_index()
            .expect("Can't delete the index");
        recovery_db
            .open_index()
            .expect("Can't open the new the index");

        let events = recovery_db
            .load_events(10, None)
            .expect("Can't load events");

        assert!(!events.is_empty());
        assert_eq!(events.len(), 10);

        let mut events: Vec<Event> = events.iter().map(|e| event_from_json(e)).collect();

        recovery_db.index_events(&events).unwrap();
        assert_eq!(
            recovery_db
                .info()
                .reindexed_events()
                .load(Ordering::Relaxed),
            10
        );

        loop {
            let serialized_events = recovery_db
                .load_events(10, events.last())
                .expect("Can't load events");

            if serialized_events.is_empty() {
                break;
            }

            events = serialized_events
                .iter()
                .map(|e| event_from_json(e))
                .collect();
            recovery_db.index_events(&events).unwrap();
        }

        assert_eq!(
            recovery_db
                .info()
                .reindexed_events()
                .load(Ordering::Relaxed),
            999
        );

        recovery_db.commit().unwrap();
    }
}
