use std::path::{Path, PathBuf};
use std::fs;

use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{ToSql};

use crate::events::{Profile, SerializedEvent};
use crate::error::{Error, Result};
use crate::config::{Config, LoadConfig};
use crate::Database;
use crate::database::{DATABASE_VERSION, EVENTS_DB_NAME};

pub struct RecoveryDatabase {
    path: PathBuf,
    connection: PooledConnection<SqliteConnectionManager>,
    pool: r2d2::Pool<SqliteConnectionManager>,
    config: Config,
    index_deleted: bool,
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

        Ok(Self {
            path: path.into(),
            connection,
            pool,
            config: config.clone(),
            index_deleted: false,
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

    pub fn load_file_events(
        &self,
        load_config: &LoadConfig,
    ) -> Result<Vec<(SerializedEvent, Profile)>> {
        Ok(Database::load_file_events(
            &self.connection,
            &load_config.room_id,
            load_config.limit,
            load_config.from_event.as_ref().map(|x| &**x),
            &load_config.direction,
        )?)
    }
}
