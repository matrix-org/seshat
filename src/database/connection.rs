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

/// A Seshat database connection.
/// The connection can be used to read data out of the database using a
/// separate thread.
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;

use fs_extra::dir;
use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;

use crate::{
    config::LoadConfig,
    error::Result,
    events::{CrawlerCheckpoint, Profile, SerializedEvent},
    Database,
};

/// Statistical information about the database.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseStats {
    /// The number number of bytes the database is using on disk.
    pub size: u64,
    /// The number of events that the database knows about.
    pub event_count: u64,
    /// The number of rooms that the database knows about.
    pub room_count: u64,
}

/// A Seshat database connection that can be used for reading.
pub struct Connection {
    pub(crate) inner: PooledConnection<SqliteConnectionManager>,
    pub(crate) path: PathBuf,
}

impl Connection {
    /// Load all the previously stored crawler checkpoints from the database.
    /// # Arguments
    pub fn load_checkpoints(&self) -> Result<Vec<CrawlerCheckpoint>> {
        let mut stmt = self.prepare(
            "SELECT room_id, token, full_crawl, direction
                                    FROM crawlercheckpoints",
        )?;

        let rows = stmt.query_map([], |row| {
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
        let checkpoint_count: i64 =
            self.query_row("SELECT COUNT(*) FROM crawlercheckpoints", [], |row| {
                row.get(0)
            })?;

        Ok(event_count == 0 && checkpoint_count == 0)
    }

    /// Is a room already indexed.
    ///
    /// Returns true if the database contains events from a room, false
    /// otherwise.
    pub fn is_room_indexed(&self, room_id: &str) -> Result<bool> {
        let event_count: i64 = Database::get_event_count_for_room(&self.inner, room_id)?;
        let checkpoint_count: i64 = self.query_row(
            "SELECT COUNT(*) FROM crawlercheckpoints WHERE room_id=?1",
            [room_id],
            |row| row.get(0),
        )?;

        Ok(event_count != 0 || checkpoint_count != 0)
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
    ///   them should be loaded.
    ///
    /// # Examples
    ///
    /// ```noexecute
    /// let config = LoadConfig::new("!testroom:localhost").limit(10);
    /// let result = connection.load_file_events(&config);
    /// ```
    ///
    /// Returns a list of tuples containing the serialized events and the
    ///   profile of the sender at the time when the event was sent.
    pub fn load_file_events(
        &self,
        load_config: &LoadConfig,
    ) -> Result<Vec<(SerializedEvent, Profile)>> {
        Ok(Database::load_file_events(
            self,
            &load_config.room_id,
            load_config.limit,
            load_config.from_event.as_deref(),
            &load_config.direction,
        )?)
    }

    /// Get the user version stored in the database.
    ///
    /// This version isn't used anywhere internally and can be set by the user
    /// to signal changes between the JSON that gets stored inside of Seshat.
    pub fn get_user_version(&self) -> Result<i64> {
        Database::get_user_version(self)
    }

    /// Set the user version to the given version.
    ///
    /// # Arguments
    ///
    /// * `version` - The new version that will be stored in the database.
    pub fn set_user_version(&self, version: i64) -> Result<()> {
        Database::set_user_version(self, version)
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
