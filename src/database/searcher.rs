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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;

use crate::config::SearchConfig;
use crate::error::Result;
use crate::events::{MxId, Profile, SerializedEvent};
use crate::index::IndexSearcher;
use crate::Database;

static BUSY_RETRY: usize = 10;
static BUSY_SLEEP: Duration = Duration::from_millis(10);

#[derive(Debug, PartialEq, Default, Clone, Serialize, Deserialize)]
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

/// The main entry point to the index and database.
pub struct Searcher {
    pub(crate) inner: IndexSearcher,
    pub(crate) database: Arc<Mutex<PooledConnection<SqliteConnectionManager>>>,
}

impl Searcher {
    /// Search the index and return events matching a search term.
    /// # Arguments
    ///
    /// * `term` - The search term that should be used to search the index.
    /// * `config` - A SearchConfig that will modify what the search result
    /// should contain.
    ///
    /// Returns a tuple of the count of matching documents and a list of
    /// `SearchResult`.
    pub fn search(&self, term: &str, config: &SearchConfig) -> Result<(usize, Vec<SearchResult>)> {
        let search_result = self.inner.search(term, config)?;

        if search_result.results.is_empty() {
            return Ok((0, vec![]));
        }

        let mut retry = 0;

        let events = loop {
            match Database::load_events(
                &*self.database.lock().unwrap(),
                &search_result.results,
                config.before_limit,
                config.after_limit,
                config.order_by_recency,
            ) {
                Ok(e) => break e,
                Err(e) => match e {
                    // Usually the busy timeout on a sqlite connection should
                    // handle this, but setting it on the connection didn't
                    // seem to get rid of database busy errors like expected.
                    rusqlite::Error::SqliteFailure(sql_error, _) => {
                        if sql_error.code == rusqlite::ffi::ErrorCode::DatabaseBusy
                            && retry < BUSY_RETRY
                        {
                            retry += 1;
                            sleep(BUSY_SLEEP);
                            continue;
                        } else {
                            return Err(e.into());
                        }
                    }
                    e => return Err(e.into()),
                },
            }
        };

        Ok((search_result.count, events))
    }
}
