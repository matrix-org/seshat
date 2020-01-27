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

use r2d2_sqlite::SqliteConnectionManager;

use crate::error::Result;
use crate::events::{CrawlerCheckpoint, Event, Profile};
use crate::index::Writer as IndexWriter;
use crate::Database;

pub(crate) struct Writer {
    inner: IndexWriter,
    connection: r2d2::PooledConnection<SqliteConnectionManager>,
    events: Vec<(Event, Profile)>,
    uncommitted_events: Vec<i64>,
}

impl Writer {
    pub fn new(
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

    pub fn add_event(&mut self, event: Event, profile: Profile) {
        self.events.push((event, profile));
    }

    pub fn write_queued_events(&mut self, force_commit: bool) -> Result<()> {
        Database::write_events(
            &mut self.connection,
            &mut self.inner,
            (None, None, &mut self.events),
            force_commit,
            &mut self.uncommitted_events,
        )?;

        Ok(())
    }

    pub fn write_historic_events(
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

    pub fn load_uncommitted_events(&mut self) -> Result<()> {
        let mut ret = Database::load_uncommitted_events(&self.connection)?;

        for (id, event) in ret.drain(..) {
            self.uncommitted_events.push(id);
            self.inner.add_event(&event);
        }

        Ok(())
    }
}
