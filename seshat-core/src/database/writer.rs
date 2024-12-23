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

// use r2d2_sqlite::SqliteConnectionManager;

use diesel_wasm_sqlite::connection::WasmSqliteConnection;
use web_sys::console;

use crate::{
    error::Result,
    events::{CrawlerCheckpoint, Event, EventId, Profile},
    index::Writer as IndexWriter,
    Database,
};

pub(crate) struct Writer {
    inner: IndexWriter,
    events: Vec<(Event, Profile)>,
    uncommitted_events: Vec<i64>,
    pending_deletion_events: Vec<EventId>,
}

impl Writer {
    pub fn new(index_writer: IndexWriter) -> Self {
        Writer {
            inner: index_writer,
            events: Vec::new(),
            uncommitted_events: Vec::new(),
            pending_deletion_events: Vec::new(),
        }
    }

    pub fn add_event(&mut self, event: Event, profile: Profile) {
        self.events.push((event, profile));
    }

    pub fn delete_event(
        &mut self,
        conn: &mut WasmSqliteConnection,
        event_id: EventId,
    ) -> Result<bool> {
        Database::delete_event_helper(
            conn,
            &mut self.inner,
            event_id,
            &mut self.pending_deletion_events,
        )
    }

    fn mark_events_as_deleted(&mut self, conn: &mut WasmSqliteConnection) -> Result<()> {
        if self.pending_deletion_events.is_empty() {
            return Ok(());
        }
        Database::mark_events_as_deleted(conn, &mut self.pending_deletion_events)
    }

    pub fn write_queued_events(
        &mut self,
        conn: &mut WasmSqliteConnection,
        force_commit: bool,
    ) -> Result<()> {
        let (_, committed) = Database::write_events(
            conn,
            &mut self.inner,
            (None, None, &mut self.events),
            force_commit,
            &mut self.uncommitted_events,
        )?;
        console::log_1(&"write_events".into());
        if committed {
            self.mark_events_as_deleted(conn)?;
        }
        console::log_1(&"mark_events_as_deleted".into());
        Ok(())
    }

    pub fn write_historic_events(
        &mut self,
        conn: &mut WasmSqliteConnection,
        checkpoint: Option<CrawlerCheckpoint>,
        old_checkpoint: Option<CrawlerCheckpoint>,
        mut events: Vec<(Event, Profile)>,
        force_commit: bool,
    ) -> Result<bool> {
        let empty_events = events.is_empty();
        let (ret, committed) = Database::write_events(
            conn,
            &mut self.inner,
            (checkpoint, old_checkpoint, &mut events),
            force_commit,
            &mut self.uncommitted_events,
        )?;

        if committed {
            self.mark_events_as_deleted(conn)?;
        }

        if empty_events {
            Ok(false)
        } else {
            Ok(ret)
        }
    }

    pub fn load_unprocessed_events(&mut self) -> Result<()> {
        let mut ret = Database::load_uncommitted_events();

        for (id, event) in ret.drain(..) {
            self.uncommitted_events.push(id);
            self.inner.add_event(&event);
        }

        let ret = Database::load_pending_deletion_events();

        for event_id in &ret {
            self.inner.delete_event(event_id);
        }

        self.pending_deletion_events.extend(ret);

        Ok(())
    }

    pub async fn wait_merging_threads(self) -> Result<()> {
        console::log_1(&"wait_merging_threads writer".into());
        self.inner.wait_merging_threads().await?;
        console::log_1(&"wait_merging_threads exit".into());
        Ok(())
    }

    pub async fn shutdown(self) -> Result<()> {
        self.inner.wait_merging_threads().await?;
        Ok(())
    }
}
