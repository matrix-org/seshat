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

use std::sync::Arc;

use web_sys::console;

use crate::{
    error::Result,
    events::{CrawlerCheckpoint, Event, EventId, Profile},
    index::Writer as IndexWriter,
    Database,
};

use super::event_database::EventDatabase;

pub(crate) struct Writer {
    inner: IndexWriter,
    events_db: Arc<EventDatabase>,
    events: Vec<(Event, Profile)>,
    uncommitted_events: Vec<i64>,
    pending_deletion_events: Vec<EventId>,
}

impl Writer {
    pub fn new(events_db: Arc<EventDatabase>, index_writer: IndexWriter) -> Self {
        Writer {
            inner: index_writer,
            events_db,
            events: Vec::new(),
            uncommitted_events: Vec::new(),
            pending_deletion_events: Vec::new(),
        }
    }

    pub fn add_event(&mut self, event: Event, profile: Profile) {
        self.events.push((event, profile));
    }

    pub fn delete_event(&mut self, event_id: EventId) -> Result<bool> {
        Database::delete_event_helper(
            self.events_db.clone(),
            &mut self.inner,
            event_id,
            &mut self.pending_deletion_events,
        )
    }

    fn mark_events_as_deleted(&mut self) -> Result<()> {
        if self.pending_deletion_events.is_empty() {
            return Ok(());
        }
        return self
            .events_db
            .mark_events_as_deleted(self.pending_deletion_events.clone());
    }

    pub fn write_queued_events(&mut self, force_commit: bool) -> Result<()> {
        let (_, committed) = Database::write_events(
            self.events_db.clone(),
            &mut self.inner,
            (None, None, &mut self.events),
            force_commit,
            &mut self.uncommitted_events,
        )?;

        if committed {
            self.mark_events_as_deleted()?;
        }

        Ok(())
    }

    pub fn write_historic_events(
        &mut self,
        checkpoint: Option<CrawlerCheckpoint>,
        old_checkpoint: Option<CrawlerCheckpoint>,
        mut events: Vec<(Event, Profile)>,
        force_commit: bool,
    ) -> Result<bool> {
        let empty_events = events.is_empty();
        let (ret, committed) = Database::write_events(
            self.events_db.clone(),
            &mut self.inner,
            (checkpoint, old_checkpoint, &mut events),
            force_commit,
            &mut self.uncommitted_events,
        )?;

        if committed {
            self.mark_events_as_deleted()?;
        }

        if empty_events {
            Ok(false)
        } else {
            Ok(ret)
        }
    }

    pub fn load_unprocessed_events(&mut self) -> Result<()> {
        console::log_1(&"!in load_unprocessed_events".into());

        let mut ret = self.events_db.load_uncommitted_events()?;
        console::log_1(&"!in load_unprocessed_events ret".into());
        for (id, event) in ret.drain(..) {
            self.uncommitted_events.push(id);
            self.inner.add_event(&event);
        }

        let ret = self.events_db.load_pending_deletion_events()?;

        for event_id in &ret {
            self.inner.delete_event(event_id);
        }

        self.pending_deletion_events.extend(ret);

        Ok(())
    }

    pub fn shutdown(self) -> Result<()> {
        self.inner.wait_merging_threads()?;
        Ok(())
    }
}
