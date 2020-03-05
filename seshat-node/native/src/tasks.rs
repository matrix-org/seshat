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
use std::path::PathBuf;
use std::sync::Mutex;

use crate::utils::*;
use neon::prelude::*;
use seshat::{
    CheckpointDirection, Connection, CrawlerCheckpoint, DatabaseStats, Event, LoadConfig, Profile,
    Receiver, RecoveryDatabase, SearchConfig, SearchResult, Searcher,
};

pub(crate) struct CommitTask {
    pub(crate) receiver: Receiver<seshat::Result<()>>,
}

impl Task for CommitTask {
    type Output = ();
    type Error = seshat::Error;
    type JsEvent = JsUndefined;

    fn perform(&self) -> Result<Self::Output, Self::Error> {
        self.receiver.recv().unwrap()
    }

    fn complete(
        self,
        mut cx: TaskContext,
        result: Result<Self::Output, Self::Error>,
    ) -> JsResult<Self::JsEvent> {
        match result {
            Ok(_) => Ok(cx.undefined()),
            Err(e) => cx.throw_error(format!("Error writing to database: {}", e.to_string())),
        }
    }
}

pub(crate) struct SearchTask {
    pub(crate) inner: Searcher,
    pub(crate) term: String,
    pub(crate) config: SearchConfig,
}

impl Task for SearchTask {
    type Output = Vec<SearchResult>;
    type Error = seshat::Error;
    type JsEvent = JsObject;

    fn perform(&self) -> Result<Self::Output, Self::Error> {
        self.inner.search(&self.term, &self.config)
    }

    fn complete(
        self,
        mut cx: TaskContext,
        result: Result<Self::Output, Self::Error>,
    ) -> JsResult<Self::JsEvent> {
        let mut ret = match result {
            Ok(r) => r,
            Err(e) => return cx.throw_type_error(e.to_string()),
        };

        let count = ret.len();
        let results = JsArray::new(&mut cx, count as u32);
        let count = JsNumber::new(&mut cx, count as f64);

        for (i, element) in ret.drain(..).enumerate() {
            let object = search_result_to_js(&mut cx, element)?;
            results.set(&mut cx, i as u32, object)?;
        }

        let search_result = JsObject::new(&mut cx);
        let highlights = JsArray::new(&mut cx, 0);

        search_result.set(&mut cx, "count", count)?;
        search_result.set(&mut cx, "results", results)?;
        search_result.set(&mut cx, "highlights", highlights)?;

        Ok(search_result)
    }
}

pub(crate) struct AddBacklogTask {
    pub(crate) receiver: Receiver<seshat::Result<bool>>,
}

impl Task for AddBacklogTask {
    type Output = bool;
    type Error = seshat::Error;
    type JsEvent = JsBoolean;

    fn perform(&self) -> Result<Self::Output, Self::Error> {
        self.receiver.recv().unwrap()
    }

    fn complete(
        self,
        mut cx: TaskContext,
        result: Result<Self::Output, Self::Error>,
    ) -> JsResult<Self::JsEvent> {
        match result {
            Ok(r) => Ok(JsBoolean::new(&mut cx, r)),
            Err(e) => cx.throw_type_error(e.to_string()),
        }
    }
}

pub(crate) struct LoadCheckPointsTask {
    pub(crate) connection: Connection,
}

impl Task for LoadCheckPointsTask {
    type Output = Vec<CrawlerCheckpoint>;
    type Error = seshat::Error;
    type JsEvent = JsArray;

    fn perform(&self) -> Result<Self::Output, Self::Error> {
        self.connection.load_checkpoints()
    }

    fn complete(
        self,
        mut cx: TaskContext,
        result: Result<Self::Output, Self::Error>,
    ) -> JsResult<Self::JsEvent> {
        let mut checkpoints = match result {
            Ok(c) => c,
            Err(e) => return cx.throw_type_error(e.to_string()),
        };
        let count = checkpoints.len();
        let ret = JsArray::new(&mut cx, count as u32);

        for (i, c) in checkpoints.drain(..).enumerate() {
            let js_checkpoint = JsObject::new(&mut cx);

            let room_id = JsString::new(&mut cx, c.room_id);
            let token = JsString::new(&mut cx, c.token);
            let full_crawl = JsBoolean::new(&mut cx, c.full_crawl);
            let direction = match c.direction {
                CheckpointDirection::Backwards => JsString::new(&mut cx, "b"),
                CheckpointDirection::Forwards => JsString::new(&mut cx, "f"),
            };

            js_checkpoint.set(&mut cx, "roomId", room_id)?;
            js_checkpoint.set(&mut cx, "token", token)?;
            js_checkpoint.set(&mut cx, "fullCrawl", full_crawl)?;
            js_checkpoint.set(&mut cx, "direction", direction)?;

            ret.set(&mut cx, i as u32, js_checkpoint)?;
        }

        Ok(ret)
    }
}

pub(crate) struct IsEmptyTask {
    pub(crate) connection: Connection,
}

impl Task for IsEmptyTask {
    type Output = bool;
    type Error = seshat::Error;
    type JsEvent = JsBoolean;

    fn perform(&self) -> Result<Self::Output, Self::Error> {
        self.connection.is_empty()
    }

    fn complete(
        self,
        mut cx: TaskContext,
        result: Result<Self::Output, Self::Error>,
    ) -> JsResult<Self::JsEvent> {
        match result {
            Ok(r) => Ok(JsBoolean::new(&mut cx, r)),
            Err(e) => cx.throw_type_error(e.to_string()),
        }
    }
}

pub(crate) struct StatsTask {
    pub(crate) connection: Connection,
}

impl Task for StatsTask {
    type Output = DatabaseStats;
    type Error = seshat::Error;
    type JsEvent = JsObject;

    fn perform(&self) -> Result<Self::Output, Self::Error> {
        self.connection.get_stats()
    }

    fn complete(
        self,
        mut cx: TaskContext,
        result: Result<Self::Output, Self::Error>,
    ) -> JsResult<Self::JsEvent> {
        match result {
            Ok(r) => {
                let result = JsObject::new(&mut cx);
                let event_count = JsNumber::new(&mut cx, r.event_count as f64);
                let room_count = JsNumber::new(&mut cx, r.room_count as f64);
                let size = JsNumber::new(&mut cx, r.size as f64);
                result.set(&mut cx, "eventCount", event_count)?;
                result.set(&mut cx, "roomCount", room_count)?;
                result.set(&mut cx, "size", size)?;
                Ok(result)
            }
            Err(e) => cx.throw_type_error(e.to_string()),
        }
    }
}

pub(crate) struct GetSizeTask {
    pub(crate) path: PathBuf,
}

impl Task for GetSizeTask {
    type Output = u64;
    type Error = fs_extra::error::Error;
    type JsEvent = JsNumber;

    fn perform(&self) -> Result<Self::Output, Self::Error> {
        dir::get_size(&self.path)
    }

    fn complete(
        self,
        mut cx: TaskContext,
        result: Result<Self::Output, Self::Error>,
    ) -> JsResult<Self::JsEvent> {
        match result {
            Ok(r) => Ok(JsNumber::new(&mut cx, r as f64)),
            Err(e) => cx.throw_type_error(e.to_string()),
        }
    }
}

pub(crate) struct DeleteTask {
    pub(crate) db_path: PathBuf,
}

impl Task for DeleteTask {
    type Output = ();
    type Error = std::io::Error;
    type JsEvent = JsUndefined;

    fn perform(&self) -> Result<Self::Output, Self::Error> {
        std::fs::remove_dir_all(self.db_path.clone())
    }

    fn complete(
        self,
        mut cx: TaskContext,
        result: Result<Self::Output, Self::Error>,
    ) -> JsResult<Self::JsEvent> {
        match result {
            Ok(_) => Ok(cx.undefined()),
            Err(e) => cx.throw_type_error(e.to_string()),
        }
    }
}

pub(crate) struct LoadFileEventsTask {
    pub(crate) inner: Connection,
    pub(crate) config: LoadConfig,
}

impl Task for LoadFileEventsTask {
    type Output = Vec<(String, Profile)>;
    type Error = seshat::Error;
    type JsEvent = JsArray;

    fn perform(&self) -> Result<Self::Output, Self::Error> {
        self.inner.load_file_events(&self.config)
    }

    fn complete(
        self,
        mut cx: TaskContext,
        result: Result<Self::Output, Self::Error>,
    ) -> JsResult<Self::JsEvent> {
        let mut ret = match result {
            Ok(r) => r,
            Err(e) => return cx.throw_type_error(e.to_string()),
        };

        let results = JsArray::new(&mut cx, ret.len() as u32);

        for (i, (source, profile)) in ret.drain(..).enumerate() {
            let result = JsObject::new(&mut cx);

            let event = deserialize_event(&mut cx, &source)?;
            let profile = profile_to_js(&mut cx, profile)?;
            result.set(&mut cx, "event", event)?;
            result.set(&mut cx, "profile", profile)?;

            results.set(&mut cx, i as u32, result)?;
        }

        Ok(results)
    }
}

pub(crate) struct ReindexTask {
    pub(crate) inner: Mutex<Option<RecoveryDatabase>>,
}

impl Task for ReindexTask {
    type Output = ();
    type Error = seshat::Error;
    type JsEvent = JsUndefined;

    fn perform(&self) -> Result<Self::Output, Self::Error> {
        let mut db = self.inner.lock().unwrap().take().unwrap();
        db.delete_the_index()?;
        db.open_index()?;

        let mut events = db.load_events(100, None)?;
        db.index_events(&events)?;

        loop {
            events = db.load_events(100, events.last())?;

            if events.is_empty() {
                break;
            }

            db.index_events(&events)?;
            db.commit()?;
        }

        db.commit_and_close()?;

        Ok(())
    }

    fn complete(
        self,
        mut cx: TaskContext,
        result: Result<Self::Output, Self::Error>,
    ) -> JsResult<Self::JsEvent> {
        match result {
            Ok(_) => Ok(cx.undefined()),
            Err(e) => cx.throw_type_error(e.to_string()),
        }
    }
}
