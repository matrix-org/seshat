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

mod utils;

use fs_extra::dir;
use std::path::PathBuf;

use neon::prelude::*;
use seshat::{
    CheckpointDirection, Config, Connection, CrawlerCheckpoint, Database, DatabaseStats,
    Language, LoadConfig, LoadDirection, Profile, Receiver, SearchConfig, SearchResult,
    Searcher,
};
use crate::utils::*;

#[no_mangle]
pub extern "C" fn __cxa_pure_virtual() {
    loop {}
}

pub struct SeshatDatabase(Option<Database>);

struct CommitTask {
    receiver: Receiver<seshat::Result<()>>,
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

struct SearchTask {
    inner: Searcher,
    term: String,
    config: SearchConfig,
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

struct AddBacklogTask {
    receiver: Receiver<seshat::Result<bool>>,
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

struct LoadCheckPointsTask {
    connection: Connection,
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

struct IsEmptyTask {
    connection: Connection,
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

struct StatsTask {
    connection: Connection,
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

struct GetSizeTask {
    path: PathBuf,
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

struct DeleteTask {
    db_path: PathBuf,
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

struct LoadFileEventsTask {
    inner: Connection,
    config: LoadConfig,
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

declare_types! {
    pub class Seshat for SeshatDatabase {
        init(mut cx) {
            let db_path: String = cx.argument::<JsString>(0)?.value();
            let mut config = Config::new();

            if let Some(c) =  cx.argument_opt(1) {
                let c = c.downcast::<JsObject>().or_throw(&mut cx)?;

                if let Ok(l) = c.get(&mut cx, "language") {
                    if let Ok(l) = l.downcast::<JsString>() {
                        let language = Language::from(l.value().as_ref());
                        match language {
                            Language::Unknown => return cx.throw_type_error(
                                format!("Unsuported language: {}", l.value())
                            ),
                            _ => {config = config.set_language(&language);}
                        }
                    }
                }

                if let Ok(p) = c.get(&mut cx, "passphrase") {
                    if let Ok(p) = p.downcast::<JsString>() {
                        let passphrase: String = p.value();
                        config = config.set_passphrase(passphrase);
                    }
                }
            }

            let db = match Database::new_with_config(&db_path, &config) {
                Ok(db) => db,
                Err(e) => {
                    let message = format!("Error opening the database: {:?}", e);
                    panic!(message)
                }
            };

            Ok(
                SeshatDatabase(Some(db))
           )
        }

        method addHistoricEventsSync(mut cx) {
            let receiver = add_historic_events_helper(&mut cx)?;
            let ret = receiver.recv().unwrap();

            match ret {
                Ok(r) => Ok(JsBoolean::new(&mut cx, r).upcast()),
                Err(e) => cx.throw_type_error(e.to_string()),
            }
        }

        method addHistoricEvents(mut cx) {
            let f = cx.argument::<JsFunction>(3)?;
            let receiver = add_historic_events_helper(&mut cx)?;

            let task = AddBacklogTask { receiver };
            task.schedule(f);

            Ok(cx.undefined().upcast())
        }

        method loadCheckpoints(mut cx) {
            let f = cx.argument::<JsFunction>(0)?;
            let mut this = cx.this();

            let connection = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;

                db.as_mut().map_or_else(|| Err("Database has been deleted"), |db| Ok(db.get_connection()))
            };

            let connection = match connection {
                Ok(c) => match c {
                    Ok(c) => c,
                    Err(e) => return cx.throw_type_error(format!("Unable to get a database connection {}", e.to_string())),
                },
                Err(e) => return cx.throw_type_error(e),
            };

            let task = LoadCheckPointsTask { connection };
            task.schedule(f);

            Ok(cx.undefined().upcast())
        }

        method addEvent(mut cx) {
            let event = cx.argument::<JsObject>(0)?;
            let event = parse_event(&mut cx, *event)?;

            let profile = match cx.argument_opt(1) {
                Some(p) => {
                    let p = p.downcast::<JsObject>().or_throw(&mut cx)?;
                    parse_profile(&mut cx, *p)?
                },
                None => Profile { displayname: None, avatar_url: None },
            };

            let ret = {
                let this = cx.this();
                let guard = cx.lock();
                let db = &this.borrow(&guard).0;
                db.as_ref().map_or_else(|| Err("Database has been deleted"), |db| { db.add_event(event, profile); Ok(()) } )
            };

            match ret {
                Ok(_) => Ok(cx.undefined().upcast()),
                Err(e) => cx.throw_type_error(e),
            }
        }

        method commit(mut cx) {
            let force: bool = match cx.argument_opt(0) {
                Some(w) => w.downcast::<JsBoolean>().or_throw(&mut cx)?.value(),
                None => false,
            };

            let f = cx.argument::<JsFunction>(1)?;
            let mut this = cx.this();

            let receiver = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;
                db.as_mut().map_or_else(|| Err("Database has been deleted"), |db| {
                    if force {
                        Ok(db.force_commit_no_wait())
                    } else {
                        Ok(db.commit_no_wait())
                    }
                })
            };

            let receiver = match receiver {
                Ok(r) => r,
                Err(e) => return cx.throw_type_error(e),
            };

            let task = CommitTask { receiver };
            task.schedule(f);

            Ok(cx.undefined().upcast())
        }

        method reload(mut cx) {
            let mut this = cx.this();

            let ret = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;
                db.as_mut().map_or_else(|| Err("Database has been deleted"), |db| Ok(db.reload()))
            };

            match ret {
                Ok(r) => match r {
                    Ok(()) => Ok(cx.undefined().upcast()),
                    Err(e) => {
                        let message = format!("Error opening the database: {:?}", e);
                        cx.throw_type_error(message)
                    }
                },
                Err(e) => cx.throw_type_error(e),
            }
        }

        method getStats(mut cx) {
            let f = cx.argument::<JsFunction>(0)?;

            let mut this = cx.this();

            let connection = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;

                db.as_mut().map_or_else(|| Err("Database has been deleted"), |db| Ok(db.get_connection()))
            };

            let connection = match connection {
                Ok(c) => match c {
                    Ok(c) => c,
                    Err(e) => return cx.throw_type_error(format!("Unable to get a database connection {}", e.to_string())),
                },
                Err(e) => return cx.throw_type_error(e),
            };

            let task = StatsTask { connection };
            task.schedule(f);

            Ok(cx.undefined().upcast())
        }

        method getSize(mut cx) {
            let f = cx.argument::<JsFunction>(0)?;

            let mut this = cx.this();

            let path = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;
                db.as_ref().map_or_else(|| Err("Database has been deleted"), |db| Ok(db.get_path().to_path_buf()))
            };

            let path = match path {
                Ok(p) => p,
                Err(e) => return cx.throw_type_error(e),
            };

            let task = GetSizeTask { path };
            task.schedule(f);

            Ok(cx.undefined().upcast())
        }

        method isEmpty(mut cx) {
            let f = cx.argument::<JsFunction>(0)?;
            let mut this = cx.this();

            let connection = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;

                db.as_mut().map_or_else(|| Err("Database has been deleted"), |db| Ok(db.get_connection()))
            };

            let connection = match connection {
                Ok(c) => match c {
                    Ok(c) => c,
                    Err(e) => return cx.throw_type_error(format!("Unable to get a database connection {}", e.to_string())),
                },
                Err(e) => return cx.throw_type_error(e),
            };

            let task = IsEmptyTask { connection };
            task.schedule(f);

            Ok(cx.undefined().upcast())
        }

        method commitSync(mut cx) {
            let wait: bool = match cx.argument_opt(0) {
                Some(w) => w.downcast::<JsBoolean>().or_throw(&mut cx)?.value(),
                None => false,
            };

            let force: bool = match cx.argument_opt(1) {
                Some(w) => w.downcast::<JsBoolean>().or_throw(&mut cx)?.value(),
                None => false,
            };

            let mut this = cx.this();

            let ret = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;

                if wait {
                    db.as_mut().map_or_else(|| Err("Database has been deleted"), |db| {
                        if force {
                            Ok(Some(db.force_commit()))
                        } else {
                            Ok(Some(db.commit()))
                        }
                    }
                   )
                } else {
                    db.as_mut().map_or_else(|| Err("Database has been deleted"), |db| { db.commit_no_wait(); Ok(None) } )
                }
            };

            let ret = match ret {
                Ok(r) => r,
                Err(e) => return cx.throw_type_error(e),
            };

            match ret {
                Some(_) => Ok(cx.undefined().upcast()),
                None => Ok(cx.undefined().upcast())
            }
        }

        method searchSync(mut cx) {
            let args = cx.argument::<JsObject>(0)?;
            let (term, config) = parse_search_object(&mut cx, args)?;
            let mut this = cx.this();

            let ret = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;
                db.as_ref().map_or_else(|| Err("Database has been deleted"), |db| Ok(db.search(&term, &config)))
            };

            let ret = match ret {
                Ok(r) => r,
                Err(e) => return cx.throw_type_error(e),
            };

            let mut ret = match ret {
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

            Ok(search_result.upcast())
        }

        method search(mut cx) {
            let args = cx.argument::<JsObject>(0)?;
            let f = cx.argument::<JsFunction>(1)?;

            let (term, config) = parse_search_object(&mut cx, args)?;

            let mut this = cx.this();

            let searcher = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;
                db.as_ref().map_or_else(|| Err("Database has been deleted"), |db| Ok(db.get_searcher()))
            };

            let searcher = match searcher {
                Ok(s) => s,
                Err(e) => return cx.throw_type_error(e.to_string()),
            };

            let task = SearchTask {
                inner: searcher,
                term,
                config
            };
            task.schedule(f);

            Ok(cx.undefined().upcast())
        }

        method delete(mut cx) {
            let f = cx.argument::<JsFunction>(0)?;

            let mut this = cx.this();

            let db = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;
                db.take()
            };

            let db = match db {
                Some(db) => db,
                None => return cx.throw_type_error("Database has been deleted")
            };

            let path = db.get_path();

            let task = DeleteTask { db_path: path.to_path_buf() };
            task.schedule(f);

            Ok(cx.undefined().upcast())
        }

        method loadFileEvents(mut cx) {
            let args = cx.argument::<JsObject>(0)?;
            let f = cx.argument::<JsFunction>(1)?;

            let room_id = args
                    .get(&mut cx, "roomId")?
                    .downcast::<JsString>()
                    .or_throw(&mut cx)?
                    .value();

            let mut config = LoadConfig::new(room_id);

            let limit = args
                    .get(&mut cx, "limit")?
                    .downcast::<JsNumber>()
                    .or_throw(&mut cx)?
                    .value();

            config = config.limit(limit as usize);

            if let Ok(e) = args.get(&mut cx, "fromEvent") {
                if let Ok(e) = e.downcast::<JsString>() {
                    config = config.from_event(e.value());
                }
            };

            if let Ok(d) = args.get(&mut cx, "direction") {
                if let Ok(e) = d.downcast::<JsString>() {
                    let direction = match e.value().to_lowercase().as_ref() {
                        "backwards" | "backward" | "b" => LoadDirection::Backwards,
                        "forwards" | "forward" | "f" => LoadDirection::Forwards,
                        "" => LoadDirection::Backwards,
                        d => return cx.throw_error(format!("Unknown load direction {}", d)),
                    };

                    config = config.direction(direction);
                }
            }

            let mut this = cx.this();

            let connection = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;
                db
                    .as_ref()
                    .map_or_else(|| Err("Database has been deleted"), |db| Ok(db.get_connection()))
            };

            let connection = match connection {
                Ok(s) => match s {
                    Ok(s) => s,
                    Err(e) => return cx.throw_type_error(e.to_string()),
                },
                Err(e) => return cx.throw_type_error(e.to_string()),
            };

            let task = LoadFileEventsTask {
                inner: connection,
                config,
            };

            task.schedule(f);

            Ok(cx.undefined().upcast())
        }
    }
}

register_module!(mut cx, { cx.export_class::<Seshat>("Seshat") });
