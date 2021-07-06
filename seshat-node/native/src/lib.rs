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

mod tasks;
mod utils;

use neon::prelude::*;
use seshat::{Database, Error, LoadConfig, LoadDirection, Profile, RecoveryDatabase, RecoveryInfo};
use std::cell::RefCell;
use std::sync::atomic::Ordering;
use std::sync::Mutex;

use crate::tasks::*;
use crate::utils::*;

pub struct Seshat {
    database: Option<Database>,
}
pub struct SeshatRecovery {
    database: Option<RecoveryDatabase>,
    info: RecoveryInfo,
}

impl Finalize for Seshat {}
impl Finalize for SeshatRecovery {}

const CLOSED_ERROR: &str = "Database has been closed or deleted";

impl SeshatRecovery {
    fn new(mut cx: FunctionContext) -> JsResult<JsBox<RefCell<SeshatRecovery>>> {
        let db_path: String = cx.argument::<JsString>(0)?.value(&mut cx);
        let args = cx.argument_opt(1);
        let config = parse_database_config(&mut cx, args)?;
        let database = RecoveryDatabase::new_with_config(db_path, &config)
            .expect("Can't open recovery database.");
        let info = database.info().clone();

        Ok(cx.boxed(RefCell::new(SeshatRecovery {
            database: Some(database),
            info,
        })))
    }

    fn reindex(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let this = cx.argument::<JsBox<RefCell<SeshatRecovery>>>(0)?;

        let database = {
            let db = &mut this.borrow_mut().database;
            db.take()
        };

        let database = match database {
            Some(db) => db,
            None => return cx.throw_type_error("A reindex has been already done"),
        };

        let task = ReindexTask {
            inner: Mutex::new(Some(database)),
        };
        task.schedule(cx)
    }

    fn get_user_version(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let this = cx.argument::<JsBox<RefCell<SeshatRecovery>>>(0)?;

        let connection = {
            let db = &mut this.borrow_mut().database;

            db.as_mut().map_or_else(
                || Err(CLOSED_ERROR),
                |db| Ok(db.get_connection()),
            )
        };

        let connection = match connection {
            Ok(c) => match c {
                Ok(c) => c,
                Err(e) => {
                    return cx.throw_type_error(format!(
                        "Unable to get a database connection {}",
                        e.to_string()
                    ))
                }
            },
            Err(e) => return cx.throw_type_error(e),
        };

        let task = GetUserVersionTask { connection };
        task.schedule(cx)
    }

    fn shutdown(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let this = cx.argument::<JsBox<RefCell<SeshatRecovery>>>(0)?;

        let database = {
            let db = &mut this.borrow_mut().database;
            db.take()
        };

        let task = ShutDownRecoveryDatabaseTask(Mutex::new(database));
        task.schedule(cx)
    }

    fn info(mut cx: FunctionContext) -> JsResult<JsObject> {
        let this = cx.argument::<JsBox<RefCell<SeshatRecovery>>>(0)?;

        let (total, reindexed) = {
            let info = &this.borrow_mut().info;

            let total = info.total_events();
            let reindexed = info.reindexed_events().load(Ordering::Relaxed);
            (total, reindexed)
        };

        let done: f64 = reindexed as f64 / total as f64;
        let total = cx.number(total as f64);
        let reindexed = cx.number(reindexed as f64);
        let done = cx.number(done);

        let info = cx.empty_object();
        info.set(&mut cx, "totalEvents", total)?;
        info.set(&mut cx, "reindexedEvents", reindexed)?;
        info.set(&mut cx, "done", done)?;

        Ok(info.upcast())
    }
}

impl Seshat {
    fn new(mut cx: FunctionContext) -> JsResult<JsBox<RefCell<Seshat>>> {
        let db_path: String = cx.argument::<JsString>(0)?.value(&mut cx);
        let args = cx.argument_opt(1);

        let config = parse_database_config(&mut cx, args)?;

        let db = match Database::new_with_config(&db_path, &config) {
            Ok(db) => db,
            Err(e) => {
                // There doesn't seem to be a way to construct custom
                // Javascript errors from the Rust side, since we never
                // throw a RangeError here, let's hack around this by using
                // one here.
                let error = match e {
                    Error::ReindexError => cx.throw_range_error("Database needs to be reindexed"),
                    e => cx.throw_error(format!("Error opening the database: {:?}", e)),
                };
                return error;
            }
        };

        Ok(cx.boxed(RefCell::new(Seshat { database: Some(db) })))
    }

    fn add_historic_events_sync(mut cx: FunctionContext) -> JsResult<JsBoolean> {
        let receiver = add_historic_events_helper(&mut cx)?;
        let ret = receiver.recv().unwrap();

        match ret {
            Ok(r) => Ok(cx.boolean(r)),
            Err(e) => cx.throw_type_error(e.to_string()),
        }
    }

    fn add_historic_events(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let receiver = add_historic_events_helper(&mut cx)?;

        let task = AddBacklogTask { receiver };
        task.schedule(cx)
    }

    fn load_checkpoints(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let this = cx.argument::<JsBox<RefCell<Seshat>>>(0)?;

        let connection = {
            let db = &mut this.borrow_mut().database;

            db.as_mut().map_or_else(
                || Err(CLOSED_ERROR),
                |db| Ok(db.get_connection()),
            )
        };

        let connection = match connection {
            Ok(c) => match c {
                Ok(c) => c,
                Err(e) => {
                    return cx.throw_type_error(format!(
                        "Unable to get a database connection {}",
                        e.to_string()
                    ))
                }
            },
            Err(e) => return cx.throw_type_error(e),
        };

        let task = LoadCheckPointsTask { connection };
        task.schedule(cx)
    }

    fn add_event(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let this = cx.argument::<JsBox<RefCell<Seshat>>>(0)?;
        let event = cx.argument::<JsObject>(1)?;
        let event = parse_event(&mut cx, *event)?;

        let profile = match cx.argument_opt(2) {
            Some(p) => {
                let p = p.downcast::<JsObject, _>(&mut cx).or_throw(&mut cx)?;
                parse_profile(&mut cx, *p)?
            }
            None => Profile {
                displayname: None,
                avatar_url: None,
            },
        };

        let ret = {
            let db = &this.borrow().database;
            db.as_ref().map_or_else(
                || Err(CLOSED_ERROR),
                |db| {
                    db.add_event(event, profile);
                    Ok(())
                },
            )
        };

        match ret {
            Ok(_) => Ok(cx.undefined()),
            Err(e) => cx.throw_type_error(e),
        }
    }

    fn delete_event(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let this = cx.argument::<JsBox<RefCell<Seshat>>>(0)?;
        let event_id = cx.argument::<JsString>(1)?.value(&mut cx);

        let receiver = {
            let db = &mut this.borrow_mut().database;
            db.as_mut().map_or_else(
                || Err(CLOSED_ERROR),
                |db| Ok(db.delete_event(&event_id)),
            )
        };

        let receiver = match receiver {
            Ok(r) => r,
            Err(e) => return cx.throw_type_error(e),
        };

        let task = DeleteEventTask { receiver };
        task.schedule(cx)
    }

    fn commit(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let this = cx.argument::<JsBox<RefCell<Seshat>>>(0)?;
        let force: bool = match cx.argument_opt(1) {
            Some(w) => w
                .downcast::<JsBoolean, _>(&mut cx)
                .or_throw(&mut cx)?
                .value(&mut cx),
            None => false,
        };

        let receiver = {
            let db = &mut this.borrow_mut().database;
            db.as_mut().map_or_else(
                || Err(CLOSED_ERROR),
                |db| {
                    if force {
                        Ok(db.force_commit_no_wait())
                    } else {
                        Ok(db.commit_no_wait())
                    }
                },
            )
        };

        let receiver = match receiver {
            Ok(r) => r,
            Err(e) => return cx.throw_type_error(e),
        };

        let task = CommitTask { receiver };
        task.schedule(cx)
    }

    fn reload(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let this = cx.argument::<JsBox<RefCell<Seshat>>>(0)?;

        let ret = {
            let db = &mut this.borrow_mut().database;
            db.as_mut().map_or_else(
                || Err(CLOSED_ERROR),
                |db| Ok(db.reload()),
            )
        };

        match ret {
            Ok(r) => match r {
                Ok(()) => Ok(cx.undefined()),
                Err(e) => {
                    let message = format!("Error opening the database: {:?}", e);
                    cx.throw_type_error(message)
                }
            },
            Err(e) => cx.throw_type_error(e),
        }
    }

    fn get_stats(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let this = cx.argument::<JsBox<RefCell<Seshat>>>(0)?;

        let connection = {
            let db = &mut this.borrow_mut().database;

            db.as_mut().map_or_else(
                || Err(CLOSED_ERROR),
                |db| Ok(db.get_connection()),
            )
        };

        let connection = match connection {
            Ok(c) => match c {
                Ok(c) => c,
                Err(e) => {
                    return cx.throw_type_error(format!(
                        "Unable to get a database connection {}",
                        e.to_string()
                    ))
                }
            },
            Err(e) => return cx.throw_type_error(e),
        };

        let task = StatsTask { connection };
        task.schedule(cx)
    }

    fn get_size(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let this = cx.argument::<JsBox<RefCell<Seshat>>>(0)?;

        let path = {
            let db = &mut this.borrow_mut().database;
            db.as_ref().map_or_else(
                || Err(CLOSED_ERROR),
                |db| Ok(db.get_path().to_path_buf()),
            )
        };

        let path = match path {
            Ok(p) => p,
            Err(e) => return cx.throw_type_error(e),
        };

        let task = GetSizeTask { path };
        task.schedule(cx)
    }

    fn is_empty(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let this = cx.argument::<JsBox<RefCell<Seshat>>>(0)?;

        let connection = {
            let db = &mut this.borrow_mut().database;

            db.as_mut().map_or_else(
                || Err(CLOSED_ERROR),
                |db| Ok(db.get_connection()),
            )
        };

        let connection = match connection {
            Ok(c) => match c {
                Ok(c) => c,
                Err(e) => {
                    return cx.throw_type_error(format!(
                        "Unable to get a database connection {}",
                        e.to_string()
                    ))
                }
            },
            Err(e) => return cx.throw_type_error(e),
        };

        let task = IsEmptyTask { connection };
        task.schedule(cx)
    }

    fn is_room_indexed(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let this = cx.argument::<JsBox<RefCell<Seshat>>>(0)?;
        let room_id = cx.argument::<JsString>(1)?.value(&mut cx);

        let connection = {
            let db = &mut this.borrow_mut().database;

            db.as_mut().map_or_else(
                || Err(CLOSED_ERROR),
                |db| Ok(db.get_connection()),
            )
        };

        let connection = match connection {
            Ok(c) => match c {
                Ok(c) => c,
                Err(e) => {
                    return cx.throw_type_error(format!(
                        "Unable to get a database connection {}",
                        e.to_string()
                    ))
                }
            },
            Err(e) => return cx.throw_type_error(e),
        };

        let task = IsRoomIndexedTask {
            connection,
            room_id,
        };
        task.schedule(cx)
    }

    fn get_user_version(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let this = cx.argument::<JsBox<RefCell<Seshat>>>(0)?;

        let connection = {
            let db = &mut this.borrow_mut().database;

            db.as_mut().map_or_else(
                || Err(CLOSED_ERROR),
                |db| Ok(db.get_connection()),
            )
        };

        let connection = match connection {
            Ok(c) => match c {
                Ok(c) => c,
                Err(e) => {
                    return cx.throw_type_error(format!(
                        "Unable to get a database connection {}",
                        e.to_string()
                    ))
                }
            },
            Err(e) => return cx.throw_type_error(e),
        };

        let task = GetUserVersionTask { connection };
        task.schedule(cx)
    }

    fn set_user_version(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let this = cx.argument::<JsBox<RefCell<Seshat>>>(0)?;
        let version = cx.argument::<JsNumber>(1)?;

        let connection = {
            let db = &mut this.borrow_mut().database;

            db.as_mut().map_or_else(
                || Err(CLOSED_ERROR),
                |db| Ok(db.get_connection()),
            )
        };

        let connection = match connection {
            Ok(c) => match c {
                Ok(c) => c,
                Err(e) => {
                    return cx.throw_type_error(format!(
                        "Unable to get a database connection {}",
                        e.to_string()
                    ))
                }
            },
            Err(e) => return cx.throw_type_error(e),
        };

        let task = SetUserVersionTask {
            connection,
            new_version: version.value(&mut cx) as i64,
        };
        task.schedule(cx)
    }

    fn commit_sync(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let this = cx.argument::<JsBox<RefCell<Seshat>>>(0)?;

        let wait: bool = match cx.argument_opt(1) {
            Some(w) => w
                .downcast::<JsBoolean, _>(&mut cx)
                .or_throw(&mut cx)?
                .value(&mut cx),
            None => false,
        };

        let force: bool = match cx.argument_opt(2) {
            Some(w) => w
                .downcast::<JsBoolean, _>(&mut cx)
                .or_throw(&mut cx)?
                .value(&mut cx),
            None => false,
        };

        let ret = {
            let db = &mut this.borrow_mut().database;

            if wait {
                db.as_mut().map_or_else(
                    || Err(CLOSED_ERROR),
                    |db| {
                        if force {
                            Ok(Some(db.force_commit()))
                        } else {
                            Ok(Some(db.commit()))
                        }
                    },
                )
            } else {
                db.as_mut().map_or_else(
                    || Err(CLOSED_ERROR),
                    |db| {
                        db.commit_no_wait();
                        Ok(None)
                    },
                )
            }
        };

        let ret = match ret {
            Ok(r) => r,
            Err(e) => return cx.throw_type_error(e),
        };

        match ret {
            Some(_) => Ok(cx.undefined()),
            None => Ok(cx.undefined()),
        }
    }

    fn search_sync(mut cx: FunctionContext) -> JsResult<JsObject> {
        let this = cx.argument::<JsBox<RefCell<Seshat>>>(0)?;
        let args = cx.argument::<JsObject>(1)?;
        let (term, config) = parse_search_object(&mut cx, args)?;

        let ret = {
            let db = &mut this.borrow_mut().database;
            db.as_ref().map_or_else(
                || Err(CLOSED_ERROR),
                |db| Ok(db.search(&term, &config)),
            )
        };

        let ret = match ret {
            Ok(r) => r,
            Err(e) => return cx.throw_type_error(e),
        };

        let mut ret = match ret {
            Ok(r) => r,
            Err(e) => return cx.throw_type_error(e.to_string()),
        };

        let count = ret.count;
        let results = JsArray::new(&mut cx, ret.results.len() as u32);
        let count = cx.number(count as f64);

        for (i, element) in ret.results.drain(..).enumerate() {
            let object = search_result_to_js(&mut cx, element)?;
            results.set(&mut cx, i as u32, object)?;
        }

        let search_result = cx.empty_object();
        let highlights = JsArray::new(&mut cx, 0);

        search_result.set(&mut cx, "count", count)?;
        search_result.set(&mut cx, "results", results)?;
        search_result.set(&mut cx, "highlights", highlights)?;

        if let Some(next_batch) = ret.next_batch {
            let next_batch = cx.string(next_batch.to_hyphenated().to_string());
            search_result.set(&mut cx, "next_batch", next_batch)?;
        }

        Ok(search_result.upcast())
    }

    fn search(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let this = cx.argument::<JsBox<RefCell<Seshat>>>(0)?;
        let args = cx.argument::<JsObject>(1)?;

        let (term, config) = parse_search_object(&mut cx, args)?;

        let searcher = {
            let db = &mut this.borrow_mut().database;
            db.as_ref().map_or_else(
                || Err(CLOSED_ERROR),
                |db| Ok(db.get_searcher()),
            )
        };

        let searcher = match searcher {
            Ok(s) => s,
            Err(e) => return cx.throw_type_error(e.to_string()),
        };

        let task = SearchTask {
            inner: searcher,
            term,
            config,
        };
        task.schedule(cx)
    }

    fn delete(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let this = cx.argument::<JsBox<RefCell<Seshat>>>(0)?;
        let db = this.borrow_mut().database.take();

        let db = match db {
            Some(db) => db,
            None => return cx.throw_type_error(CLOSED_ERROR),
        };

        let db_path = db.get_path().to_path_buf();
        let receiver = db.shutdown();

        let task = DeleteTask {
            db_path,
            shutdown_receiver: receiver,
        };
        task.schedule(cx)
    }

    fn change_passphrase(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let this = cx.argument::<JsBox<RefCell<Seshat>>>(0)?;
        let new_passphrase = cx.argument::<JsString>(1)?;

        let db = {
            let db = &mut this.borrow_mut().database;
            db.take()
        };

        let db = match db {
            Some(db) => db,
            None => return cx.throw_type_error(CLOSED_ERROR),
        };

        let task = ChangePassphraseTask {
            database: Mutex::new(Some(db)),
            new_passphrase: new_passphrase.value(&mut cx),
        };

        task.schedule(cx)
    }

    fn shutdown(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let this = cx.argument::<JsBox<RefCell<Seshat>>>(0)?;

        let db = {
            let db = &mut this.borrow_mut().database;
            db.take()
        };

        let db = match db {
            Some(db) => db,
            None => return cx.throw_type_error(CLOSED_ERROR),
        };

        let receiver = db.shutdown();

        let task = ShutDownTask {
            shutdown_receiver: receiver,
        };
        task.schedule(cx)
    }

    fn load_file_events(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let this = cx.argument::<JsBox<RefCell<Seshat>>>(0)?;
        let args = cx.argument::<JsObject>(1)?;

        let room_id = args
            .get(&mut cx, "roomId")?
            .downcast::<JsString, _>(&mut cx)
            .or_throw(&mut cx)?
            .value(&mut cx);

        let mut config = LoadConfig::new(room_id);

        let limit = args
            .get(&mut cx, "limit")?
            .downcast::<JsNumber, _>(&mut cx)
            .or_throw(&mut cx)?
            .value(&mut cx);

        config = config.limit(limit as usize);

        if let Ok(e) = args.get(&mut cx, "fromEvent") {
            if let Ok(e) = e.downcast::<JsString, _>(&mut cx) {
                config = config.from_event(e.value(&mut cx));
            }
        };

        if let Ok(d) = args.get(&mut cx, "direction") {
            if let Ok(e) = d.downcast::<JsString, _>(&mut cx) {
                let direction = match e.value(&mut cx).to_lowercase().as_ref() {
                    "backwards" | "backward" | "b" => LoadDirection::Backwards,
                    "forwards" | "forward" | "f" => LoadDirection::Forwards,
                    "" => LoadDirection::Backwards,
                    d => return cx.throw_error(format!("Unknown load direction {}", d)),
                };

                config = config.direction(direction);
            }
        }

        let connection = {
            let db = &mut this.borrow_mut().database;
            db.as_ref().map_or_else(
                || Err(CLOSED_ERROR),
                |db| Ok(db.get_connection()),
            )
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

        task.schedule(cx)
    }
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("createRecoveryDb", SeshatRecovery::new)?;
    cx.export_function("reindexRecoveryDb", SeshatRecovery::reindex)?;
    cx.export_function("getUserVersionRecoveryDb", SeshatRecovery::get_user_version)?;
    cx.export_function("shutdownRecoveryDb", SeshatRecovery::shutdown)?;
    cx.export_function("getInfoRecoveryDb", SeshatRecovery::info)?;

    cx.export_function("createDb", Seshat::new)?;
    cx.export_function("addHistoricEventsSync", Seshat::add_historic_events_sync)?;
    cx.export_function("addHistoricEvents", Seshat::add_historic_events)?;
    cx.export_function("loadCheckpoints", Seshat::load_checkpoints)?;
    cx.export_function("addEvent", Seshat::add_event)?;
    cx.export_function("deleteEvent", Seshat::delete_event)?;
    cx.export_function("commit", Seshat::commit)?;
    cx.export_function("reload", Seshat::reload)?;
    cx.export_function("getStats", Seshat::get_stats)?;
    cx.export_function("getSize", Seshat::get_size)?;
    cx.export_function("isEmpty", Seshat::is_empty)?;
    cx.export_function("isRoomIndexed", Seshat::is_room_indexed)?;
    cx.export_function("getUserVersion", Seshat::get_user_version)?;
    cx.export_function("setUserVersion", Seshat::set_user_version)?;
    cx.export_function("commitSync", Seshat::commit_sync)?;
    cx.export_function("searchSync", Seshat::search_sync)?;
    cx.export_function("search", Seshat::search)?;
    cx.export_function("deleteDb", Seshat::delete)?;
    cx.export_function("changePassphrase", Seshat::change_passphrase)?;
    cx.export_function("shutdown", Seshat::shutdown)?;
    cx.export_function("loadFileEvents", Seshat::load_file_events)?;

    Ok(())
}
