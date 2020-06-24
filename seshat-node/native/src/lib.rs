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
use std::sync::atomic::Ordering;
use std::sync::Mutex;

use crate::tasks::*;
use crate::utils::*;

pub struct SeshatDatabase(Option<Database>);
pub struct SeshatRecoveryDb {
    database: Option<RecoveryDatabase>,
    info: RecoveryInfo,
}

declare_types! {
    pub class SeshatRecovery for SeshatRecoveryDb {
        init(mut cx) {
            let db_path: String = cx.argument::<JsString>(0)?.value();
            let args =  cx.argument_opt(1);
            let config = parse_database_config(&mut cx, args)?;
            let database = RecoveryDatabase::new_with_config(db_path, &config)
                .expect("Can't open recovery database.");
            let info = database.info().clone();

            Ok(SeshatRecoveryDb{
                database: Some(database),
                info
            })
        }

        method reindex(mut cx) {
            let f = cx.argument::<JsFunction>(0)?;
            let mut this = cx.this();

            let database = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).database;
                db.take()
            };

            let database = match database {
                Some(db) => db,
                None => return cx.throw_type_error("A reindex has been already done"),
            };

            let task = ReindexTask { inner: Mutex::new(Some(database)) };
            task.schedule(f);

            Ok(cx.undefined().upcast())
        }

        method getUserVersion(mut cx) {
            let f = cx.argument::<JsFunction>(0)?;
            let mut this = cx.this();

            let connection = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).database;

                db.as_mut().map_or_else(|| Err("Database has been closed or deleted"),
                                        |db| Ok(db.get_connection()))
            };

            let connection = match connection {
                Ok(c) => match c {
                    Ok(c) => c,
                    Err(e) => return cx.throw_type_error(format!(
                        "Unable to get a database connection {}",
                        e.to_string()
                    )),
                },
                Err(e) => return cx.throw_type_error(e),
            };

            let task = GetUserVersionTask { connection };
            task.schedule(f);

            Ok(cx.undefined().upcast())
        }

        method shutdown(mut cx) {
            let f = cx.argument::<JsFunction>(0)?;

            let mut this = cx.this();

            let database = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).database;
                db.take()
            };

            let task = ShutDownRecoveryDatabaseTask(Mutex::new(database));
            task.schedule(f);

            Ok(cx.undefined().upcast())
        }

        method info(mut cx) {
            let mut this = cx.this();

            let (total, reindexed) = {
                let guard = cx.lock();
                let info = &this.borrow_mut(&guard).info;

                let total = info.total_events();
                let reindexed = info.reindexed_events().load(Ordering::Relaxed);
                (total, reindexed)
            };

            let done: f64 = reindexed as f64 / total as f64;
            let total = JsNumber::new(&mut cx, total as f64);
            let reindexed = JsNumber::new(&mut cx, reindexed as f64);
            let done = JsNumber::new(&mut cx, done);

            let info = JsObject::new(&mut cx);
            info.set(&mut cx, "totalEvents", total)?;
            info.set(&mut cx, "reindexedEvents", reindexed)?;
            info.set(&mut cx, "done", done)?;

            Ok(info.upcast())
        }
    }

    pub class Seshat for SeshatDatabase {
        init(mut cx) {
            let db_path: String = cx.argument::<JsString>(0)?.value();
            let args =  cx.argument_opt(1);

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
                        e => cx.throw_error(format!("Error opening the database: {:?}", e))
                    };
                    return error;
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

                db.as_mut().map_or_else(|| Err("Database has been closed or deleted"),
                                        |db| Ok(db.get_connection()))
            };

            let connection = match connection {
                Ok(c) => match c {
                    Ok(c) => c,
                    Err(e) => return cx.throw_type_error(format!(
                        "Unable to get a database connection {}",
                        e.to_string()
                    )),
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
                db.as_ref().map_or_else(|| Err("Database has been closed or deleted"),
                                        |db| { db.add_event(event, profile); Ok(()) } )
            };

            match ret {
                Ok(_) => Ok(cx.undefined().upcast()),
                Err(e) => cx.throw_type_error(e),
            }
        }

        method deleteEvent(mut cx) {
            let event_id = cx.argument::<JsString>(0)?.value();
            let f = cx.argument::<JsFunction>(1)?;
            let mut this = cx.this();

            let receiver = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;
                db.as_mut().map_or_else(|| Err("Database has been closed or deleted"), |db| {
                    Ok(db.delete_event(&event_id))
                })
            };

            let receiver = match receiver {
                Ok(r) => r,
                Err(e) => return cx.throw_type_error(e),
            };

            let task = DeleteEventTask { receiver };
            task.schedule(f);

            Ok(cx.undefined().upcast())
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
                db.as_mut().map_or_else(|| Err("Database has been closed or deleted"), |db| {
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
                db.as_mut().map_or_else(|| Err("Database has been closed or deleted"),
                                        |db| Ok(db.reload()))
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

                db.as_mut().map_or_else(|| Err("Database has been closed or deleted"),
                                        |db| Ok(db.get_connection()))
            };

            let connection = match connection {
                Ok(c) => match c {
                    Ok(c) => c,
                    Err(e) => return cx.throw_type_error(format!(
                        "Unable to get a database connection {}",
                        e.to_string()
                    )),
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
                db.as_ref().map_or_else(|| Err("Database has been closed or deleted"),
                                        |db| Ok(db.get_path().to_path_buf()))
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

                db.as_mut().map_or_else(|| Err("Database has been closed or deleted"),
                                        |db| Ok(db.get_connection()))
            };

            let connection = match connection {
                Ok(c) => match c {
                    Ok(c) => c,
                    Err(e) => return cx.throw_type_error(format!(
                        "Unable to get a database connection {}",
                        e.to_string()
                    )),
                },
                Err(e) => return cx.throw_type_error(e),
            };

            let task = IsEmptyTask { connection };
            task.schedule(f);

            Ok(cx.undefined().upcast())
        }

        method isRoomIndexed(mut cx) {
            let room_id = cx.argument::<JsString>(0)?.value();
            let f = cx.argument::<JsFunction>(1)?;
            let mut this = cx.this();

            let connection = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;

                db.as_mut().map_or_else(|| Err("Database has been closed or deleted"),
                                        |db| Ok(db.get_connection()))
            };

            let connection = match connection {
                Ok(c) => match c {
                    Ok(c) => c,
                    Err(e) => return cx.throw_type_error(format!(
                        "Unable to get a database connection {}",
                        e.to_string()
                    )),
                },
                Err(e) => return cx.throw_type_error(e),
            };

            let task = IsRoomIndexedTask { connection, room_id };
            task.schedule(f);

            Ok(cx.undefined().upcast())
        }

        method getUserVersion(mut cx) {
            let f = cx.argument::<JsFunction>(0)?;
            let mut this = cx.this();

            let connection = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;

                db.as_mut().map_or_else(|| Err("Database has been closed or deleted"),
                                        |db| Ok(db.get_connection()))
            };

            let connection = match connection {
                Ok(c) => match c {
                    Ok(c) => c,
                    Err(e) => return cx.throw_type_error(format!(
                        "Unable to get a database connection {}",
                        e.to_string()
                    )),
                },
                Err(e) => return cx.throw_type_error(e),
            };

            let task = GetUserVersionTask { connection };
            task.schedule(f);

            Ok(cx.undefined().upcast())
        }

        method setUserVersion(mut cx) {
            let version = cx.argument::<JsNumber>(0)?;
            let f = cx.argument::<JsFunction>(1)?;
            let mut this = cx.this();

            let connection = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;

                db.as_mut().map_or_else(|| Err("Database has been closed or deleted"),
                                        |db| Ok(db.get_connection()))
            };

            let connection = match connection {
                Ok(c) => match c {
                    Ok(c) => c,
                    Err(e) => return cx.throw_type_error(format!(
                        "Unable to get a database connection {}",
                        e.to_string()
                    )),
                },
                Err(e) => return cx.throw_type_error(e),
            };

            let task = SetUserVersionTask { connection, new_version: version.value() as i64 };
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
                    db.as_mut().map_or_else(|| Err("Database has been closed or deleted"), |db| {
                        if force {
                            Ok(Some(db.force_commit()))
                        } else {
                            Ok(Some(db.commit()))
                        }
                    }
                   )
                } else {
                    db.as_mut().map_or_else(|| Err("Database has been closed or deleted"),
                                            |db| { db.commit_no_wait(); Ok(None) } )
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
                db.as_ref().map_or_else(|| Err("Database has been closed or deleted"),
                                        |db| Ok(db.search(&term, &config)))
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
            let count = JsNumber::new(&mut cx, count as f64);

            for (i, element) in ret.results.drain(..).enumerate() {
                let object = search_result_to_js(&mut cx, element)?;
                results.set(&mut cx, i as u32, object)?;
            }

            let search_result = JsObject::new(&mut cx);
            let highlights = JsArray::new(&mut cx, 0);

            search_result.set(&mut cx, "count", count)?;
            search_result.set(&mut cx, "results", results)?;
            search_result.set(&mut cx, "highlights", highlights)?;

            if let Some(next_batch) = ret.next_batch {
                let next_batch = JsString::new(&mut cx, next_batch.to_hyphenated().to_string());
                search_result.set(&mut cx, "next_batch", next_batch)?;
            }

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
                db.as_ref().map_or_else(|| Err("Database has been closed or deleted"),
                                        |db| Ok(db.get_searcher()))
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
                None => return cx.throw_type_error("Database has been closed or deleted")
            };

            let db_path = db.get_path().to_path_buf();
            let receiver = db.shutdown();

            let task = DeleteTask {
                db_path,
                shutdown_receiver: receiver,
            };
            task.schedule(f);

            Ok(cx.undefined().upcast())
        }

        method changePassphrase(mut cx) {
            let new_passphrase = cx.argument::<JsString>(0)?;
            let f = cx.argument::<JsFunction>(1)?;

            let mut this = cx.this();

            let db = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;
                db.take()
            };

            let db = match db {
                Some(db) => db,
                None => return cx.throw_type_error("Database has been closed or deleted")
            };

            let task = ChangePassphraseTask {
                database: Mutex::new(Some(db)),
                new_passphrase: new_passphrase.value(),
            };

            task.schedule(f);

            Ok(cx.undefined().upcast())
        }

        method shutdown(mut cx) {
            let f = cx.argument::<JsFunction>(0)?;

            let mut this = cx.this();

            let db = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;
                db.take()
            };

            let db = match db {
                Some(db) => db,
                None => return cx.throw_type_error("Database has been closed or deleted")
            };

            let receiver = db.shutdown();

            let task = ShutDownTask {
                shutdown_receiver: receiver,
            };
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
                    .map_or_else(|| Err("Database has been closed or deleted"),
                                 |db| Ok(db.get_connection()))
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

register_module!(mut cx, {
    cx.export_class::<Seshat>("Seshat")?;
    cx.export_class::<SeshatRecovery>("SeshatRecovery")?;
    Ok(())
});
