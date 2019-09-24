// Copyright 2019 The Matrix.org Foundation CIC
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

#[macro_use]
extern crate neon;
use fs_extra::dir;
use std::path::PathBuf;
use std::sync::mpsc::Receiver;

use neon::prelude::*;
use neon_serde;
use serde_json;
use seshat::{
    BacklogCheckpoint, Connection, Database, Event, EventType, Profile, SearchConfig, SearchResult,
    Searcher,
};

#[no_mangle]
pub extern "C" fn __cxa_pure_virtual() {
    loop {}
}

pub struct SeshatDatabase(Database);

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
    type Output = Vec<BacklogCheckpoint>;
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

            js_checkpoint.set(&mut cx, "room_id", room_id)?;
            js_checkpoint.set(&mut cx, "token", token)?;
            js_checkpoint.set(&mut cx, "full_crawl", full_crawl)?;

            ret.set(&mut cx, i as u32, js_checkpoint)?;
        }

        Ok(ret)
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

declare_types! {
    pub class Seshat for SeshatDatabase {
        init(mut cx) {
            let db_path: String = cx.argument::<JsString>(0)?.value();

            let db = match Database::new(&db_path) {
                Ok(db) => db,
                Err(e) => {
                    let message = format!("Error opening the database: {:?}", e);
                    panic!(message)
                }
            };

            Ok(
                SeshatDatabase(db)
           )
        }

        method addBacklogEventsSync(mut cx) {
            let receiver = add_backlog_events_helper(&mut cx)?;
            let ret = receiver.recv().unwrap();

            match ret {
                Ok(r) => Ok(JsBoolean::new(&mut cx, r).upcast()),
                Err(e) => cx.throw_type_error(e.to_string()),
            }
        }

        method addBacklogEvents(mut cx) {
            let f = cx.argument::<JsFunction>(3)?;
            let receiver = add_backlog_events_helper(&mut cx)?;

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
                db.get_connection()
            };

            let connection = match connection {
                Ok(c) => c,
                Err(e) => return cx.throw_type_error(format!("Unable to get a database connection {}", e.to_string())),
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
                None => Profile { display_name: None, avatar_url: None },
            };

            {
                let this = cx.this();
                let guard = cx.lock();
                let db = &this.borrow(&guard).0;
                db.add_event(event, profile);
            }

            Ok(cx.undefined().upcast())
        }

        method commitAsync(mut cx) {
            let f = cx.argument::<JsFunction>(0)?;
            let mut this = cx.this();

            let receiver = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;
                db.commit_no_wait()
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
                db.reload()
            };

            match ret {
                Ok(()) => Ok(cx.undefined().upcast()),
                Err(e) => {
                    let message = format!("Error opening the database: {:?}", e);
                    panic!(message)
                }
            }
        }

        method getSize(mut cx) {
            let f = cx.argument::<JsFunction>(0)?;

            let mut this = cx.this();

            let path = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;
                db.get_path().to_path_buf()
            };

            let task = GetSizeTask { path };
            task.schedule(f);

            Ok(cx.undefined().upcast())

        }

        method commitSync(mut cx) {
            let wait: bool = match cx.argument_opt(0) {
                Some(w) => w.downcast::<JsBoolean>().or_throw(&mut cx)?.value(),
                None => false,
            };

            let mut this = cx.this();

            let ret = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;

                if wait {
                    Some(db.commit())
                } else {
                    db.commit_no_wait();
                    None
                }
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
                db.search(&term, &config)
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

        method searchAsync(mut cx) {
            let args = cx.argument::<JsObject>(0)?;
            let f = cx.argument::<JsFunction>(1)?;

            let (term, config) = parse_search_object(&mut cx, args)?;

            let mut this = cx.this();

            let searcher = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;
                db.get_searcher()
            };

            let task = SearchTask {
                inner: searcher,
                term,
                config
            };
            task.schedule(f);

            Ok(cx.undefined().upcast())
        }
    }
}

fn parse_search_object(
    cx: &mut CallContext<Seshat>,
    argument: Handle<JsObject>,
) -> Result<(String, SearchConfig), neon::result::Throw> {
    let term = argument
        .get(&mut *cx, "search_term")?
        .downcast::<JsString>()
        .or_throw(&mut *cx)?
        .value();

    let limit: usize = argument
        .get(&mut *cx, "limit")?
        .downcast::<JsNumber>()
        .unwrap_or_else(|_| JsNumber::new(&mut *cx, 10))
        .value() as usize;

    let before_limit: usize = argument
        .get(&mut *cx, "before_limit")?
        .downcast::<JsNumber>()
        .unwrap_or_else(|_| JsNumber::new(&mut *cx, 0))
        .value() as usize;

    let after_limit: usize = argument
        .get(&mut *cx, "before_limit")?
        .downcast::<JsNumber>()
        .unwrap_or_else(|_| JsNumber::new(&mut *cx, 0))
        .value() as usize;

    let order_by_recent: bool = argument
        .get(&mut *cx, "order_by_recent")?
        .downcast::<JsBoolean>()
        .unwrap_or_else(|_| JsBoolean::new(&mut *cx, false))
        .value();

    let room_id = argument.get(&mut *cx, "room_id");

    let room_id: Option<String> = match room_id {
        Ok(r) => {
            if let Ok(r) = r.downcast::<JsString>() {
                Some(r.value())
            } else {
                None
            }
        }
        Err(_e) => None,
    };

    let config = SearchConfig {
        limit,
        before_limit,
        after_limit,
        order_by_recent,
        room_id,
    };

    Ok((term, config))
}

fn parse_checkpoint(
    cx: &mut CallContext<Seshat>,
    argument: Option<Handle<JsValue>>,
) -> Result<Option<BacklogCheckpoint>, neon::result::Throw> {
    match argument {
        Some(c) => match c.downcast::<JsObject>() {
            Ok(object) => Ok(Some(js_checkpoint_to_rust(cx, *object)?)),
            Err(_e) => {
                let _o = c.downcast::<JsNull>().or_throw(cx)?;
                Ok(None)
            }
        },
        None => Ok(None),
    }
}

fn add_backlog_events_helper(
    cx: &mut CallContext<Seshat>,
) -> Result<Receiver<seshat::Result<bool>>, neon::result::Throw> {
    let js_events = cx.argument::<JsArray>(0)?;
    let mut js_events: Vec<Handle<JsValue>> = js_events.to_vec(cx)?;

    let js_checkpoint = cx.argument_opt(1);
    let new_checkpoint: Option<BacklogCheckpoint> = parse_checkpoint(cx, js_checkpoint)?;

    let js_checkpoint = cx.argument_opt(2);
    let old_checkpoint: Option<BacklogCheckpoint> = parse_checkpoint(cx, js_checkpoint)?;

    let mut events: Vec<(Event, Profile)> = Vec::new();

    for obj in js_events.drain(..) {
        let obj = obj.downcast::<JsObject>().or_throw(cx)?;

        let event = obj.get(cx, "event")?.downcast::<JsObject>().or_throw(cx)?;
        let event = parse_event(cx, *event)?;

        let profile: Profile = match obj.get(cx, "profile") {
            Ok(p) => {
                if let Ok(p) = p.downcast::<JsObject>() {
                    parse_profile(cx, *p)?
                } else {
                    Profile {
                        display_name: None,
                        avatar_url: None,
                    }
                }
            }
            Err(_e) => Profile {
                display_name: None,
                avatar_url: None,
            },
        };

        events.push((event, profile));
    }

    let receiver = {
        let this = cx.this();
        let guard = cx.lock();
        let db = &this.borrow(&guard).0;
        db.add_backlog_events(events, new_checkpoint, old_checkpoint)
    };

    Ok(receiver)
}

fn search_result_to_js<'a, C: Context<'a>>(
    cx: &mut C,
    mut result: SearchResult,
) -> Result<Handle<'a, JsObject>, neon::result::Throw> {
    let rank = cx.number(f64::from(result.score));

    let source = serde_json::from_str(&result.event_source);
    let source: serde_json::Value = match source {
        Ok(s) => s,
        Err(e) => {
            return cx.throw_type_error(format!(
                "Couldn't load the event from the store: {}",
                e.to_string()
            ))
        }
    };

    let source = neon_serde::to_value(&mut *cx, &source)?;

    let object = JsObject::new(&mut *cx);
    let context = JsObject::new(&mut *cx);

    let before = JsArray::new(&mut *cx, result.events_before.len() as u32);
    let after = JsArray::new(&mut *cx, result.events_after.len() as u32);
    let profile_info = JsObject::new(&mut *cx);

    for (i, event) in result.events_before.iter().enumerate() {
        let js_event = serde_json::from_str(event);
        let js_event: serde_json::Value = match js_event {
            Ok(e) => e,
            Err(_) => continue,
        };
        let js_event = neon_serde::to_value(&mut *cx, &js_event)?;
        before.set(&mut *cx, i as u32, js_event)?;
    }

    for (i, event) in result.events_after.iter().enumerate() {
        let js_event = serde_json::from_str(event);
        let js_event: serde_json::Value = match js_event {
            Ok(e) => e,
            Err(_) => continue,
        };
        let js_event = neon_serde::to_value(&mut *cx, &js_event)?;
        after.set(&mut *cx, i as u32, js_event)?;
    }

    for (sender, profile) in result.profile_info.drain() {
        let (js_sender, js_profile) = profile_to_js(cx, sender, profile)?;
        profile_info.set(&mut *cx, js_sender, js_profile)?;
    }

    context.set(&mut *cx, "events_before", before)?;
    context.set(&mut *cx, "events_after", after)?;
    context.set(&mut *cx, "profile_info", profile_info)?;

    object.set(&mut *cx, "rank", rank)?;
    object.set(&mut *cx, "result", source)?;
    object.set(&mut *cx, "context", context)?;

    Ok(object)
}

fn profile_to_js<'a, C: Context<'a>>(
    cx: &mut C,
    sender: String,
    profile: Profile,
) -> Result<(Handle<'a, JsString>, Handle<'a, JsObject>), neon::result::Throw> {
    let js_profile = JsObject::new(&mut *cx);

    let js_sender = JsString::new(&mut *cx, sender);

    match profile.display_name {
        Some(name) => {
            let js_name = JsString::new(&mut *cx, name);
            js_profile.set(&mut *cx, "display_name", js_name)?;
        }
        None => {
            js_profile.set(&mut *cx, "display_name", JsNull::new())?;
        }
    };

    match profile.avatar_url {
        Some(avatar) => {
            let js_avatar = JsString::new(&mut *cx, avatar);
            js_profile.set(&mut *cx, "avatar_url", js_avatar)?;
        }
        None => {
            js_profile.set(&mut *cx, "avatar_url", JsNull::new())?;
        }
    }

    Ok((js_sender, js_profile))
}

fn parse_event(
    cx: &mut CallContext<Seshat>,
    event: JsObject,
) -> Result<Event, neon::result::Throw> {
    let sender: String = event
        .get(&mut *cx, "sender")?
        .downcast::<JsString>()
        .or_else(|_| cx.throw_type_error("Event doesn't contain a valid sender"))?
        .value();

    let event_id: String = event
        .get(&mut *cx, "event_id")?
        .downcast::<JsString>()
        .or_else(|_| cx.throw_type_error("Event doesn't contain a valid event id"))?
        .value();

    let server_timestamp: i64 = event
        .get(&mut *cx, "origin_server_ts")?
        .downcast::<JsNumber>()
        .or_else(|_| cx.throw_type_error("Event doesn't contain a valid timestamp"))?
        .value() as i64;

    let room_id: String = event
        .get(&mut *cx, "room_id")?
        .downcast::<JsString>()
        .or_else(|_| cx.throw_type_error("Event doesn't contain a valid room id"))?
        .value();

    let content = event
        .get(&mut *cx, "content")?
        .downcast::<JsObject>()
        .or_else(|_| cx.throw_type_error("Event doesn't contain any content"))?;

    let event_type: EventType = event
        .get(&mut *cx, "type")?
        .downcast::<JsString>()
        .or_else(|_| cx.throw_type_error("Event doesn't contain a valid type"))?
        .value()
        .into();

    let content_value = match event_type {
        EventType::Message => content
            .get(&mut *cx, "body")?
            .downcast::<JsString>()
            .or_else(|_| cx.throw_type_error("Event doesn't contain a valid body"))?
            .value(),

        EventType::Topic => content
            .get(&mut *cx, "topic")?
            .downcast::<JsString>()
            .or_else(|_| cx.throw_type_error("Event doesn't contain a valid topic"))?
            .value(),

        EventType::Name => content
            .get(&mut *cx, "topic")?
            .downcast::<JsString>()
            .or_else(|_| cx.throw_type_error("Event doesn't contain a valid name"))?
            .value(),
        _ => return cx.throw_type_error("Unsuported event type"),
    };

    let event_value = event.as_value(&mut *cx);
    let event_source: serde_json::Value = neon_serde::from_value(&mut *cx, event_value)?;
    let event_source: String = serde_json::to_string(&event_source)
        .or_else(|e| cx.throw_type_error(format!("Cannot serialize event {}", e)))?;

    Ok(Event {
        event_type,
        content_value,
        event_id,
        sender,
        server_ts: server_timestamp,
        room_id,
        source: event_source,
    })
}

fn parse_profile(
    cx: &mut CallContext<Seshat>,
    profile: JsObject,
) -> Result<Profile, neon::result::Throw> {
    let display_name: Option<String> = match profile
        .get(&mut *cx, "display_name")?
        .downcast::<JsString>()
    {
        Ok(s) => Some(s.value()),
        Err(_e) => None,
    };

    let avatar_url: Option<String> =
        match profile.get(&mut *cx, "avatar_url")?.downcast::<JsString>() {
            Ok(s) => Some(s.value()),
            Err(_e) => None,
        };

    Ok(Profile {
        display_name,
        avatar_url,
    })
}

fn js_checkpoint_to_rust(
    cx: &mut CallContext<Seshat>,
    object: JsObject,
) -> Result<BacklogCheckpoint, neon::result::Throw> {
    let room_id = object
        .get(&mut *cx, "room_id")?
        .downcast::<JsString>()
        .or_throw(&mut *cx)?
        .value();
    let token = object
        .get(&mut *cx, "token")?
        .downcast::<JsString>()
        .or_throw(&mut *cx)?
        .value();
    let full_crawl: bool = object
        .get(&mut *cx, "full_crawl")?
        .downcast::<JsBoolean>()
        .unwrap_or_else(|_| JsBoolean::new(&mut *cx, false))
        .value();

    Ok(BacklogCheckpoint {
        room_id,
        token,
        full_crawl,
    })
}

register_module!(mut cx, { cx.export_class::<Seshat>("Seshat") });
