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
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Condvar, Mutex};
use std::sync::mpsc::Receiver;

use neon::prelude::*;
use neon_serde;
use serde_json;
use seshat::{Database, Connection, Event, Profile, SearchResult, Searcher, BacklogCheckpoint};

pub struct SeshatDatabase(Database);

struct CommitTask {
    last_opstamp: usize,
    cvar: Arc<(Mutex<AtomicUsize>, Condvar)>,
}

impl Task for CommitTask {
    type Output = usize;
    type Error = String;
    type JsEvent = JsNumber;

    fn perform(&self) -> Result<Self::Output, Self::Error> {
        Ok(Database::wait_for_commit(self.last_opstamp, &self.cvar))
    }

    fn complete(
        self,
        mut cx: TaskContext,
        result: Result<Self::Output, Self::Error>,
    ) -> JsResult<Self::JsEvent> {
        Ok(cx.number(result.unwrap() as f64))
    }
}

struct SearchTask {
    inner: Searcher,
    term: String,
    limit: usize,
    before_limit: usize,
    after_limit: usize,
}

impl Task for SearchTask {
    type Output = Vec<SearchResult>;
    type Error = String;
    type JsEvent = JsValue;

    fn perform(&self) -> Result<Self::Output, Self::Error> {
        Ok(self
            .inner
            .search(&self.term, self.limit, self.before_limit, self.after_limit))
    }

    fn complete(
        self,
        mut cx: TaskContext,
        result: Result<Self::Output, Self::Error>,
    ) -> JsResult<Self::JsEvent> {
        let mut ret = result.unwrap();

        let count = ret.len();
        let results = JsArray::new(&mut cx, count as u32);
        let count = JsNumber::new(&mut cx, count as f64);

        for (i, element) in ret.drain(..).enumerate() {
            let object = search_result_to_js(&mut cx, element);
            results.set(&mut cx, i as u32, object).unwrap();
        }

        let search_result = JsObject::new(&mut cx);
        let highlights = JsArray::new(&mut cx, 0);

        search_result.set(&mut cx, "count", count).unwrap();
        search_result.set(&mut cx, "results", results).unwrap();
        search_result.set(&mut cx, "highlights", highlights).unwrap();

        Ok(search_result.upcast())
    }
}

struct AddBacklogTask {
    receiver: Receiver<seshat::Result<()>>
}

impl Task for AddBacklogTask {
    type Output = ();
    type Error = seshat::Error;
    type JsEvent = JsValue;

    fn perform(&self) -> Result<Self::Output, Self::Error> {
        self.receiver.recv().unwrap()
    }

    fn complete(
        self,
        mut cx: TaskContext,
        result: Result<Self::Output, Self::Error>,
    ) -> JsResult<Self::JsEvent> {
        match result {
            Ok(_) => Ok(cx.undefined().upcast()),
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

            js_checkpoint.set(&mut cx, "room_id", room_id).unwrap();
            js_checkpoint.set(&mut cx, "token", token).unwrap();

            ret.set(&mut cx, i as u32, js_checkpoint).unwrap();
        }

        Ok(ret)
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
                Ok(_) => Ok(cx.undefined().upcast()),
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

            let (last_opstamp, cvar) = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;
                db.commit_get_cvar()
            };

            let task = CommitTask { last_opstamp, cvar };
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
                Some(r) => Ok(cx.number(r as f64).upcast()),
                None => Ok(cx.undefined().upcast())
            }
        }

        method searchSync(mut cx) {
            let term: String = cx.argument::<JsString>(0)?.value();
            let limit: usize = cx.argument::<JsNumber>(1)?.value() as usize;
            let before_limit: usize = cx.argument::<JsNumber>(2)?.value() as usize;
            let after_limit: usize = cx.argument::<JsNumber>(3)?.value() as usize;

            let mut this = cx.this();

            let mut ret = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;
                db.search(&term, limit, before_limit, after_limit)
            };

            let count = ret.len();
            let results = JsArray::new(&mut cx, count as u32);
            let count = JsNumber::new(&mut cx, count as f64);

            for (i, element) in ret.drain(..).enumerate() {
                let object = search_result_to_js(&mut cx, element);
                results.set(&mut cx, i as u32, object).unwrap();
            }

            let search_result = JsObject::new(&mut cx);
            let highlights = JsArray::new(&mut cx, 0);

            search_result.set(&mut cx, "count", count).unwrap();
            search_result.set(&mut cx, "results", results).unwrap();
            search_result.set(&mut cx, "highlights", highlights).unwrap();

            Ok(search_result.upcast())
        }

        method searchAsync(mut cx) {
            let term: String = cx.argument::<JsString>(0)?.value();
            let limit: usize = cx.argument::<JsNumber>(1)?.value() as usize;
            let before_limit: usize = cx.argument::<JsNumber>(2)?.value() as usize;
            let after_limit: usize = cx.argument::<JsNumber>(3)?.value() as usize;

            let f = cx.argument::<JsFunction>(4)?;

            let mut this = cx.this();

            let searcher = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;
                db.get_searcher()
            };

            let task = SearchTask {
                inner: searcher,
                term,
                limit,
                before_limit,
                after_limit
            };
            task.schedule(f);

            Ok(cx.undefined().upcast())
        }
    }
}

fn parse_checkpoint(cx: &mut CallContext<Seshat>, argument: Option<Handle<JsValue>>) -> Result<Option<BacklogCheckpoint>, neon::result::Throw> {
    match argument {
        Some(c) => {
            match c.downcast::<JsObject>() {
                Ok(object) => Ok(Some(js_checkpoint_to_rust(cx, *object)?)),
                Err(_e) => {
                    let _o = c.downcast::<JsNull>().or_throw(cx)?;
                    Ok(None)
                }
            }
        },
        None => Ok(None),
    }
}

fn add_backlog_events_helper(cx: &mut CallContext<Seshat>) -> Result<Receiver<seshat::Result<()>>, neon::result::Throw> {
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
        // TODO make the profile optional.
        let profile = obj.get(cx, "profile")?.downcast::<JsObject>().or_throw(cx)?;

        let event = parse_event(cx, *event)?;
        let profile = parse_profile(cx, *profile)?;

        events.push((event, profile));
    }

    let receiver = {
        let this = cx.this();
        let guard = cx.lock();
        let db = &this.borrow(&guard).0;
        // TODO remove this unwrap.
        db.add_backlog_events(events, new_checkpoint, old_checkpoint)
    };

    Ok(receiver)
}

fn search_result_to_js<'a, C: Context<'a>>(
    cx: &mut C,
    mut result: SearchResult,
) -> Handle<'a, JsObject> {
    let rank = cx.number(f64::from(result.score));

    // TODO handle these unwraps. While it is unlikely that deserialization will
    // fail since we control what gets inserted into the database and we
    // previously serialized the string that gets deserialized we don't want to
    // crash if someone else puts events into the database.

    let source: serde_json::Value = serde_json::from_str(&result.event_source).unwrap();
    let source = neon_serde::to_value(&mut *cx, &source).unwrap();

    let object = JsObject::new(&mut *cx);
    let context = JsObject::new(&mut *cx);

    let before = JsArray::new(&mut *cx, result.events_before.len() as u32);
    let after = JsArray::new(&mut *cx, result.events_after.len() as u32);
    let profile_info = JsObject::new(&mut *cx);

    for (i, event) in result.events_before.iter().enumerate() {
        let js_event: serde_json::Value = serde_json::from_str(event).unwrap();
        let js_event = neon_serde::to_value(&mut *cx, &js_event).unwrap();
        before.set(&mut *cx, i as u32, js_event).unwrap();
    }

    for (i, event) in result.events_after.iter().enumerate() {
        let js_event: serde_json::Value = serde_json::from_str(event).unwrap();
        let js_event = neon_serde::to_value(&mut *cx, &js_event).unwrap();
        after.set(&mut *cx, i as u32, js_event).unwrap();
    }

    for (sender, profile) in result.profile_info.drain() {
        let (js_sender, js_profile) = profile_to_js(cx, sender, profile);
        profile_info.set(&mut *cx, js_sender, js_profile).unwrap();
    }

    context.set(&mut *cx, "events_before", before).unwrap();
    context.set(&mut *cx, "events_after", after).unwrap();
    context.set(&mut *cx, "profile_info", profile_info).unwrap();

    object.set(&mut *cx, "rank", rank).unwrap();
    object.set(&mut *cx, "result", source).unwrap();
    object.set(&mut *cx, "context", context).unwrap();

    object
}

fn profile_to_js<'a, C: Context<'a>>(
    cx: &mut C,
    sender: String,
    profile: Profile,
) -> (Handle<'a, JsString>, Handle<'a, JsObject>) {
    let js_profile = JsObject::new(&mut *cx);

    let js_sender = JsString::new(&mut *cx, sender);

    match profile.display_name {
        Some(name) => {
            let js_name = JsString::new(&mut *cx, name);
            js_profile.set(&mut *cx, "display_name", js_name).unwrap();
        }
        None => {
            js_profile
                .set(&mut *cx, "display_name", JsNull::new())
                .unwrap();
        }
    };

    match profile.avatar_url {
        Some(avatar) => {
            let js_avatar = JsString::new(&mut *cx, avatar);
            js_profile.set(&mut *cx, "avatar_url", js_avatar).unwrap();
        }
        None => {
            js_profile
                .set(&mut *cx, "avatar_url", JsNull::new())
                .unwrap();
        }
    }

    (js_sender, js_profile)
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

    // TODO allow the name or topic to be stored as well.
    let body: String = content
        .get(&mut *cx, "body")?
        .downcast::<JsString>()
        .or_else(|_| cx.throw_type_error("Event doesn't contain a valid body"))?
        .value();

    let event_value = event.as_value(&mut *cx);
    let event_source: serde_json::Value = neon_serde::from_value(&mut *cx, event_value)?;
    let event_source: String = serde_json::to_string(&event_source)
        .or_else(|e| cx.throw_type_error(format!("Cannot serialize event {}", e)))?;

    Ok(Event {
        body,
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
    let room_id = object.get(&mut *cx, "room_id")?.downcast::<JsString>().or_throw(&mut *cx)?.value();
    let token = object.get(&mut *cx, "token")?.downcast::<JsString>().or_throw(&mut *cx)?.value();

    Ok(BacklogCheckpoint { room_id, token })
}

register_module!(mut cx, { cx.export_class::<Seshat>("Seshat") });
