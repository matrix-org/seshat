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

use neon::prelude::*;
use neon_serde;
use serde_json;
use seshat::{Database, Event, Profile, SearchResult, Searcher};

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
            .search(&self.term, self.before_limit, self.after_limit))
    }

    fn complete(
        self,
        mut cx: TaskContext,
        result: Result<Self::Output, Self::Error>,
    ) -> JsResult<Self::JsEvent> {
        let mut ret = result.unwrap();
        let results = JsArray::new(&mut cx, ret.len() as u32);

        for (i, element) in ret.drain(..).enumerate() {
            let object = search_result_to_js(&mut cx, element);
            results.set(&mut cx, i as u32, object).unwrap();
        }

        Ok(results.upcast())
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
            let before_limit: usize = cx.argument::<JsNumber>(1)?.value() as usize;
            let after_limit: usize = cx.argument::<JsNumber>(2)?.value() as usize;

            let mut this = cx.this();

            let mut ret = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;
                db.search(&term, before_limit, after_limit)
            };

            let results = JsArray::new(&mut cx, ret.len() as u32);

            for (i, element) in ret.drain(..).enumerate() {
                let object = search_result_to_js(&mut cx, element);
                results.set(&mut cx, i as u32, object).unwrap();
            }

            Ok(results.upcast())
        }

        method searchAsync(mut cx) {
            let term: String = cx.argument::<JsString>(0)?.value();
            let before_limit: usize = cx.argument::<JsNumber>(1)?.value() as usize;
            let after_limit: usize = cx.argument::<JsNumber>(2)?.value() as usize;

            let f = cx.argument::<JsFunction>(3)?;

            let mut this = cx.this();

            let searcher = {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;
                db.get_searcher()
            };

            let task = SearchTask {
                inner: searcher,
                term,
                before_limit,
                after_limit
            };
            task.schedule(f);

            Ok(cx.undefined().upcast())
        }
    }
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

register_module!(mut cx, { cx.export_class::<Seshat>("Seshat") });
