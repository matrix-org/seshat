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

use crate::Seshat;
use neon::prelude::*;
use seshat::{
    CheckpointDirection, Config, CrawlerCheckpoint, Event, EventType, Language, Profile, Receiver,
    SearchConfig, SearchResult,
};
use std::cell::RefCell;
use uuid::Uuid;

pub(crate) fn parse_database_config(
    cx: &mut FunctionContext,
    argument: Option<Handle<JsValue>>,
) -> Result<Config, neon::result::Throw> {
    let mut config = Config::new();

    if let Some(c) = argument {
        let c = c.downcast::<JsObject, _>(cx).or_throw(&mut *cx)?;

        if let Ok(l) = c.get(&mut *cx, "language") {
            if let Ok(l) = l.downcast::<JsString, _>(cx) {
                let language = Language::from(l.value(cx).as_ref());
                match language {
                    Language::Unknown => {
                        let value = l.value(cx);
                        return cx.throw_type_error(format!("Unsuported language: {}", value));
                    }
                    _ => {
                        config = config.set_language(&language);
                    }
                }
            }
        }

        if let Ok(p) = c.get(&mut *cx, "passphrase") {
            if let Ok(p) = p.downcast::<JsString, _>(cx) {
                let passphrase: String = p.value(cx);
                config = config.set_passphrase(passphrase);
            }
        }
    }

    Ok(config)
}

pub(crate) fn parse_search_object(
    cx: &mut FunctionContext,
    argument: Handle<JsObject>,
) -> Result<(String, SearchConfig), neon::result::Throw> {
    let term = argument
        .get(&mut *cx, "search_term")?
        .downcast::<JsString, _>(cx)
        .or_throw(&mut *cx)?
        .value(cx);

    let mut config = SearchConfig::new();

    if let Ok(v) = argument.get(&mut *cx, "limit") {
        if let Ok(v) = v.downcast::<JsNumber, _>(cx) {
            config.limit(v.value(cx) as usize);
        }
    }

    if let Ok(v) = argument.get(&mut *cx, "before_limit") {
        if let Ok(v) = v.downcast::<JsNumber, _>(cx) {
            config.before_limit(v.value(cx) as usize);
        }
    }

    if let Ok(v) = argument.get(&mut *cx, "after_limit") {
        if let Ok(v) = v.downcast::<JsNumber, _>(cx) {
            config.after_limit(v.value(cx) as usize);
        }
    }

    if let Ok(v) = argument.get(&mut *cx, "order_by_recency") {
        if let Ok(v) = v.downcast::<JsBoolean, _>(cx) {
            config.order_by_recency(v.value(cx));
        }
    }

    if let Ok(r) = argument.get(&mut *cx, "room_id") {
        if let Ok(r) = r.downcast::<JsString, _>(cx) {
            config.for_room(&r.value(cx));
        }
    }

    if let Ok(t) = argument.get(&mut *cx, "next_batch") {
        if let Ok(t) = t.downcast::<JsString, _>(cx) {
            let token = if let Ok(t) = Uuid::parse_str(&t.value(cx)) {
                t
            } else {
                let value = t.value(cx);
                return cx.throw_type_error(format!("Invalid next_batch token {}", value));
            };

            config.next_batch(token);
        }
    }

    if let Ok(k) = argument.get(&mut *cx, "keys") {
        if let Ok(k) = k.downcast::<JsArray, _>(cx) {
            let mut keys: Vec<Handle<JsValue>> = k.to_vec(&mut *cx)?;

            for key in keys.drain(..) {
                let key = key
                    .downcast::<JsString, _>(cx)
                    .or_throw(&mut *cx)?
                    .value(cx);
                match key.as_ref() {
                    "content.body" => config.with_key(EventType::Message),
                    "content.topic" => config.with_key(EventType::Topic),
                    "content.name" => config.with_key(EventType::Name),
                    _ => return cx.throw_type_error(format!("Invalid search key {}", key)),
                };
            }
        }
    }

    Ok((term, config))
}

pub(crate) fn parse_checkpoint(
    cx: &mut FunctionContext,
    argument: Option<Handle<JsValue>>,
) -> Result<Option<CrawlerCheckpoint>, neon::result::Throw> {
    match argument {
        Some(c) => match c.downcast::<JsObject, _>(cx) {
            Ok(object) => Ok(Some(js_checkpoint_to_rust(cx, *object)?)),
            Err(_e) => {
                let _o = c.downcast::<JsNull, _>(cx).or_throw(cx)?;
                Ok(None)
            }
        },
        None => Ok(None),
    }
}

pub(crate) fn add_historic_events_helper(
    cx: &mut FunctionContext,
) -> Result<Receiver<seshat::Result<bool>>, neon::result::Throw> {
    let js_events = cx.argument::<JsArray>(1)?;
    let mut js_events: Vec<Handle<JsValue>> = js_events.to_vec(cx)?;

    let js_checkpoint = cx.argument_opt(2);
    let new_checkpoint: Option<CrawlerCheckpoint> = parse_checkpoint(cx, js_checkpoint)?;

    let js_checkpoint = cx.argument_opt(3);
    let old_checkpoint: Option<CrawlerCheckpoint> = parse_checkpoint(cx, js_checkpoint)?;

    let mut events: Vec<(Event, Profile)> = Vec::new();

    for obj in js_events.drain(..) {
        let obj = obj.downcast::<JsObject, _>(cx).or_throw(cx)?;

        let event = obj
            .get(cx, "event")?
            .downcast::<JsObject, _>(cx)
            .or_throw(cx)?;
        let event = parse_event(cx, *event)?;

        let profile: Profile = match obj.get(cx, "profile") {
            Ok(p) => {
                if let Ok(p) = p.downcast::<JsObject, _>(cx) {
                    parse_profile(cx, *p)?
                } else {
                    Profile {
                        displayname: None,
                        avatar_url: None,
                    }
                }
            }
            Err(_e) => Profile {
                displayname: None,
                avatar_url: None,
            },
        };

        events.push((event, profile));
    }

    let receiver = {
        let this = cx.argument::<JsBox<RefCell<Seshat>>>(0)?;
        let db = &this.borrow_mut().database;
        db.as_ref().map_or_else(
            || Err("Database has been deleted"),
            |db| Ok(db.add_historic_events(events, new_checkpoint, old_checkpoint)),
        )
    };

    let receiver = match receiver {
        Ok(r) => r,
        Err(e) => return cx.throw_type_error(e),
    };

    Ok(receiver)
}

pub(crate) fn deserialize_event<'a, C: Context<'a>>(
    cx: &mut C,
    source: &str,
) -> Result<Handle<'a, JsValue>, neon::result::Throw> {
    let source = serde_json::from_str(source);
    let source: serde_json::Value = match source {
        Ok(s) => s,
        Err(e) => {
            return cx.throw_type_error(format!(
                "Couldn't load the event from the store: {}",
                e.to_string()
            ))
        }
    };

    let ret = match neon_serde::to_value(&mut *cx, &source) {
        Ok(v) => v,
        Err(e) => return cx.throw_error::<_, _>(e.to_string()),
    };

    Ok(ret)
}

pub(crate) fn search_result_to_js<'a, C: Context<'a>>(
    cx: &mut C,
    mut result: SearchResult,
) -> Result<Handle<'a, JsObject>, neon::result::Throw> {
    let rank = cx.number(f64::from(result.score));

    let event = deserialize_event(cx, &result.event_source)?;

    let object = cx.empty_object();
    let context = cx.empty_object();

    let before = JsArray::new(cx, result.events_before.len() as u32);
    let after = JsArray::new(cx, result.events_after.len() as u32);
    let profile_info = cx.empty_object();

    for (i, event) in result.events_before.iter().enumerate() {
        let js_event = serde_json::from_str(event);
        let js_event: serde_json::Value = match js_event {
            Ok(e) => e,
            Err(_) => continue,
        };
        let js_event = match neon_serde::to_value(&mut *cx, &js_event) {
            Ok(v) => v,
            Err(e) => return cx.throw_error::<_, _>(e.to_string()),
        };
        before.set(&mut *cx, i as u32, js_event)?;
    }

    for (i, event) in result.events_after.iter().enumerate() {
        let js_event = serde_json::from_str(event);
        let js_event: serde_json::Value = match js_event {
            Ok(e) => e,
            Err(_) => continue,
        };
        let js_event = match neon_serde::to_value(&mut *cx, &js_event) {
            Ok(v) => v,
            Err(e) => return cx.throw_error::<_, _>(e.to_string()),
        };
        after.set(&mut *cx, i as u32, js_event)?;
    }

    for (sender, profile) in result.profile_info.drain() {
        let (js_sender, js_profile) = sender_and_profile_to_js(cx, sender, profile)?;
        profile_info.set(&mut *cx, js_sender, js_profile)?;
    }

    context.set(&mut *cx, "events_before", before)?;
    context.set(&mut *cx, "events_after", after)?;
    context.set(&mut *cx, "profile_info", profile_info)?;

    object.set(&mut *cx, "rank", rank)?;
    object.set(&mut *cx, "result", event)?;
    object.set(&mut *cx, "context", context)?;

    Ok(object)
}

pub(crate) fn profile_to_js<'a, C: Context<'a>>(
    cx: &mut C,
    profile: Profile,
) -> Result<Handle<'a, JsObject>, neon::result::Throw> {
    let js_profile = cx.empty_object();

    match profile.displayname {
        Some(name) => {
            let js_name = cx.string(name);
            js_profile.set(&mut *cx, "displayname", js_name)?;
        }
        None => {
            let null = cx.null();
            js_profile.set(&mut *cx, "displayname", null)?;
        }
    }

    match profile.avatar_url {
        Some(avatar) => {
            let js_avatar = cx.string(avatar);
            js_profile.set(&mut *cx, "avatar_url", js_avatar)?;
        }
        None => {
            let null = cx.null();
            js_profile.set(&mut *cx, "avatar_url", null)?;
        }
    }

    Ok(js_profile)
}

pub(crate) fn sender_and_profile_to_js<'a, C: Context<'a>>(
    cx: &mut C,
    sender: String,
    profile: Profile,
) -> Result<(Handle<'a, JsString>, Handle<'a, JsObject>), neon::result::Throw> {
    let js_sender = cx.string(sender);
    let js_profile = profile_to_js(cx, profile)?;

    Ok((js_sender, js_profile))
}

pub(crate) fn parse_event(
    cx: &mut FunctionContext,
    event: JsObject,
) -> Result<Event, neon::result::Throw> {
    let sender: String = event
        .get(&mut *cx, "sender")?
        .downcast::<JsString, _>(cx)
        .or_else(|_| cx.throw_type_error("Event doesn't contain a valid sender"))?
        .value(cx);

    let event_id: String = event
        .get(&mut *cx, "event_id")?
        .downcast::<JsString, _>(cx)
        .or_else(|_| cx.throw_type_error("Event doesn't contain a valid event id"))?
        .value(cx);

    let server_timestamp: i64 = event
        .get(&mut *cx, "origin_server_ts")?
        .downcast::<JsNumber, _>(cx)
        .or_else(|_| cx.throw_type_error("Event doesn't contain a valid timestamp"))?
        .value(cx) as i64;

    let room_id: String = event
        .get(&mut *cx, "room_id")?
        .downcast::<JsString, _>(cx)
        .or_else(|_| cx.throw_type_error("Event doesn't contain a valid room id"))?
        .value(cx);

    let content = event
        .get(&mut *cx, "content")?
        .downcast::<JsObject, _>(cx)
        .or_else(|_| cx.throw_type_error("Event doesn't contain any content"))?;

    let event_type = event
        .get(&mut *cx, "type")?
        .downcast::<JsString, _>(cx)
        .or_else(|_| cx.throw_type_error("Event doesn't contain a valid type"))?
        .value(cx);

    let event_type: EventType = match event_type.as_ref() {
        "m.room.message" => EventType::Message,
        "m.room.name" => EventType::Name,
        "m.room.topic" => EventType::Topic,
        _ => return cx.throw_type_error("Unsuported event type"),
    };

    let content_value = match event_type {
        EventType::Message => content
            .get(&mut *cx, "body")?
            .downcast::<JsString, _>(cx)
            .or_else(|_| cx.throw_type_error("Event doesn't contain a valid body"))?
            .value(cx),

        EventType::Topic => content
            .get(&mut *cx, "topic")?
            .downcast::<JsString, _>(cx)
            .or_else(|_| cx.throw_type_error("Event doesn't contain a valid topic"))?
            .value(cx),

        EventType::Name => content
            .get(&mut *cx, "name")?
            .downcast::<JsString, _>(cx)
            .or_else(|_| cx.throw_type_error("Event doesn't contain a valid name"))?
            .value(cx),
    };

    let msgtype = match event_type {
        EventType::Message => Some(
            content
                .get(&mut *cx, "msgtype")?
                .downcast::<JsString, _>(cx)
                .or_else(|_| {
                    cx.throw_type_error("m.room.message event doesn't contain a valid msgtype")
                })?
                .value(cx),
        ),
        _ => None,
    };

    let event_value = event.as_value(&mut *cx);
    let event_source: serde_json::Value = match neon_serde::from_value(&mut *cx, event_value) {
        Ok(v) => v,
        Err(e) => return cx.throw_error::<_, _>(e.to_string()),
    };
    let event_source: String = serde_json::to_string(&event_source)
        .or_else(|e| cx.throw_type_error(format!("Cannot serialize event {}", e)))?;

    Ok(Event {
        event_type,
        content_value,
        msgtype,
        event_id,
        sender,
        server_ts: server_timestamp,
        room_id,
        source: event_source,
    })
}

pub(crate) fn parse_profile(
    cx: &mut FunctionContext,
    profile: JsObject,
) -> Result<Profile, neon::result::Throw> {
    let displayname: Option<String> = match profile
        .get(&mut *cx, "displayname")?
        .downcast::<JsString, _>(cx)
    {
        Ok(s) => Some(s.value(cx)),
        Err(_e) => None,
    };

    let avatar_url: Option<String> = match profile
        .get(&mut *cx, "avatar_url")?
        .downcast::<JsString, _>(cx)
    {
        Ok(s) => Some(s.value(cx)),
        Err(_e) => None,
    };

    Ok(Profile {
        displayname,
        avatar_url,
    })
}

pub(crate) fn js_checkpoint_to_rust(
    cx: &mut FunctionContext,
    object: JsObject,
) -> Result<CrawlerCheckpoint, neon::result::Throw> {
    let room_id = object
        .get(&mut *cx, "roomId")?
        .downcast::<JsString, _>(cx)
        .or_throw(&mut *cx)?
        .value(cx);
    let token = object
        .get(&mut *cx, "token")?
        .downcast::<JsString, _>(cx)
        .or_throw(&mut *cx)?
        .value(cx);
    let full_crawl: bool = object
        .get(&mut *cx, "fullCrawl")?
        .downcast::<JsBoolean, _>(cx)
        .unwrap_or_else(|_| cx.boolean(false))
        .value(cx);
    let direction = object
        .get(&mut *cx, "direction")?
        .downcast::<JsString, _>(cx)
        .unwrap_or_else(|_| cx.string(""))
        .value(cx);

    let direction = match direction.to_lowercase().as_ref() {
        "backwards" | "backward" | "b" => CheckpointDirection::Backwards,
        "forwards" | "forward" | "f" => CheckpointDirection::Forwards,
        "" => CheckpointDirection::Backwards,
        d => return cx.throw_error(format!("Unknown checkpoint direction {}", d)),
    };

    Ok(CrawlerCheckpoint {
        room_id,
        token,
        full_crawl,
        direction,
    })
}
