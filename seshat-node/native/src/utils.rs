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
use neon_serde;
use serde_json;
use seshat::{
    CheckpointDirection, CrawlerCheckpoint, Event, EventType, Profile, Receiver, SearchConfig,
    SearchResult,
};

pub(crate) fn parse_search_object(
    cx: &mut CallContext<Seshat>,
    argument: Handle<JsObject>,
) -> Result<(String, SearchConfig), neon::result::Throw> {
    let term = argument
        .get(&mut *cx, "search_term")?
        .downcast::<JsString>()
        .or_throw(&mut *cx)?
        .value();

    let mut config = SearchConfig::new();

    if let Ok(v) = argument.get(&mut *cx, "limit") {
        if let Ok(v) = v.downcast::<JsNumber>() {
            config.limit(v.value() as usize);
        }
    }

    if let Ok(v) = argument.get(&mut *cx, "before_limit") {
        if let Ok(v) = v.downcast::<JsNumber>() {
            config.before_limit(v.value() as usize);
        }
    }

    if let Ok(v) = argument.get(&mut *cx, "after_limit") {
        if let Ok(v) = v.downcast::<JsNumber>() {
            config.after_limit(v.value() as usize);
        }
    }

    if let Ok(v) = argument.get(&mut *cx, "order_by_recency") {
        if let Ok(v) = v.downcast::<JsBoolean>() {
            config.order_by_recency(v.value());
        }
    }

    if let Ok(r) = argument.get(&mut *cx, "room_id") {
        if let Ok(r) = r.downcast::<JsString>() {
            config.for_room(&r.value());
        }
    }

    if let Ok(k) = argument.get(&mut *cx, "keys") {
        if let Ok(k) = k.downcast::<JsArray>() {
            let mut keys: Vec<Handle<JsValue>> = k.to_vec(&mut *cx)?;

            for key in keys.drain(..) {
                let key = key.downcast::<JsString>().or_throw(&mut *cx)?.value();
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
    cx: &mut CallContext<Seshat>,
    argument: Option<Handle<JsValue>>,
) -> Result<Option<CrawlerCheckpoint>, neon::result::Throw> {
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

pub(crate) fn add_historic_events_helper(
    cx: &mut CallContext<Seshat>,
) -> Result<Receiver<seshat::Result<bool>>, neon::result::Throw> {
    let js_events = cx.argument::<JsArray>(0)?;
    let mut js_events: Vec<Handle<JsValue>> = js_events.to_vec(cx)?;

    let js_checkpoint = cx.argument_opt(1);
    let new_checkpoint: Option<CrawlerCheckpoint> = parse_checkpoint(cx, js_checkpoint)?;

    let js_checkpoint = cx.argument_opt(2);
    let old_checkpoint: Option<CrawlerCheckpoint> = parse_checkpoint(cx, js_checkpoint)?;

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
        let this = cx.this();
        let guard = cx.lock();
        let db = &this.borrow(&guard).0;
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

    let ret = neon_serde::to_value(&mut *cx, &source)?;
    Ok(ret)
}

pub(crate) fn search_result_to_js<'a, C: Context<'a>>(
    cx: &mut C,
    mut result: SearchResult,
) -> Result<Handle<'a, JsObject>, neon::result::Throw> {
    let rank = cx.number(f64::from(result.score));

    let event = deserialize_event(&mut *cx, &result.event_source)?;

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
    let js_profile = JsObject::new(&mut *cx);

    match profile.displayname {
        Some(name) => {
            let js_name = JsString::new(&mut *cx, name);
            js_profile.set(&mut *cx, "displayname", js_name)?;
        }
        None => {
            js_profile.set(&mut *cx, "displayname", JsNull::new())?;
        }
    }

    match profile.avatar_url {
        Some(avatar) => {
            let js_avatar = JsString::new(&mut *cx, avatar);
            js_profile.set(&mut *cx, "avatar_url", js_avatar)?;
        }
        None => {
            js_profile.set(&mut *cx, "avatar_url", JsNull::new())?;
        }
    }

    Ok(js_profile)
}

pub(crate) fn sender_and_profile_to_js<'a, C: Context<'a>>(
    cx: &mut C,
    sender: String,
    profile: Profile,
) -> Result<(Handle<'a, JsString>, Handle<'a, JsObject>), neon::result::Throw> {
    let js_sender = JsString::new(&mut *cx, sender);
    let js_profile = profile_to_js(cx, profile)?;

    Ok((js_sender, js_profile))
}

pub(crate) fn parse_event(
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

    let event_type = event
        .get(&mut *cx, "type")?
        .downcast::<JsString>()
        .or_else(|_| cx.throw_type_error("Event doesn't contain a valid type"))?
        .value();

    let event_type: EventType = match event_type.as_ref() {
        "m.room.message" => EventType::Message,
        "m.room.name" => EventType::Name,
        "m.room.topic" => EventType::Topic,
        _ => return cx.throw_type_error("Unsuported event type"),
    };

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
            .get(&mut *cx, "name")?
            .downcast::<JsString>()
            .or_else(|_| cx.throw_type_error("Event doesn't contain a valid name"))?
            .value(),
    };

    let msgtype = match event_type {
        EventType::Message => Some(
            content
                .get(&mut *cx, "msgtype")?
                .downcast::<JsString>()
                .or_else(|_| {
                    cx.throw_type_error("m.room.message event doesn't contain a valid msgtype")
                })?
                .value(),
        ),
        _ => None,
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
        msgtype,
    })
}

pub(crate) fn parse_profile(
    cx: &mut CallContext<Seshat>,
    profile: JsObject,
) -> Result<Profile, neon::result::Throw> {
    let displayname: Option<String> =
        match profile.get(&mut *cx, "displayname")?.downcast::<JsString>() {
            Ok(s) => Some(s.value()),
            Err(_e) => None,
        };

    let avatar_url: Option<String> =
        match profile.get(&mut *cx, "avatar_url")?.downcast::<JsString>() {
            Ok(s) => Some(s.value()),
            Err(_e) => None,
        };

    Ok(Profile {
        displayname,
        avatar_url,
    })
}

pub(crate) fn js_checkpoint_to_rust(
    cx: &mut CallContext<Seshat>,
    object: JsObject,
) -> Result<CrawlerCheckpoint, neon::result::Throw> {
    let room_id = object
        .get(&mut *cx, "roomId")?
        .downcast::<JsString>()
        .or_throw(&mut *cx)?
        .value();
    let token = object
        .get(&mut *cx, "token")?
        .downcast::<JsString>()
        .or_throw(&mut *cx)?
        .value();
    let full_crawl: bool = object
        .get(&mut *cx, "fullCrawl")?
        .downcast::<JsBoolean>()
        .unwrap_or_else(|_| JsBoolean::new(&mut *cx, false))
        .value();
    let direction = object
        .get(&mut *cx, "direction")?
        .downcast::<JsString>()
        .unwrap_or_else(|_| JsString::new(&mut *cx, ""))
        .value();

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
