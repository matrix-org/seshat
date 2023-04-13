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

        if let Some(l) = c.get_opt::<JsString, _, _>(&mut *cx, "language")? {
            let language = Language::from(l.value(cx).as_ref());

            match language {
                Language::Unknown => {
                    let value = l.value(cx);
                    return cx.throw_type_error(format!("Unsupported language: {}", value));
                }
                _ => {
                    config = config.set_language(&language);
                }
            }
        }

        if let Some(p) = c.get_opt::<JsString, _, _>(&mut *cx, "passphrase")? {
            let passphrase: String = p.value(cx);
            config = config.set_passphrase(passphrase);
        }
    }

    Ok(config)
}

pub(crate) fn parse_search_object(
    cx: &mut FunctionContext,
    argument: Handle<JsObject>,
) -> Result<(String, SearchConfig), neon::result::Throw> {
    let term = argument
        .get::<JsString, _, _>(&mut *cx, "search_term")?
        .value(cx);

    let mut config = SearchConfig::new();

    if let Some(v) = argument.get_opt::<JsNumber, _, _>(&mut *cx, "limit")? {
        config.limit(v.value(cx) as usize);
    }

    if let Some(v) = argument.get_opt::<JsNumber, _, _>(&mut *cx, "before_limit")? {
        config.before_limit(v.value(cx) as usize);
    }

    if let Some(v) = argument.get_opt::<JsNumber, _, _>(&mut *cx, "after_limit")? {
        config.after_limit(v.value(cx) as usize);
    }

    if let Some(v) = argument.get_opt::<JsBoolean, _, _>(&mut *cx, "order_by_recency")? {
        config.order_by_recency(v.value(cx));
    }

    if let Some(r) = argument.get_opt::<JsString, _, _>(&mut *cx, "room_id")? {
        config.for_room(&r.value(cx));
    }

    if let Some(t) = argument.get_opt::<JsString, _, _>(&mut *cx, "next_batch")? {
        let token = if let Ok(t) = Uuid::parse_str(&t.value(cx)) {
            t
        } else {
            let value = t.value(cx);
            return cx.throw_type_error(format!("Invalid next_batch token {}", value));
        };

        config.next_batch(token);
    }

    if let Some(k) = argument.get_opt::<JsArray, _, _>(&mut *cx, "keys")? {
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

    Ok((term, config))
}

pub(crate) fn parse_checkpoint(
    cx: &mut FunctionContext,
    argument: Option<Handle<JsValue>>,
) -> Result<Option<CrawlerCheckpoint>, neon::result::Throw> {
    match argument {
        Some(c) => match c.downcast::<JsObject, _>(cx) {
            Ok(object) => Ok(Some(js_checkpoint_to_rust(cx, object)?)),
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

        let event = obj.get::<JsObject, _, _>(cx, "event")?;
        let event = parse_event(cx, event)?;

        let profile: Profile = match obj.get_opt::<JsObject, _, _>(cx, "profile")? {
            Some(p) => parse_profile(cx, p)?,
            None => Profile {
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

    let ret = match neon_serde3::to_value(&mut *cx, &source) {
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
        let js_event = match neon_serde3::to_value(&mut *cx, &js_event) {
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
        let js_event = match neon_serde3::to_value(&mut *cx, &js_event) {
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
    event: Handle<JsObject>,
) -> Result<Event, neon::result::Throw> {
    let get_string = |cx: &mut FunctionContext, event: Handle<JsObject>, key: &str, error: &str| {
        Ok(event
            .get_value(&mut *cx, key)
            .and_then(|v| {
                v.downcast::<JsString, _>(cx)
                    .or_else(|_| cx.throw_type_error(error))
            })?
            .value(cx))
    };

    let sender = get_string(cx, event, "sender", "Event doesn't contain a valid sender")?;
    let event_id = get_string(
        cx,
        event,
        "event_id",
        "Event doesn't contain a valid event id",
    )?;
    let server_timestamp = event
        .get_value(&mut *cx, "origin_server_ts")
        .and_then(|v| {
            v.downcast::<JsNumber, _>(cx)
                .or_else(|_| cx.throw_type_error("Event doesn't contain a valid timestamp"))
        })?
        .value(cx) as i64;

    let room_id = get_string(
        cx,
        event,
        "room_id",
        "Event doesn't contain a valid room id",
    )?;

    let content = event.get_value(&mut *cx, "content").and_then(|v| {
        v.downcast::<JsObject, _>(cx)
            .or_else(|_| cx.throw_type_error("Event doesn't contain any content"))
    })?;
    let event_type = get_string(cx, event, "type", "Event doesn't contain a valid type")?;

    let event_type: EventType = match event_type.as_ref() {
        "m.room.message" => EventType::Message,
        "m.room.name" => EventType::Name,
        "m.room.topic" => EventType::Topic,
        e => return cx.throw_type_error(format!("Unsupported event type {e}")),
    };

    let key = match event_type {
        EventType::Message => "body",
        EventType::Topic => "topic",
        EventType::Name => "name",
    };

    let content_value = get_string(
        cx,
        content,
        key,
        &format!("Event deosn't contain a valid {key}"),
    )?;

    let msgtype = match event_type {
        EventType::Message => Some(get_string(
            cx,
            content,
            "msgtype",
            "m.room.message event doesn't contain a valid msgtype",
        )?),
        _ => None,
    };

    let event_value = event.as_value(&mut *cx);
    let event_source: serde_json::Value = match neon_serde3::from_value(&mut *cx, event_value) {
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
    profile: Handle<JsObject>,
) -> Result<Profile, neon::result::Throw> {
    let get_optional_string =
        |cx: &mut FunctionContext, value: Handle<'_, JsValue>, error: &str| {
            if value.is_a::<JsUndefined, _>(cx) {
                Ok(None)
            } else {
                Ok(Some(
                    value
                        .downcast::<JsString, _>(cx)
                        .or_else(|_| cx.throw_type_error(error))?
                        .value(cx),
                ))
            }
        };

    let displayname: Option<String> = profile
        .get_value(cx, "displayname")
        .and_then(|v| get_optional_string(cx, v, "Event has an invalid display name"))?;

    let avatar_url: Option<String> = profile
        .get_value(&mut *cx, "avatar_url")
        .and_then(|v| get_optional_string(cx, v, "Event has an invalid avatar URL"))?;

    Ok(Profile {
        displayname,
        avatar_url,
    })
}

pub(crate) fn js_checkpoint_to_rust(
    cx: &mut FunctionContext,
    object: Handle<JsObject>,
) -> Result<CrawlerCheckpoint, neon::result::Throw> {
    let room_id = object.get::<JsString, _, _>(&mut *cx, "roomId")?.value(cx);
    let token = object.get::<JsString, _, _>(&mut *cx, "token")?.value(cx);
    let full_crawl: bool = object
        .get_opt::<JsBoolean, _, _>(&mut *cx, "fullCrawl")?
        .unwrap_or_else(|| cx.boolean(false))
        .value(cx);
    let direction = object
        .get_opt::<JsString, _, _>(&mut *cx, "direction")?
        .unwrap_or_else(|| cx.string(""))
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
