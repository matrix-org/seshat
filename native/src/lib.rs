#[macro_use]
extern crate neon;
extern crate seshat;
extern crate neon_serde;
extern crate serde_json;

use neon::prelude::*;
use seshat::{Database, Event, Profile};

pub struct SeshatDatabase(Database);

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

        method add_event(mut cx) {
            let event = cx.argument::<JsObject>(0)?;

            let sender: String = event
                .get(&mut cx, "sender")?
                .downcast::<JsString>()
                .or_else(|_| cx.throw_type_error("Event doesn't contain a valid sender"))?
                .value();

            let event_id: String = event
                .get(&mut cx, "event_id")?
                .downcast::<JsString>()
                .or_else(|_| cx.throw_type_error("Event doesn't contain a valid event id"))?
                .value();

            let server_timestamp: i64 = event
                .get(&mut cx, "origin_server_ts")?
                .downcast::<JsNumber>()
                .or_else(|_| cx.throw_type_error("Event doesn't contain a valid timestamp"))?
                .value() as i64;

            let room_id: String = event
                .get(&mut cx, "room_id")?
                .downcast::<JsString>()
                .or_else(|_| cx.throw_type_error("Event doesn't contain a valid room id"))?
                .value();

            let content = event
                .get(&mut cx, "content")?
                .downcast::<JsObject>()
                .or_else(|_| cx.throw_type_error("Event doesn't contain any content"))?;

            let event_value = event.as_value(&mut cx);
            let event_source: serde_json::Value = neon_serde::from_value(&mut cx, event_value)?;
            let event_source: String = serde_json::to_string(&event_source)
                .or_else(|e| cx.throw_type_error(format!("Cannot serialize event {}", e)))?;

            let event = Event {
                body: "Test message".to_string(),
                event_id,
                sender,
                server_ts: server_timestamp,
                room_id,
                source: event_source
            };

            let profile = Profile::new("@alice", "");

            let this = cx.this();

            {
                let guard = cx.lock();
                let db = &this.borrow(&guard).0;
                db.add_event(event, profile);
            }

            Ok(cx.undefined().upcast())
        }

        method commit(mut cx) {
            let wait: bool = cx.argument::<JsBoolean>(0)?.value();
            let mut this = cx.this();

            {
                let guard = cx.lock();
                let db = &mut this.borrow_mut(&guard).0;

                if wait {
                    db.commit();
                } else {
                    db.commit_no_wait();
                }
            }
            Ok(cx.undefined().upcast())
        }
    }
}

register_module!(mut cx, {
    cx.export_class::<Seshat>("Seshat")
});
