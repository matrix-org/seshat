#[macro_use]
extern crate neon;
extern crate seshat;

use neon::prelude::*;
use seshat::Database;

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

            let sender = event
                .get(&mut cx, "sender")?
                .downcast::<JsString>()
                .or_else(|_| cx.throw_type_error("Event doesn't contain a valid sender"))?
				.value();

			let event_id = event
                .get(&mut cx, "event_id")?
                .downcast::<JsString>()
                .or_else(|_| cx.throw_type_error("Event doesn't contain a valid event id"))?
				.value();


            println!("Event sender is {}", sender);
            // let profile = cx.argument::<JsObject>(0)?;

            Ok(cx.undefined().upcast())
        }
    }
}

register_module!(mut cx, {
    cx.export_class::<Seshat>("Seshat")
});
