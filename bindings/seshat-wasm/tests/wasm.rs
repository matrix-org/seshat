// #![recursion_limit = "256"]
// #![cfg(target_arch = "wasm32")]

wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use seshat_wasm::{new_seshat_db, Config};
use wasm_bindgen_test::*;

#[wasm_bindgen_test]
async fn test_create_db() {
    let config = Config {
        language: seshat_wasm::Language::English,
        passphrase: Some("pass".to_string()),
    };
    let db = new_seshat_db("./".to_string(), config).await;
    match db {
        Ok(_) => console_log!("db created"),
        Err(e) => console_log!("Failed to create db connection {e:?}"),
    }

    // let event = Event{
    //     event_type: EventType.Message,
    //      "message", "m.room.message", "1", "dave.blah", 9007199254740991n, "room123", "")
    // }
    // db.add_event(event, profile);
    assert!(true)
}
