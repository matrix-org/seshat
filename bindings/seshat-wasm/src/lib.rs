use seshat::{
    Database as SDatabase, Error, Event as SEvent, EventType as SEventType, Profile as SProfile,
};

use wasm_bindgen::{prelude::wasm_bindgen, JsError};
use wasm_bindgen_test::console_log;

#[wasm_bindgen]
#[derive(Debug, PartialEq, Clone)]
#[allow(missing_docs)]
pub enum Language {
    Arabic,
    Danish,
    Dutch,
    English,
    Finnish,
    French,
    German,
    Greek,
    Hungarian,
    Italian,
    Portuguese,
    Romanian,
    Russian,
    Spanish,
    Swedish,
    Tamil,
    Turkish,
    Unknown,
}

impl From<&Language> for seshat::Language {
    fn from(language: &Language) -> Self {
        match language {
            Language::Arabic => Self::Arabic,
            Language::Danish => Self::Danish,
            Language::Dutch => Self::Dutch,
            Language::English => Self::English,
            Language::Finnish => Self::Finnish,
            Language::French => Self::French,
            Language::German => Self::German,
            Language::Greek => Self::Greek,
            Language::Hungarian => Self::Hungarian,
            Language::Italian => Self::Italian,
            Language::Portuguese => Self::Portuguese,
            Language::Romanian => Self::Romanian,
            Language::Russian => Self::Russian,
            Language::Spanish => Self::Spanish,
            Language::Swedish => Self::Swedish,
            Language::Tamil => Self::Tamil,
            Language::Turkish => Self::Turkish,
            Language::Unknown => Self::Unknown,
        }
    }
}

#[derive(Clone)]
#[wasm_bindgen(getter_with_clone)]
/// Configuration for the seshat database.
pub struct Config {
    pub language: Language,
    pub passphrase: Option<String>,
}

#[wasm_bindgen]
impl Config {
    #[wasm_bindgen(constructor)]
    pub fn new(language: Language, passphrase: Option<String>) -> Self {
        Self {
            language: language,
            passphrase: passphrase,
        }
    }
}

impl From<&Config> for seshat::Config {
    fn from(config: &Config) -> Self {
        let mut s_config = Self::new().set_language(&seshat::Language::from(&config.language));
        if let Some(p) = &config.passphrase {
            s_config = s_config.set_passphrase(p.as_str());
        }
        return s_config;
    }
}

#[wasm_bindgen]
/// Configuration for the seshat database.
pub struct Database {
    inner: SDatabase,
}

#[wasm_bindgen]
#[derive(Clone)]
pub enum EventType {
    Message,
    Name,
    Topic,
}

impl From<&EventType> for SEventType {
    fn from(event_type: &EventType) -> Self {
        match event_type {
            EventType::Message => Self::Message,
            EventType::Name => Self::Name,
            EventType::Topic => Self::Topic,
        }
    }
}

#[derive(Clone)]
#[wasm_bindgen(getter_with_clone)]
pub struct Event {
    /// The type of the event.
    pub event_type: EventType,
    /// The textual representation of a message, this part of the event will be
    /// indexed.
    pub content_value: String,
    /// The type of the message if the event is of a m.room.message type.
    pub msgtype: Option<String>,
    /// The unique identifier of this event.
    pub event_id: String,
    /// The MXID of the user who sent this event.
    pub sender: String,
    /// Timestamp in milliseconds on the originating Homeserver when this event
    /// was sent.
    pub server_ts: i64,
    /// The ID of the room associated with this event.
    pub room_id: String,
    /// The serialized JSON string of the event. This string will be returned
    /// by a search later on.
    pub source: String,
}

impl From<&Event> for SEvent {
    fn from(event: &Event) -> Self {
        Self {
            event_type: SEventType::from(&event.event_type),
            content_value: event.content_value.clone(),
            msgtype: event.msgtype.clone(),
            event_id: event.event_id.clone(),
            sender: event.sender.clone(),
            server_ts: event.server_ts,
            room_id: event.room_id.clone(),
            source: event.source.clone(),
        }
    }
}

#[wasm_bindgen]
impl Event {
    #[wasm_bindgen(constructor)]
    pub fn new(
        event_type: EventType,
        content_value: String,
        msgtype: Option<String>,
        event_id: String,
        sender: String,
        server_ts: i64,
        room_id: String,
        source: String,
    ) -> Self {
        Self {
            event_type: event_type,
            content_value: content_value,
            msgtype: msgtype,
            event_id: event_id,
            sender: sender,
            server_ts: server_ts,
            room_id: room_id,
            source: source,
        }
    }
}

#[derive(Clone)]
#[wasm_bindgen(getter_with_clone)]
pub struct Profile {
    displayname: Option<String>,
    avatar_url: Option<String>,
}

impl From<Profile> for SProfile {
    fn from(profile: Profile) -> Self {
        Self {
            displayname: profile.displayname,
            avatar_url: profile.avatar_url,
        }
    }
}

#[wasm_bindgen]
impl Profile {
    #[wasm_bindgen(constructor)]
    pub fn new(displayname: Option<String>, avatar_url: Option<String>) -> Self {
        Self {
            displayname: displayname,
            avatar_url: avatar_url,
        }
    }
}

#[wasm_bindgen]
impl Database {
    pub fn add_event(&self, event: Event, profile: Profile) {
        self.inner
            .add_event(SEvent::from(&event), SProfile::from(profile));
    }
}

#[wasm_bindgen]
pub async fn new_seshat_db(path: String, config: Config) -> Result<Database, JsError> {
    let s_config = seshat::Config::from(&config);
    let db = match SDatabase::new_with_config(&path, &s_config).await {
        Ok(db) => db,
        Err(e) => {
            // There doesn't seem to be a way to construct custom
            // Javascript errors from the Rust side, since we never
            // throw a RangeError here, let's hack around this by using
            // one here.
            let error = match e {
                Error::ReindexError => Err(JsError::new("Database needs to be reindexed")),
                e => Err(JsError::new(&format!(
                    "Error opening the database: {:?}",
                    e
                ))),
            };
            return error;
        }
    };
    console_log!("new_seshat_db return ");
    return Ok(Database { inner: db });
}
