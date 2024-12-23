use std::collections::HashMap;

use diesel::connection::{set_default_instrumentation, Instrumentation, InstrumentationEvent};
use js_sys::Map;
use seshat::{
    Database as SDatabase, Error, Event as SEvent, EventType as SEventType, Profile as SProfile,
    SearchBatch as SSearchBatch, SearchConfig as SSearchConfig, SearchResult as SSearchResult,
};

use uuid::Uuid;
use wasm_bindgen::{prelude::wasm_bindgen, JsError};
use wasm_bindgen_test::console_log;

pub use wasm_bindgen_rayon::init_thread_pool;
use web_sys::console;

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

#[derive(Clone)]
#[wasm_bindgen(getter_with_clone)]
/// Configuration for the seshat data
pub struct SearchConfig {
    limit: usize,
    before_limit: usize,
    after_limit: usize,
    order_by_recency: bool,
    room_id: Option<String>,
    keys: Vec<EventType>,
    next_batch: Option<String>,
}

#[wasm_bindgen]
impl SearchConfig {
    #[wasm_bindgen(constructor)]
    pub fn new(
        limit: usize,
        before_limit: usize,
        after_limit: usize,
        order_by_recency: bool,
        room_id: Option<String>,
        keys: Vec<EventType>,
        next_batch: Option<String>,
    ) -> Self {
        Self {
            limit: limit,
            before_limit: before_limit,
            after_limit: after_limit,
            order_by_recency: order_by_recency,
            room_id: room_id,
            keys: keys,
            next_batch: next_batch,
        }
    }
}

impl From<&SearchConfig> for SSearchConfig {
    fn from(event: &SearchConfig) -> Self {
        let batch_uuid = if let Some(batch) = event.next_batch.clone() {
            Uuid::parse_str(&batch).ok()
        } else {
            None
        };

        Self {
            limit: event.limit,
            before_limit: event.before_limit,
            after_limit: event.after_limit,
            order_by_recency: event.order_by_recency,
            room_id: event.room_id.clone(),
            keys: event
                .keys
                .clone()
                .into_iter()
                .map(|k| SEventType::from(&k))
                .collect(),
            next_batch: batch_uuid,
        }
    }
}

#[derive(Clone)]
#[wasm_bindgen(getter_with_clone)]
pub struct SearchBatch {
    pub count: usize,
    /// The list of search results that were returned. The number of results is
    /// always smaller of equal to the count and depends on the limit that was
    /// given in the `SearchConfig`.
    pub results: Vec<SearchResult>,
    /// A token that can be set in the `SearchConfig` to continue fetching the
    /// next batch of `SearchResult`s.
    pub next_batch: Option<String>,
}

impl From<&SSearchBatch> for SearchBatch {
    fn from(event: &SSearchBatch) -> Self {
        let res = event
            .results
            .clone()
            .into_iter()
            .map(|r| SearchResult::from(&r))
            .collect();
        Self {
            count: event.count,
            results: res,
            next_batch: event.next_batch.map(|it| it.to_string()),
        }
    }
}

#[derive(Clone)]
#[wasm_bindgen(getter_with_clone)]
pub struct SearchResult {
    /// The score that the full text search assigned to this event.
    pub score: f32,
    /// The serialized source of the event that matched a search.
    pub event_source: String,
    /// Events that happened before our matched event.
    pub events_before: Vec<String>,
    /// Events that happened after our matched event.
    pub events_after: Vec<String>,
    /// The profile of the sender of the matched event.
    pub profile_info: Map,
}

trait IntoFfi {
    fn into_ffi(self) -> js_sys::Map;
}

impl IntoFfi for &HashMap<String, SProfile> {
    fn into_ffi(self) -> js_sys::Map {
        let map = js_sys::Map::new();
        for (k, v) in self.iter() {
            let p = Profile::from(v);
            map.set(&k.into(), &p.into());
        }
        map
    }
}

impl From<&SSearchResult> for SearchResult {
    fn from(event: &SSearchResult) -> Self {
        Self {
            score: event.score,
            event_source: event.event_source.clone(),
            events_before: event.events_before.clone(),
            events_after: event.events_after.clone(),
            profile_info: (&event.profile_info).into_ffi(),
        }
    }
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

impl From<&Profile> for SProfile {
    fn from(profile: &Profile) -> Self {
        Self {
            displayname: profile.displayname.clone(),
            avatar_url: profile.avatar_url.clone(),
        }
    }
}

impl From<&SProfile> for Profile {
    fn from(profile: &SProfile) -> Self {
        Self {
            displayname: profile.displayname.clone(),
            avatar_url: profile.avatar_url.clone(),
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
    pub fn add_event(&mut self, event: Event, profile: Profile) {
        self.inner
            .add_event(SEvent::from(&event), SProfile::from(&profile));
    }

    pub fn commit(&mut self) {
        console_log!("commit");
        self.inner.commit().unwrap();
    }

    pub fn force_commit(&mut self) {
        console_log!("force_commit");
        self.inner.force_commit().unwrap();
    }

    pub async fn wait_merging_threads(self) {
        console_log!("wait_merging_threads");
        let _ = self.inner.wait_merging_threads().await;
        console_log!("wait_merging_threads exit");
    }

    pub fn reload(&mut self) {
        console_log!("reload");
        self.inner.reload().expect("reload failed");
        console_log!("reload exit");
    }
}

#[wasm_bindgen]
impl Database {
    pub async fn search(&mut self, term: &str) -> Result<SearchBatch, JsError> {
        let batch = match &self
            .inner
            // .search(term, &SSearchConfig::from(&config))
            .search(term, &Default::default())
            .await
        {
            Ok(batch) => SearchBatch::from(batch),
            Err(e) => {
                // There doesn't seem to be a way to construct custom
                // Javascript errors from the Rust side, since we never
                // throw a RangeError here, let's hack around this by using
                // one here.
                let error = match e {
                    e => Err(JsError::new(&format!(
                        "Error opening the database: {:?}",
                        e
                    ))),
                };
                return error;
            }
        };
        return Ok(batch);
    }
}

fn simple_logger() -> Option<Box<dyn Instrumentation>> {
    // we need the explicit argument type there due
    // to bugs in rustc
    Some(Box::new(|event: InstrumentationEvent<'_>| {
        console::log_1(&format!("SQL: {event:?}").into());
    }))
}

#[wasm_bindgen]
pub async fn new_seshat_db(path: String, config: Config) -> Result<Database, JsError> {
    let result = set_default_instrumentation(simple_logger);
    let logconfig = tracing_wasm::WASMLayerConfigBuilder::default()
        .set_console_config(tracing_wasm::ConsoleConfig::ReportWithoutConsoleColor)
        .build();

    tracing_wasm::set_as_global_default_with_config(logconfig);

    console_error_panic_hook::set_once();

    if result.is_err() {
        console::log_1(&"set_default_instrumentation error".into());
    } else {
        console::log_1(&"set_default_instrumentation no error".into());
    }
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
