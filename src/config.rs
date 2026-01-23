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

use uuid::Uuid;
#[cfg(feature = "encryption")]
use zeroize::Zeroizing;

use crate::events::{EventType, RoomId};

const DEFAULT_LOAD_LIMIT: usize = 20;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(default)]
/// Search configuration
/// A search configuration allows users to limit the search to a specific room
/// or limit the search to specific event types.
/// The search result can be configured in various ways as well.
pub struct SearchConfig {
    pub(crate) limit: usize,
    pub(crate) before_limit: usize,
    pub(crate) after_limit: usize,
    pub(crate) order_by_recency: bool,
    pub(crate) room_id: Option<RoomId>,
    pub(crate) keys: Vec<EventType>,
    pub(crate) next_batch: Option<Uuid>,
}

impl SearchConfig {
    /// Create a new default search configuration.
    pub fn new() -> Self {
        Default::default()
    }

    /// Limit the search to a specific room.
    /// The default is to search all rooms.
    /// # Arguments
    ///
    /// * `room_id` - The unique id of the room.
    pub fn for_room(&mut self, room_id: &str) -> &mut Self {
        self.room_id = Some(room_id.to_owned());
        self
    }

    /// Limit the number of events that will be returned in the search result.
    /// The default for the limit is 10.
    /// # Arguments
    ///
    /// * `limit` - The max number of events to return in the search result.
    pub fn limit(&mut self, limit: usize) -> &mut Self {
        self.limit = limit;
        self
    }

    /// Limit the number of events that happened before our matching event in
    /// the search result.
    /// The default for the limit is 0.
    /// # Arguments
    ///
    /// * `limit` - The max number of contextual events to return in the search
    ///   result.
    pub fn before_limit(&mut self, limit: usize) -> &mut Self {
        self.before_limit = limit;
        self
    }

    /// Limit the number of events that happened after our matching event in
    /// the search result.
    /// The default for the limit is 0.
    /// # Arguments
    ///
    /// * `limit` - The max number of contextual events to return in the search
    ///   result.
    pub fn after_limit(&mut self, limit: usize) -> &mut Self {
        self.after_limit = limit;
        self
    }

    /// Should the matching events be ordered by recency. The default is to
    /// order them by the search score.
    /// # Arguments
    ///
    /// * `order_by_recency` - Flag to determine if we should order by recency.
    ///   result.
    pub fn order_by_recency(&mut self, order_by_recency: bool) -> &mut Self {
        self.order_by_recency = order_by_recency;
        self
    }

    /// Set the event types that should be used as search keys.
    ///
    /// This limits which events will be searched for. This method can be called
    /// multiple times to add multiple event types. The default is to search all
    /// event types.
    ///
    /// # Arguments
    ///
    /// * `key` - The event type that should be included in the search.
    pub fn with_key(&mut self, key: EventType) -> &mut Self {
        self.keys.push(key);
        self.keys.sort();
        self.keys.dedup();

        self
    }

    /// The point to return events from. If given, this should be a next_batch
    ///   result from a previous search.
    pub fn next_batch(&mut self, token: Uuid) -> &mut Self {
        self.next_batch = Some(token);
        self
    }
}

impl Default for SearchConfig {
    fn default() -> Self {
        SearchConfig {
            limit: 10,
            before_limit: 0,
            after_limit: 0,
            order_by_recency: false,
            room_id: None,
            keys: Vec::new(),
            next_batch: None,
        }
    }
}

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

impl Language {
    pub(crate) fn as_tokenizer_name(&self) -> String {
        match self {
            Language::Unknown => "default".to_owned(),
            lang => format!("seshat_{:?}", lang),
        }
    }

    pub(crate) fn as_tantivy(&self) -> tantivy::tokenizer::Language {
        match self {
            Language::Arabic => tantivy::tokenizer::Language::Arabic,
            Language::Danish => tantivy::tokenizer::Language::Danish,
            Language::Dutch => tantivy::tokenizer::Language::Dutch,
            Language::English => tantivy::tokenizer::Language::English,
            Language::Finnish => tantivy::tokenizer::Language::Finnish,
            Language::French => tantivy::tokenizer::Language::French,
            Language::German => tantivy::tokenizer::Language::German,
            Language::Greek => tantivy::tokenizer::Language::Greek,
            Language::Hungarian => tantivy::tokenizer::Language::Hungarian,
            Language::Italian => tantivy::tokenizer::Language::Italian,
            Language::Portuguese => tantivy::tokenizer::Language::Portuguese,
            Language::Romanian => tantivy::tokenizer::Language::Romanian,
            Language::Russian => tantivy::tokenizer::Language::Russian,
            Language::Spanish => tantivy::tokenizer::Language::Spanish,
            Language::Swedish => tantivy::tokenizer::Language::Swedish,
            Language::Tamil => tantivy::tokenizer::Language::Tamil,
            Language::Turkish => tantivy::tokenizer::Language::Turkish,
            _ => panic!("Unsupported language by tantivy"),
        }
    }
}

impl From<&str> for Language {
    fn from(string: &str) -> Self {
        match string.to_lowercase().as_ref() {
            "arabic" => Language::Arabic,
            "danish" => Language::Danish,
            "dutch" => Language::Dutch,
            "english" => Language::English,
            "finnish" => Language::Finnish,
            "french" => Language::French,
            "german" => Language::German,
            "greek" => Language::Greek,
            "hungarian" => Language::Hungarian,
            "italian" => Language::Italian,
            "portuguese" => Language::Portuguese,
            "romanian" => Language::Romanian,
            "russian" => Language::Russian,
            "spanish" => Language::Spanish,
            "swedish" => Language::Swedish,
            "tamil" => Language::Tamil,
            "turkish" => Language::Turkish,
            _ => Language::Unknown,
        }
    }
}

/// Tokenizer mode for the search index.
///
/// This determines how text is tokenized for indexing and searching.
#[derive(Debug, PartialEq, Clone)]
pub enum TokenizerMode {
    /// Language-based tokenizer using stemming (default).
    ///
    /// Uses SimpleTokenizer with language-specific stemming.
    /// Best for languages with word boundaries (e.g., English, German).
    LanguageBased,
    /// N-gram tokenizer for multi-language support.
    ///
    /// Splits text into character n-grams of specified sizes.
    /// Best for languages without clear word boundaries (e.g., Japanese, Chinese).
    Ngram {
        /// Minimum n-gram size (default: 2)
        min_gram: usize,
        /// Maximum n-gram size (default: 4)
        max_gram: usize,
    },
}

impl Default for TokenizerMode {
    fn default() -> Self {
        TokenizerMode::LanguageBased
    }
}

impl TokenizerMode {
    /// Returns the tokenizer name for this mode.
    ///
    /// For N-gram mode, the name includes min/max gram sizes to ensure
    /// schema mismatch detection when these values change.
    pub(crate) fn as_tokenizer_name(&self, language: &Language) -> String {
        match self {
            TokenizerMode::Ngram { min_gram, max_gram } => {
                format!("seshat_ngram_{}_{}", min_gram, max_gram)
            }
            TokenizerMode::LanguageBased => language.as_tokenizer_name(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
/// Configuration for the seshat database.
pub struct Config {
    pub(crate) language: Language,
    pub(crate) tokenizer_mode: TokenizerMode,
    #[cfg(feature = "encryption")]
    pub(crate) passphrase: Option<Zeroizing<String>>,
}

impl Config {
    /// Create a new default Seshat database configuration.
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the indexing language.
    ///
    /// This is used when `TokenizerMode::LanguageBased` is active.
    ///
    /// # Arguments
    ///
    /// * `language` - The language that will be used to index messages.
    pub fn set_language(mut self, language: &Language) -> Self {
        self.language = language.clone();
        self
    }

    /// Set the tokenizer mode to N-gram.
    ///
    /// N-gram tokenizer splits text into character n-grams, which is useful
    /// for languages without clear word boundaries (e.g., Japanese, Chinese).
    ///
    /// Note: Changing the tokenizer mode on an existing database will cause
    /// a schema mismatch error. Delete the database and recreate it to use
    /// a different tokenizer mode.
    ///
    /// # Arguments
    ///
    /// * `min_gram` - Minimum n-gram size (typically 2)
    /// * `max_gram` - Maximum n-gram size (typically 4)
    pub fn use_ngram_tokenizer(mut self, min_gram: usize, max_gram: usize) -> Self {
        self.tokenizer_mode = TokenizerMode::Ngram { min_gram, max_gram };
        self
    }

    /// Set the tokenizer mode to language-based (default).
    ///
    /// This uses SimpleTokenizer with language-specific stemming.
    pub fn use_language_based_tokenizer(mut self) -> Self {
        self.tokenizer_mode = TokenizerMode::LanguageBased;
        self
    }

    /// Set the passphrase of the database.
    /// # Arguments
    ///
    /// * `passphrase` - The passphrase of the database.
    #[cfg(feature = "encryption")]
    pub fn set_passphrase<P: Into<String>>(mut self, passphrase: P) -> Self {
        self.passphrase = Some(Zeroizing::new(passphrase.into()));
        self
    }
}

impl Default for Config {
    fn default() -> Config {
        Config {
            language: Language::Unknown,
            tokenizer_mode: TokenizerMode::default(),
            #[cfg(feature = "encryption")]
            passphrase: None,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[allow(missing_docs)]
pub enum LoadDirection {
    #[default]
    #[serde(rename = "b", alias = "backwards", alias = "backward")]
    Backwards,
    #[serde(rename = "f", alias = "forwards", alias = "forward")]
    Forwards,
}

/// Configuration for the event loading methods.
///
/// A load configuration allows users to limit the number of events that will be
/// loaded or to continue loading events from a specific point in the room
/// history.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadConfig {
    pub(crate) room_id: String,
    pub(crate) limit: usize,
    pub(crate) from_event: Option<String>,
    #[serde(default)]
    pub(crate) direction: LoadDirection,
}

impl LoadConfig {
    /// Create a new LoadConfig
    ///
    /// The config will be created with a default limit of 20 events.
    ///
    /// # Arguments
    ///
    /// * `room_id` - The room from which to load the events.
    pub fn new<R: Into<String>>(room_id: R) -> Self {
        LoadConfig {
            room_id: room_id.into(),
            limit: DEFAULT_LOAD_LIMIT,
            from_event: None,
            direction: LoadDirection::default(),
        }
    }

    /// Set the maximum amount of events that we want to load.
    /// # Arguments
    ///
    /// * `limit` - The new limit that should be set.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Set the event from which we should continue loading events.
    ///
    /// # Arguments
    ///
    /// * `event_id` - An event id of a previous event returned by this
    ///   method. If set events that are older than the event with the given
    ///   event ID will be returned.
    #[allow(clippy::wrong_self_convention)]
    pub fn from_event<E: Into<String>>(mut self, event_id: E) -> Self {
        self.from_event = Some(event_id.into());
        self
    }

    /// Set the direction that we are going to continue loading events from.
    ///
    /// This is only used if we are continuing loading events, that is if
    /// `from_event()` is also set.
    ///
    /// # Arguments
    ///
    /// * `direction` - The direction that should be used.
    pub fn direction(mut self, direction: LoadDirection) -> Self {
        self.direction = direction;
        self
    }
}
