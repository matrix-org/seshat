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

#[cfg(feature = "encryption")]
mod encrypted_dir;
#[cfg(feature = "encryption")]
mod encrypted_stream;
mod japanese_tokenizer;

use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use lru_cache::LruCache;
use tantivy as tv;
use tantivy::collector::{Count, MultiCollector, TopDocs};
use tantivy::Term;
use uuid::Uuid;

use crate::config::{Config, Language, SearchConfig};
use crate::events::{Event, EventId, EventType};
#[cfg(feature = "encryption")]
use crate::index::encrypted_dir::{EncryptedMmapDirectory, PBKDF_COUNT};
use crate::index::japanese_tokenizer::TinySegmenterTokenizer;

// Tantivy requires at least 3MB per writer thread and will panic if we
// give it less than 3MB for the total writer heap size. The amount of writer
// threads that Tantivy will spawn depends on the amount of heap we give it.
// The logic for the number of threads is as follows:
//
//     num_threads = { num_cpu,                  if heap_size / num_cpu >= 3MB
//                   { max(heap_size / 3MB, 1),  if heap_size / num_cpu <  3MB
//
// We give Tantivy 50MB of heap size, which would spawn up to 16 writer threads
// given a CPU with 16 or more cores.
const TANTIVY_WRITER_HEAP_SIZE: usize = 50_000_000;

// Tantivy doesn't behave nicely if `commit()` is called too often on the index
// writer. A commit means that Tantivy will spawn threads that will try to merge
// index segments together, that is, it tries to merge a bunch of smaller files
// into one larger.
//
// If users call commit faster than those threads manage to merge the segments
// the number of spawned threads keeps on increasing.
//
// To mitigate this we limit the commit rate using the following constants. We
// either wait for 500 events to be queued up or wait 5 seconds.
//
// Those constants have been picked empirically by running the database example.
// The COMMIT_TIME is fairly conservative. This does mean that users will have
// to wait 5 seconds before they will manage to see search results for newly
// added events.

/// How many events should we add to the index before we are allowed to commit.
const COMMIT_RATE: usize = 500;
/// How long should we wait between commits if there aren't enough events
/// committed.
const COMMIT_TIME: Duration = Duration::from_secs(5);

/// How many searches should be cached so pagination is supported.
const SEARCH_CACHE_SIZE: usize = 100;
/// How much should the result limit increase every time we need to find more
/// results due to a paginated search.
const SEARCH_LIMIT_INCREMENT: usize = 50;

#[cfg(test)]
use tempfile::TempDir;

#[cfg(test)]
use crate::events::{EVENT, JAPANESE_EVENTS, TOPIC_EVENT};

pub(crate) struct Index {
    index: tv::Index,
    reader: tv::IndexReader,
    body_field: tv::schema::Field,
    topic_field: tv::schema::Field,
    name_field: tv::schema::Field,
    event_id_field: tv::schema::Field,
    sender_field: tv::schema::Field,
    date_field: tv::schema::Field,
    room_id_field: tv::schema::Field,
    search_cache: Arc<RwLock<LruCache<Uuid, Search>>>,
}

#[derive(Clone)]
struct Search {
    search_term: Arc<String>,
    search_config: Arc<SearchConfig>,
    event_ids: Arc<Vec<String>>,
}

#[derive(Debug)]
pub(crate) struct SearchResult {
    pub(crate) count: usize,
    pub(crate) results: Vec<(f32, EventId)>,
    pub(crate) next_batch: Option<Uuid>,
}

pub(crate) struct Writer {
    inner: tv::IndexWriter,
    body_field: tv::schema::Field,
    topic_field: tv::schema::Field,
    name_field: tv::schema::Field,
    event_id_field: tv::schema::Field,
    sender_field: tv::schema::Field,
    date_field: tv::schema::Field,
    added_events: usize,
    commit_timestamp: std::time::Instant,
    room_id_field: tv::schema::Field,
}

impl Writer {
    pub fn commit(&mut self) -> Result<bool, tv::TantivyError> {
        self.commit_helper(false)
    }

    fn commit_helper(&mut self, force: bool) -> Result<bool, tv::TantivyError> {
        if self.added_events > 0
            && (force
                || self.added_events >= COMMIT_RATE
                || self.commit_timestamp.elapsed() >= COMMIT_TIME)
        {
            self.inner.commit()?;
            self.added_events = 0;
            self.commit_timestamp = std::time::Instant::now();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn force_commit(&mut self) -> Result<(), tv::TantivyError> {
        self.commit_helper(true)?;
        Ok(())
    }

    pub fn add_event(&mut self, event: &Event) {
        let mut doc = tv::Document::default();

        match event.event_type {
            EventType::Message => doc.add_text(self.body_field, &event.content_value),
            EventType::Topic => doc.add_text(self.topic_field, &event.content_value),
            EventType::Name => doc.add_text(self.name_field, &event.content_value),
        }

        doc.add_text(self.event_id_field, &event.event_id);
        doc.add_text(self.room_id_field, &event.room_id);
        doc.add_text(self.sender_field, &event.sender);
        doc.add_u64(self.date_field, event.server_ts as u64);

        self.inner.add_document(doc);
        self.added_events += 1;
    }

    /// Delete the event with the given event id from the index.
    pub fn delete_event(&mut self, event_id: &str) {
        let term = Term::from_field_text(self.event_id_field, &event_id);
        self.inner.delete_term(term);
        self.inner.commit().unwrap();
    }

    pub fn wait_merging_threads(self) -> Result<(), tv::TantivyError> {
        self.inner.wait_merging_threads()
    }
}

pub(crate) struct IndexSearcher {
    inner: tv::LeasedItem<tv::Searcher>,
    schema: tv::schema::Schema,
    tokenizer: tv::tokenizer::TokenizerManager,
    body_field: tv::schema::Field,
    topic_field: tv::schema::Field,
    name_field: tv::schema::Field,
    room_id_field: tv::schema::Field,
    #[used]
    sender_field: tv::schema::Field,
    #[used]
    date_field: tv::schema::Field,
    event_id_field: tv::schema::Field,
    search_cache: Arc<RwLock<LruCache<Uuid, Search>>>,
}

impl IndexSearcher {
    fn parse_query(
        &self,
        term: &str,
        config: &SearchConfig,
    ) -> Result<Box<dyn tv::query::Query>, tv::TantivyError> {
        let mut keys = Vec::new();

        let term = if let Some(room) = &config.room_id {
            keys.push(self.room_id_field);
            format!("+room_id:\"{}\" AND \"{}\"", room, term)
        } else if term.is_empty() {
            "*".to_owned()
        } else {
            term.to_owned()
        };

        if config.keys.is_empty() {
            keys.append(&mut vec![
                self.body_field,
                self.topic_field,
                self.name_field,
            ]);
        } else {
            for key in config.keys.iter() {
                match key {
                    EventType::Message => keys.push(self.body_field),
                    EventType::Topic => keys.push(self.topic_field),
                    EventType::Name => keys.push(self.name_field),
                }
            }
        }

        let query_parser =
            tv::query::QueryParser::new(self.schema.clone(), keys, self.tokenizer.clone());

        Ok(query_parser.parse_query(&term)?)
    }

    fn search_helper(
        &self,
        og_limit: usize,
        limit: usize,
        order_by_recency: bool,
        previous_results: &[EventId],
        query: &Box<dyn tv::query::Query>,
    ) -> Result<((usize, Vec<(f32, EventId)>), Vec<EventId>), tv::TantivyError> {
        let mut multicollector = MultiCollector::new();
        let count_handle = multicollector.add_collector(Count);

        let (mut result, top_docs) = if order_by_recency {
            let top_docs_handle = multicollector
                .add_collector(TopDocs::with_limit(limit).order_by_u64_field(self.date_field));

            let mut result = self.inner.search(query, &multicollector)?;
            let mut top_docs = top_docs_handle.extract(&mut result);
            (
                result,
                top_docs
                    .drain(..)
                    .map(|(_, address)| (1.0, address))
                    .collect(),
            )
        } else {
            let top_docs_handle = multicollector.add_collector(TopDocs::with_limit(limit));
            let mut result = self.inner.search(query, &multicollector)?;

            let top_docs = top_docs_handle.extract(&mut result);
            (result, top_docs)
        };

        let mut docs = Vec::new();
        let mut event_ids = Vec::new();

        let count = count_handle.extract(&mut result);

        let end = count == top_docs.len();

        for (score, docaddress) in top_docs {
            let doc = match self.inner.doc(docaddress) {
                Ok(d) => d,
                Err(_e) => continue,
            };

            let event_id: EventId = match doc.get_first(self.event_id_field) {
                Some(s) => s.text().unwrap().to_owned(),
                None => continue,
            };

            // Skip results that were already returne in a previous search.
            if previous_results.contains(&event_id) {
                continue;
            }

            event_ids.push(event_id.clone());
            docs.push((score, event_id));
        }

        if docs.len() < og_limit {
            if end {
                Ok(((count, docs), event_ids))
            } else {
                self.search_helper(
                    og_limit,
                    limit + SEARCH_LIMIT_INCREMENT,
                    order_by_recency,
                    &previous_results,
                    &query,
                )
            }
        } else {
            Ok(((count, docs), event_ids))
        }
    }

    pub fn search(
        &self,
        term: &str,
        config: &SearchConfig,
    ) -> Result<SearchResult, tv::TantivyError> {
        let past_search = if let Some(token) = &config.next_batch {
            let mut search_cache = self.search_cache.write().unwrap();
            search_cache.get_mut(&token).cloned()
        } else {
            None
        };

        let ((result, event_ids), term, config) = if let Some(past_search) = past_search {
            let query = self.parse_query(term, &past_search.search_config)?;
            let previous_results = &past_search.event_ids;

            let (result, mut event_ids) = self.search_helper(
                config.limit,
                config.limit,
                config.order_by_recency,
                previous_results,
                &query,
            )?;

            // Add the previous results to the current ones.
            event_ids.extend(previous_results.iter().cloned());

            (
                (result, event_ids),
                past_search.search_term.clone(),
                past_search.search_config.clone(),
            )
        } else {
            let query = self.parse_query(term, config)?;
            (
                self.search_helper(
                    config.limit,
                    config.limit,
                    config.order_by_recency,
                    &[],
                    &query,
                )?,
                Arc::new(term.to_owned()),
                Arc::new(config.clone()),
            )
        };

        let (count, results) = result;

        let next_batch = if event_ids.len() == count {
            None
        } else {
            let mut search_cache = self.search_cache.write().unwrap();
            let search = Search {
                search_term: term,
                search_config: config,
                event_ids: Arc::new(event_ids),
            };

            let token = Uuid::new_v4();
            search_cache.insert(token, search);
            Some(token)
        };

        Ok(SearchResult {
            next_batch,
            count,
            results,
        })
    }
}

impl Index {
    pub fn new<P: AsRef<Path>>(path: P, config: &Config) -> Result<Index, tv::TantivyError> {
        let tokenizer_name = config.language.as_tokenizer_name();

        let text_field_options = Index::create_text_options(&tokenizer_name);
        let mut schemabuilder = tv::schema::Schema::builder();

        let body_field = schemabuilder.add_text_field("body", text_field_options.clone());
        let topic_field = schemabuilder.add_text_field("topic", text_field_options.clone());
        let name_field = schemabuilder.add_text_field("name", text_field_options);

        let date_field = schemabuilder.add_u64_field("date", tv::schema::FAST);

        let sender_field = schemabuilder.add_text_field("sender", tv::schema::STRING);
        let room_id_field =
            schemabuilder.add_text_field("room_id", tv::schema::STORED | tv::schema::STRING);

        let event_id_field =
            schemabuilder.add_text_field("event_id", tv::schema::STORED | tv::schema::STRING);

        let schema = schemabuilder.build();

        let index = Index::open_index(path, config, schema)?;
        let reader = index.reader()?;

        match config.language {
            Language::Unknown => (),
            Language::Japanese => {
                index
                    .tokenizers()
                    .register(&tokenizer_name, TinySegmenterTokenizer::new());
            }
            _ => {
                let tokenizer = tv::tokenizer::TextAnalyzer::from(tv::tokenizer::SimpleTokenizer)
                    .filter(tv::tokenizer::RemoveLongFilter::limit(40))
                    .filter(tv::tokenizer::LowerCaser)
                    .filter(tv::tokenizer::Stemmer::new(config.language.as_tantivy()));
                index.tokenizers().register(&tokenizer_name, tokenizer);
            }
        }

        Ok(Index {
            index,
            reader,
            body_field,
            topic_field,
            name_field,
            event_id_field,
            sender_field,
            date_field,
            room_id_field,
            search_cache: Arc::new(RwLock::new(LruCache::new(SEARCH_CACHE_SIZE))),
        })
    }

    #[cfg(feature = "encryption")]
    fn open_index<P: AsRef<Path>>(
        path: P,
        config: &Config,
        schema: tv::schema::Schema,
    ) -> tv::Result<tv::Index> {
        match &config.passphrase {
            Some(p) => {
                let dir = EncryptedMmapDirectory::open_or_create(path, &p, PBKDF_COUNT)?;
                tv::Index::open_or_create(dir, schema)
            }
            None => {
                let dir = tv::directory::MmapDirectory::open(path)?;
                tv::Index::open_or_create(dir, schema)
            }
        }
    }

    #[cfg(not(feature = "encryption"))]
    fn open_index<P: AsRef<Path>>(
        path: P,
        _config: &Config,
        schema: tv::schema::Schema,
    ) -> tv::Result<tv::Index> {
        let dir = tv::directory::MmapDirectory::open(path)?;
        tv::Index::open_or_create(dir, schema)
    }

    #[cfg(feature = "encryption")]
    pub fn change_passphrase<P: AsRef<Path>>(
        path: P,
        old_passphrase: &str,
        new_passphrase: &str,
    ) -> Result<(), tv::TantivyError> {
        EncryptedMmapDirectory::change_passphrase(
            path,
            old_passphrase,
            new_passphrase,
            PBKDF_COUNT,
        )?;
        Ok(())
    }

    fn create_text_options(tokenizer: &str) -> tv::schema::TextOptions {
        let indexing = tv::schema::TextFieldIndexing::default()
            .set_tokenizer(tokenizer)
            .set_index_option(tv::schema::IndexRecordOption::WithFreqsAndPositions);
        tv::schema::TextOptions::default().set_indexing_options(indexing)
    }

    pub fn get_searcher(&self) -> IndexSearcher {
        let searcher = self.reader.searcher();
        let schema = self.index.schema();
        let tokenizer = self.index.tokenizers().clone();

        IndexSearcher {
            inner: searcher,
            schema,
            tokenizer,
            body_field: self.body_field,
            topic_field: self.topic_field,
            name_field: self.name_field,
            room_id_field: self.room_id_field,
            sender_field: self.sender_field,
            date_field: self.date_field,
            event_id_field: self.event_id_field,
            search_cache: self.search_cache.clone(),
        }
    }

    pub fn reload(&self) -> Result<(), tv::TantivyError> {
        self.reader.reload()
    }

    pub fn get_writer(&self) -> Result<Writer, tv::TantivyError> {
        Ok(Writer {
            inner: self
                .index
                .writer_with_num_threads(1, TANTIVY_WRITER_HEAP_SIZE)?,
            body_field: self.body_field,
            topic_field: self.topic_field,
            name_field: self.name_field,
            event_id_field: self.event_id_field,
            room_id_field: self.room_id_field,
            sender_field: self.sender_field,
            date_field: self.date_field,
            added_events: 0,
            commit_timestamp: std::time::Instant::now(),
        })
    }
}

#[test]
fn add_an_event() {
    let tmpdir = TempDir::new().unwrap();
    let config = Config::new().set_language(&Language::English);
    let index = Index::new(&tmpdir, &config).unwrap();

    let mut writer = index.get_writer().unwrap();

    writer.add_event(&EVENT);
    writer.force_commit().unwrap();
    index.reload().unwrap();

    let searcher = index.get_searcher();
    let result = searcher
        .search("Test", &Default::default())
        .unwrap()
        .results;

    let event_id = EVENT.event_id.to_string();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].1, event_id)
}

#[test]
fn add_events_to_differing_rooms() {
    let tmpdir = TempDir::new().unwrap();
    let config = Config::new().set_language(&Language::English);
    let index = Index::new(&tmpdir, &config).unwrap();

    let event_id = EVENT.event_id.to_string();
    let mut writer = index.get_writer().unwrap();

    let mut event2 = EVENT.clone();
    event2.room_id = "!Test2:room".to_string();

    writer.add_event(&EVENT);
    writer.add_event(&event2);

    writer.force_commit().unwrap();
    index.reload().unwrap();

    let searcher = index.get_searcher();
    let result = searcher
        .search("Test", &SearchConfig::new().for_room(&EVENT.room_id))
        .unwrap()
        .results;

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].1, event_id);

    let result = searcher
        .search("Test", &Default::default())
        .unwrap()
        .results;
    assert_eq!(result.len(), 2);
}

#[test]
fn switch_languages() {
    let tmpdir = TempDir::new().unwrap();
    let config = Config::new().set_language(&Language::English);
    let index = Index::new(&tmpdir, &config).unwrap();

    let mut writer = index.get_writer().unwrap();

    writer.add_event(&EVENT);
    writer.force_commit().unwrap();
    index.reload().unwrap();

    let searcher = index.get_searcher();
    let result = searcher
        .search("Test", &Default::default())
        .unwrap()
        .results;

    let event_id = EVENT.event_id.to_string();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].1, event_id);

    drop(index);

    let config = Config::new().set_language(&Language::German);
    let index = Index::new(&tmpdir, &config);

    assert!(index.is_err())
}

#[test]
fn japanese_tokenizer() {
    let tmpdir = TempDir::new().unwrap();
    let config = Config::new().set_language(&Language::Japanese);
    let index = Index::new(&tmpdir, &config).unwrap();

    let mut writer = index.get_writer().unwrap();

    for event in JAPANESE_EVENTS.iter() {
        writer.add_event(event);
    }

    writer.force_commit().unwrap();
    index.reload().unwrap();

    let searcher = index.get_searcher();
    let result = searcher
        .search("伝説", &Default::default())
        .unwrap()
        .results;

    let event_id = JAPANESE_EVENTS[1].event_id.to_string();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].1, event_id);
}

#[test]
fn event_count() {
    let tmpdir = TempDir::new().unwrap();
    let config = Config::new().set_language(&Language::English);
    let index = Index::new(&tmpdir, &config).unwrap();

    let mut writer = index.get_writer().unwrap();

    assert_eq!(writer.added_events, 0);
    writer.add_event(&EVENT);
    assert_eq!(writer.added_events, 1);

    writer.force_commit().unwrap();
    assert_eq!(writer.added_events, 0);
}

#[test]
fn delete_an_event() {
    let tmpdir = TempDir::new().unwrap();
    let config = Config::new().set_language(&Language::English);
    let index = Index::new(&tmpdir, &config).unwrap();

    let mut writer = index.get_writer().unwrap();

    writer.add_event(&EVENT);
    writer.add_event(&TOPIC_EVENT);
    writer.force_commit().unwrap();
    index.reload().unwrap();

    let searcher = index.get_searcher();
    let result = searcher
        .search("Test", &Default::default())
        .unwrap()
        .results;

    let event_id = &EVENT.event_id;

    assert_eq!(result.len(), 2);
    assert_eq!(&result[0].1, event_id);

    writer.delete_event(event_id);
    writer.force_commit().unwrap();
    index.reload().unwrap();

    let searcher = index.get_searcher();
    let result = searcher
        .search("Test", &Default::default())
        .unwrap()
        .results;
    assert_eq!(result.len(), 1);
    assert_eq!(&result[0].1, &TOPIC_EVENT.event_id);
}

#[test]
fn paginated_search() {
    let tmpdir = TempDir::new().unwrap();
    let config = Config::new().set_language(&Language::English);
    let index = Index::new(&tmpdir, &config).unwrap();

    let mut writer = index.get_writer().unwrap();

    writer.add_event(&EVENT);
    writer.add_event(&TOPIC_EVENT);
    writer.force_commit().unwrap();
    index.reload().unwrap();

    let searcher = index.get_searcher();
    let first_search = searcher
        .search("Test", SearchConfig::new().limit(1))
        .unwrap();

    assert_eq!(first_search.results.len(), 1);

    let second_search = searcher
        .search(
            "Test",
            SearchConfig::new()
                .limit(1)
                .next_batch(first_search.next_batch.unwrap()),
        )
        .unwrap();
    assert_eq!(second_search.results.len(), 1);
    assert_eq!(&first_search.results[0].1, &EVENT.event_id);
    assert_eq!(&second_search.results[0].1, &TOPIC_EVENT.event_id);
    assert!(second_search.next_batch.is_none());
}
