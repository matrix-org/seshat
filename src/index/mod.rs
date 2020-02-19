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

use std::convert::TryInto;
use std::path::Path;
use std::time::Duration;
use tantivy as tv;
use tantivy::chrono::{NaiveDateTime, Utc};
use tantivy::tokenizer::Tokenizer;
use tantivy::Term;

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
}

pub(crate) struct Writer {
    pub(crate) inner: tv::IndexWriter,
    pub(crate) body_field: tv::schema::Field,
    pub(crate) topic_field: tv::schema::Field,
    pub(crate) name_field: tv::schema::Field,
    pub(crate) event_id_field: tv::schema::Field,
    pub(crate) sender_field: tv::schema::Field,
    pub(crate) date_field: tv::schema::Field,
    pub(crate) added_events: usize,
    pub(crate) commit_timestamp: std::time::Instant,
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

        let seconds: i64 = event.server_ts / 1000;
        let nano_seconds: u32 = ((event.server_ts % 1000) * 1000)
            .try_into()
            .unwrap_or_default();
        let naive_date = NaiveDateTime::from_timestamp_opt(seconds, nano_seconds);

        if let Some(d) = naive_date {
            let date = tv::DateTime::from_utc(d, Utc);
            doc.add_date(self.date_field, &date);
        }

        self.inner.add_document(doc);
        self.added_events += 1;
    }

    /// Delete the event with the given event id from the index.
    pub fn delete_event(&mut self, event_id: &str) {
        let term = Term::from_field_text(self.event_id_field, &event_id);
        self.inner.delete_term(term);
        self.inner.commit().unwrap();
    }
}

pub(crate) struct IndexSearcher {
    pub(crate) inner: tv::LeasedItem<tv::Searcher>,
    pub(crate) schema: tv::schema::Schema,
    pub(crate) tokenizer: tv::tokenizer::TokenizerManager,
    pub(crate) body_field: tv::schema::Field,
    pub(crate) topic_field: tv::schema::Field,
    pub(crate) name_field: tv::schema::Field,
    pub(crate) room_id_field: tv::schema::Field,
    #[used]
    pub(crate) sender_field: tv::schema::Field,
    #[used]
    pub(crate) date_field: tv::schema::Field,
    pub(crate) event_id_field: tv::schema::Field,
}

impl IndexSearcher {
    pub fn search(
        &self,
        term: &str,
        config: &SearchConfig,
    ) -> Result<Vec<(f32, EventId)>, tv::TantivyError> {
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

        let query = query_parser.parse_query(&term)?;

        let result = self
            .inner
            .search(&query, &tv::collector::TopDocs::with_limit(config.limit))?;

        let mut docs = Vec::new();

        for (score, docaddress) in result {
            let doc = match self.inner.doc(docaddress) {
                Ok(d) => d,
                Err(_e) => continue,
            };

            let event_id: EventId = match doc.get_first(self.event_id_field) {
                Some(s) => s.text().unwrap().to_owned(),
                None => continue,
            };

            docs.push((score, event_id));
        }
        Ok(docs)
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

        let date_field = schemabuilder.add_date_field("date", tv::schema::INDEXED);

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
    let result = searcher.search("Test", &Default::default()).unwrap();

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
        .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].1, event_id);

    let result = searcher.search("Test", &Default::default()).unwrap();
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
    let result = searcher.search("Test", &Default::default()).unwrap();

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
    let result = searcher.search("伝説", &Default::default()).unwrap();

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
    let result = searcher.search("Test", &Default::default()).unwrap();

    let event_id = &EVENT.event_id;

    assert_eq!(result.len(), 2);
    assert_eq!(&result[0].1, event_id);

    writer.delete_event(event_id);
    writer.force_commit().unwrap();
    index.reload().unwrap();

    let searcher = index.get_searcher();
    let result = searcher.search("Test", &Default::default()).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(&result[0].1, &TOPIC_EVENT.event_id);
}
