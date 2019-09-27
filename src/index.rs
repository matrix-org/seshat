// Copyright 2019 The Matrix.org Foundation CIC
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

use std::path::Path;
use tantivy as tv;
use tantivy::tokenizer::Tokenizer;

use crate::japanese_tokenizer::TinySegmenterTokenizer;
use crate::types::{Event, EventId, EventType, Language, SearchConfig};

#[cfg(test)]
use tempfile::TempDir;

#[cfg(test)]
use crate::types::{EVENT, JAPANESE_EVENTS};

pub(crate) struct Index {
    index: tv::Index,
    reader: tv::IndexReader,
    body_field: tv::schema::Field,
    topic_field: tv::schema::Field,
    name_field: tv::schema::Field,
    event_id_field: tv::schema::Field,
    room_id_field: tv::schema::Field,
    server_timestamp_field: tv::schema::Field,
}

pub(crate) struct Writer {
    pub(crate) inner: tv::IndexWriter,
    pub(crate) body_field: tv::schema::Field,
    pub(crate) topic_field: tv::schema::Field,
    pub(crate) name_field: tv::schema::Field,
    pub(crate) event_id_field: tv::schema::Field,
    room_id_field: tv::schema::Field,
    server_timestamp_field: tv::schema::Field,
}

impl Writer {
    pub fn commit(&mut self) -> Result<(), tv::Error> {
        self.inner.commit()?;
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
        doc.add_u64(self.server_timestamp_field, event.server_ts as u64);

        self.inner.add_document(doc);
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
    pub(crate) event_id_field: tv::schema::Field,
    pub(crate) server_timestamp_field: tv::schema::Field,
}

impl IndexSearcher {
    pub fn search(
        &self,
        term: &str,
        config: &SearchConfig,
    ) -> Result<Vec<(f32, EventId)>, tv::Error> {
        let mut keys = Vec::new();

        let term = if let Some(room) = &config.room_id {
            keys.push(self.room_id_field);
            format!("{} AND room_id:\"{}\"", term, room)
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

        if config.order_by_recent {
            let collector = tv::collector::TopDocs::with_limit(config.limit);
            let collector = collector.order_by_u64_field(self.server_timestamp_field);

            let result = self.inner.search(&query, &collector)?;

            let mut docs = Vec::new();

            for (_, docaddress) in result {
                let doc = match self.inner.doc(docaddress) {
                    Ok(d) => d,
                    Err(_e) => continue,
                };

                let event_id: EventId = match doc.get_first(self.event_id_field) {
                    Some(s) => s.text().unwrap().to_owned(),
                    None => continue,
                };

                docs.push((1.0, event_id));
            }
            Ok(docs)
        } else {
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
}

impl Index {
    pub fn new<P: AsRef<Path>>(path: P, language: &Language) -> Result<Index, tv::Error> {
        let tokenizer_name = language.as_tokenizer_name();

        let text_field_options = Index::create_text_options(&tokenizer_name);
        let mut schemabuilder = tv::schema::Schema::builder();

        let body_field = schemabuilder.add_text_field("body", text_field_options.clone());
        let topic_field = schemabuilder.add_text_field("topic", text_field_options.clone());
        let name_field = schemabuilder.add_text_field("name", text_field_options);
        let room_id_field = schemabuilder.add_text_field("room_id", tv::schema::STRING);
        let server_timestamp_field =
            schemabuilder.add_u64_field("server_timestamp", tv::schema::FAST);

        let event_id_field = schemabuilder.add_text_field("event_id", tv::schema::STORED);

        let schema = schemabuilder.build();

        let index_dir = tv::directory::MmapDirectory::open(path)?;

        let index = tv::Index::open_or_create(index_dir, schema)?;
        let reader = index.reader()?;

        match language {
            Language::Unknown => (),
            Language::Japanese => {
                index
                    .tokenizers()
                    .register(&tokenizer_name, TinySegmenterTokenizer::new());
            }
            _ => {
                let tokenizer = tv::tokenizer::SimpleTokenizer
                    .filter(tv::tokenizer::RemoveLongFilter::limit(40))
                    .filter(tv::tokenizer::LowerCaser)
                    .filter(tv::tokenizer::Stemmer::new(language.as_tantivy()));
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
            room_id_field,
            server_timestamp_field,
        })
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
            event_id_field: self.event_id_field,
            server_timestamp_field: self.server_timestamp_field,
        }
    }

    pub fn reload(&self) -> Result<(), tv::Error> {
        self.reader.reload()
    }

    pub fn get_writer(&self) -> Result<Writer, tv::Error> {
        Ok(Writer {
            inner: self.index.writer(50_000_000)?,
            body_field: self.body_field,
            topic_field: self.topic_field,
            name_field: self.name_field,
            event_id_field: self.event_id_field,
            room_id_field: self.room_id_field,
            server_timestamp_field: self.server_timestamp_field,
        })
    }
}

#[test]
fn add_an_event() {
    let tmpdir = TempDir::new().unwrap();
    let index = Index::new(&tmpdir, &Language::English).unwrap();

    let mut writer = index.get_writer().unwrap();

    writer.add_event(&EVENT);
    writer.commit().unwrap();
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
    let index = Index::new(&tmpdir, &Language::English).unwrap();

    let event_id = EVENT.event_id.to_string();
    let mut writer = index.get_writer().unwrap();

    let mut event2 = EVENT.clone();
    event2.room_id = "!Test2:room".to_string();

    writer.add_event(&EVENT);
    writer.add_event(&event2);

    writer.commit().unwrap();
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
fn order_results_by_date() {
    let tmpdir = TempDir::new().unwrap();
    let index = Index::new(&tmpdir, &Language::English).unwrap();

    let event_id = EVENT.event_id.to_string();
    let mut writer = index.get_writer().unwrap();

    let mut event2 = EVENT.clone();
    event2.server_ts += 100;

    writer.add_event(&EVENT);
    writer.add_event(&event2);

    writer.commit().unwrap();
    index.reload().unwrap();

    let searcher = index.get_searcher();
    let config = SearchConfig {
        order_by_recent: true,
        ..Default::default()
    };
    let result = searcher.search("Test", &config).unwrap();

    assert_eq!(result.len(), 2);
    assert_eq!(result[1].1, event_id);
}

#[test]
fn switch_languages() {
    let tmpdir = TempDir::new().unwrap();
    let index = Index::new(&tmpdir, &Language::English).unwrap();

    let mut writer = index.get_writer().unwrap();

    writer.add_event(&EVENT);
    writer.commit().unwrap();
    index.reload().unwrap();

    let searcher = index.get_searcher();
    let result = searcher.search("Test", &Default::default()).unwrap();

    let event_id = EVENT.event_id.to_string();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].1, event_id);

    drop(index);

    let index = Index::new(&tmpdir, &Language::German);

    assert!(index.is_err())
}

#[test]
fn japanese_tokenizer() {
    let tmpdir = TempDir::new().unwrap();
    let index = Index::new(&tmpdir, &Language::Japanese).unwrap();

    let mut writer = index.get_writer().unwrap();

    for event in JAPANESE_EVENTS.iter() {
        writer.add_event(event);
    }

    writer.commit().unwrap();
    index.reload().unwrap();

    let searcher = index.get_searcher();
    let result = searcher.search("伝説", &Default::default()).unwrap();

    let event_id = JAPANESE_EVENTS[1].event_id.to_string();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].1, event_id);
}
