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

use tantivy as tv;
use tempdir::TempDir;
use std::path::Path;

pub struct Index {
    index: tv::Index,
    writer: tv::IndexWriter,
    reader: tv::IndexReader,
    body_field: tv::schema::Field,
    topic_field: tv::schema::Field,
    name_field: tv::schema::Field,
    source_field: tv::schema::Field,
}

impl Index {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Index, tv::Error> {
        let mut schemabuilder = tv::schema::Schema::builder();

        let body_field = schemabuilder.add_text_field("body", tv::schema::TEXT);
        let topic_field = schemabuilder.add_text_field("topic", tv::schema::TEXT);
        let name_field = schemabuilder.add_text_field("name", tv::schema::TEXT);
        let source_field = schemabuilder.add_text_field("source", tv::schema::STORED);

        let schema = schemabuilder.build();

        let index_dir = tv::directory::MmapDirectory::open(path)?;

        let index = tv::Index::open_or_create(index_dir, schema)?;
        let writer = index.writer(50_000_000)?;
        let reader = index.reader_builder().reload_policy(tv::ReloadPolicy::OnCommit).try_into()?;

        Ok(Index  {
            index,
            writer,
            reader,
            body_field,
            topic_field,
            name_field,
            source_field,
        })
    }

    pub fn commit(&mut self) {
        self.writer.commit().unwrap();
    }

    pub fn search(&self, term: &str) -> Vec<(tv::Score, tv::DocAddress)> {
        let searcher = self.reader.searcher();
        let query_parser = tv::query::QueryParser::for_index(
            &self.index,
            vec![self.body_field, self.topic_field, self.name_field]
        );

        let query = match query_parser.parse_query(term) {
            Ok(q) => q,
            Err(_e) => return vec![]
        };

        match searcher.search(&query, &tv::collector::TopDocs::with_limit(10)) {
            Ok(result) => result,
            Err(_e) => vec![]
        }
    }

    pub fn add_event(&mut self, body: &str, source: &str) {
        let mut doc = tv::Document::default();
        doc.add_text(self.body_field, body);
        doc.add_text(self.source_field, source);

        self.writer.add_document(doc);
    }
}

#[test]
fn add_an_event() {
    let tmpdir = TempDir::new("matrix-search").unwrap();
    let mut index = Index::new(&tmpdir).unwrap();
    index.add_event(
        "Test message",
        "{
            content: {
                body: Test message, msgtype: m.text
            },
            event_id: $15163622445EBvZJ:localhost,
            origin_server_ts: 1516362244026,
            sender: @example2:localhost,
            type: m.room.message,
            unsigned: {age: 43289803095},
            user_id: @example2:localhost,
            age: 43289803095
        }"
    );
    index.commit();
    index.commit();

    let result = index.search("Test");

    assert_eq!(result.len(), 1);
}
