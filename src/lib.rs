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

//! Seshat - a full text search library for Matrix clients.

#![warn(missing_docs)]

#[cfg(test)]
#[macro_use]
extern crate lazy_static;

mod config;
mod database;
mod index;
mod japanese_tokenizer;
mod events;
mod error;

pub use database::{Connection, Database, Searcher, SearchResult};

pub use error::{Result, Error};

pub use config::{Config, Language, SearchConfig};
pub use events::{Event, EventType, Profile, CrawlerCheckpoint, CheckpointDirection};

pub use std::sync::mpsc::Receiver;

#[cfg(test)]
pub use events::{EVENT, EVENT_SOURCE, TOPIC_EVENT, TOPIC_EVENT_SOURCE};
