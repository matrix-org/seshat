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

use thiserror::Error;

/// Result type for seshat operations.
pub type Result<T> = std::result::Result<T, Error>;

#[allow(clippy::upper_case_acronyms)]
#[derive(Error, Debug)]
/// Seshat error types.
pub enum Error {
    // #[error("Sqlite pool error: {}", _0)]
    /// Error signaling that there was an error with the Sqlite connection
    /// pool.
    // PoolError(#[from] r2d2::Error),
    #[error("Sqlite database error: {}", _0)]
    /// Error signaling that there was an error with a Sqlite transaction.
    DatabaseError(#[from] diesel::result::Error),
    #[error("Index error: {}", _0)]
    /// Error signaling that there was an error with the event indexer.
    IndexError(tantivy::TantivyError),
    #[error("File system error: {}", _0)]
    /// Error signaling that there was an error while reading from the
    /// filesystem.
    FsError(#[from] fs_extra::error::Error),
    #[error("IO error: {}", _0)]
    /// Error signaling that there was an error while doing a IO operation.
    IOError(#[from] std::io::Error),
    /// Error signaling that the database passphrase was incorrect.
    #[error("Error unlocking the database: {}", _0)]
    DatabaseUnlockError(String),
    /// Error when opening the Seshat database and reading the database version.
    #[error("Database version missmatch.")]
    DatabaseVersionError,
    /// Error when opening the Seshat database and reading the database version.
    #[error("Error opening the database: {}", _0)]
    DatabaseOpenError(String),
    /// Error signaling that sqlcipher support is missing.
    #[error("Sqlcipher error: {}", _0)]
    SqlCipherError(String),
    /// Error indicating that the index needs to be rebuilt.
    #[error("Error opening the database, the index needs to be rebuilt.")]
    ReindexError,
}

impl From<tantivy::TantivyError> for Error {
    fn from(err: tantivy::TantivyError) -> Self {
        Error::IndexError(err)
    }
}
