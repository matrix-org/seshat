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

use std::{
    cmp::Ordering,
    collections::{hash_map, HashMap},
    fmt::format,
};

// use rusqlite::{params, params_from_iter, ToSql};

// #[cfg(test)]
// use r2d2::PooledConnection;
// #[cfg(test)]
// use r2d2_sqlite::SqliteConnectionManager;

use diesel::{
    dsl::insert_or_ignore_into,
    prelude::QueryableByName,
    query_dsl::methods::FilterDsl,
    sql_query,
    sql_types::{BigInt, Bool, Integer},
    update, ExpressionMethods, Identifiable, Insertable, Queryable, RunQueryDsl, Selectable,
};
use diesel_wasm_sqlite::connection::WasmSqliteConnection;
use tantivy::schema;
use web_sys::console;

use crate::{
    config::LoadDirection,
    database::{schema::version_table, SearchResult, DATABASE_VERSION},
    error::Result,
    events::{CrawlerCheckpoint, Event, EventContext, EventId, Profile, SerializedEvent},
    index::Writer as IndexWriter,
    Database, EventType,
};

const FILE_EVENT_TYPES: &str = "'m.image', 'm.file', 'm.audio', 'm.video'";
use super::schema::reindex_needed_table;
pub(crate) use diesel_migrations::{embed_migrations, EmbeddedMigrations};
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("../migrations");

#[derive(QueryableByName, Selectable, Insertable, Debug, PartialEq, Clone)]
#[diesel(table_name = version_table)]
#[diesel(primary_key(id))]
pub struct Version {
    #[diesel(sql_type = BigInt)]
    version: i64,
}

#[derive(QueryableByName, Insertable, Debug, PartialEq, Clone)]
#[diesel(table_name = reindex_needed_table)]
pub struct ReindexNeeded {
    #[diesel(sql_type = Bool)]
    reindex_needed: bool,
}

impl Database {
    /// Write the events to the database.
    /// Returns a tuple containing a boolean and an array if integers. The
    /// boolean notifies us if all the events were already added to the
    /// database, the integers are the database ids of our events.
    pub(crate) fn write_events_helper(
        // conn: &WasmSqliteConnection,
        index_writer: &mut IndexWriter,
        events: &mut Vec<(Event, Profile)>,
    ) -> Result<(bool, Vec<i64>)> {
        let mut ret = Vec::new();
        let mut event_ids = Vec::new();

        for (mut e, mut p) in events.drain(..) {
            let event_id = Database::save_event(&mut e, &mut p)?;
            match event_id {
                Some(event_id) => {
                    index_writer.add_event(&e);
                    ret.push(false);
                    event_ids.push(event_id);
                }
                None => {
                    ret.push(true);
                    continue;
                }
            }
        }

        Ok((ret.iter().all(|&x| x), event_ids))
    }

    pub(crate) fn delete_event_helper(
        // conn: &mut WasmSqliteConnection,
        index_writer: &mut IndexWriter,
        event_id: EventId,
        pending_deletion_events: &mut Vec<EventId>,
    ) -> Result<bool> {
        // let transaction = connection.transaction()?;

        Database::delete_event_by_id(&event_id);
        // transaction.execute(
        //     "INSERT OR IGNORE INTO pending_deletion_events (event_id) VALUES (?1)",
        //     [&event_id],
        // )?;
        // transaction.commit().unwrap();

        index_writer.delete_event(&event_id);
        pending_deletion_events.push(event_id);

        let committed = index_writer.commit()?;

        // if committed {
        //     Database::mark_events_as_deleted(connection, pending_deletion_events)?;
        // }

        Ok(committed)
    }

    pub(crate) fn mark_events_as_deleted(
        // conn: &mut WasmSqliteConnection,
        events: &mut Vec<EventId>,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        // let transaction = connection.transaction()?;

        for chunk in events.chunks(50) {
            let parameter_str = ", ?".repeat(chunk.len() - 1);

            // let mut stmt = transaction.prepare(&format!(
            //     "DELETE from pending_deletion_events
            //          WHERE event_id IN (?{})",
            //     &parameter_str
            // ))?;

            // stmt.execute(params_from_iter(chunk))?;
        }

        // transaction.commit()?;
        events.clear();

        Ok(())
    }

    /// Delete non committed events from the database that were committed
    pub(crate) fn mark_events_as_indexed(
        // conn: &mut WasmSqliteConnection,
        events: &mut Vec<i64>,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        // let transaction = connection.transaction()?;

        for chunk in events.chunks(50) {
            let parameter_str = ", ?".repeat(chunk.len() - 1);

            // let mut stmt = transaction.prepare(&format!(
            //     "DELETE from uncommitted_events
            //          WHERE id IN (?{})",
            //     &parameter_str
            // ))?;

            // stmt.execute(params_from_iter(chunk))?;
        }

        // transaction.commit()?;
        events.clear();

        Ok(())
    }

    pub(crate) fn write_events(
        // conn: &mut WasmSqliteConnection,
        index_writer: &mut IndexWriter,
        message: (
            Option<CrawlerCheckpoint>,
            Option<CrawlerCheckpoint>,
            &mut Vec<(Event, Profile)>,
        ),
        force_commit: bool,
        uncommitted_events: &mut Vec<i64>,
    ) -> Result<(bool, bool)> {
        let (new_checkpoint, old_checkpoint, events) = message;
        // let transaction = connection.transaction()?;

        let (ret, event_ids) = Database::write_events_helper(index_writer, events)?;
        Database::replace_crawler_checkpoint(
            // &transaction,
            new_checkpoint.as_ref(),
            old_checkpoint.as_ref(),
        );

        // transaction.commit()?;

        uncommitted_events.extend(event_ids);

        let committed = if force_commit {
            index_writer.force_commit()?;
            true
        } else {
            index_writer.commit()?
        };

        if committed {
            // Database::mark_events_as_indexed(connection, uncommitted_events)?;
        }

        Ok((ret, committed))
    }

    pub(crate) fn get_user_version(// conn: &WasmSqliteConnection
    ) -> Result<i64> {
        // Ok(connection.query_row("SELECT version FROM user_version", [], |row| row.get(0))?)
        Ok(0)
    }

    pub(crate) fn set_user_version(
        // conn: &WasmSqliteConnection,
        version: i64,
    ) -> Result<()> {
        // connection.execute("UPDATE user_version SET version = ?", [version])?;
        Ok(())
    }

    pub(crate) fn get_version(conn: &mut WasmSqliteConnection) -> Result<(i64, bool)> {
        let _ = sql_query(
            "CREATE TABLE IF NOT EXISTS version_table (
                    id INTEGER NOT NULL PRIMARY KEY CHECK (id = 1),
                    version INTEGER NOT NULL
                )",
        )
        .execute(conn);

        let _ = sql_query(
            "CREATE TABLE IF NOT EXISTS reindex_needed_table (
                id INTEGER NOT NULL PRIMARY KEY CHECK (id = 1),
                reindex_needed BOOL NOT NULL
            )",
        )
        .execute(conn);

        let reindex_count = insert_or_ignore_into(reindex_needed_table::table)
            .values(reindex_needed_table::reindex_needed.eq(false))
            .execute(conn);

        console::log_1(&format!("reindex_count {:?}", reindex_count).into());

        let version_count = insert_or_ignore_into(version_table::table)
            .values(version_table::version.eq(DATABASE_VERSION))
            .execute(conn);

        console::log_1(&format!("version_count {:?}", version_count).into());
        let mut version = sql_query("SELECT version FROM version_table")
            .load::<Version>(conn)
            .unwrap()[0]
            .version;

        let mut reindex_needed = sql_query("SELECT reindex_needed FROM reindex_needed_table")
            .load::<ReindexNeeded>(conn)
            .unwrap()[0]
            .reindex_needed;

        // Do database migrations here before bumping the database version.

        if version == 1 {
            // rusqlite claims that this execute call returns rows even though
            // it does not, running it using query() fails as well. We catch the
            // error and check if it's the ExecuteReturnedResults error, if it
            // is we safely ignore it.

            let _ = sql_query("ALTER TABLE profiles RENAME TO profile").execute(conn);
            // match result {
            // Ok(_) => (),
            // Err(e) => match e {
            // rusqlite::Error::ExecuteReturnedResults => (),
            // _ => return Err(e.into()),
            // },
            // }
            let _ = sql_query("UPDATE version SET version = '2'").execute(conn);

            version = 2;
        }

        if version == 2 {
            let _ = conn.immediate_transaction::<_, diesel::result::Error, _>(|conn| {
                update(reindex_needed_table::table)
                    .set(reindex_needed_table::reindex_needed.eq(true))
                    .execute(conn)?;

                update(version_table::table)
                    .set(version_table::version.eq(3))
                    .execute(conn)?;
                Ok(())
            });
            reindex_needed = true;
            version = 3;
        }

        if version == 3 {
            let _ = conn.immediate_transaction::<_, diesel::result::Error, _>(|conn| {
                update(reindex_needed_table::table)
                    .set(reindex_needed_table::reindex_needed.eq(true))
                    .execute(conn)?;
                update(version_table::table)
                    .set(version_table::version.eq(4))
                    .execute(conn)?;
                Ok(())
            });
            reindex_needed = true;
            version = 4;
        }

        Ok((version, reindex_needed))
    }

    pub(crate) fn create_tables(conn: &mut WasmSqliteConnection) -> Result<()> {
        let _ = sql_query(
            "CREATE TABLE IF NOT EXISTS profile (
                id INTEGER NOT NULL PRIMARY KEY,
                user_id TEXT NOT NULL,
                displayname TEXT NOT NULL,
                avatar_url TEXT NOT NULL,
                UNIQUE(user_id,displayname,avatar_url)
            )",
        )
        .execute(conn);

        let _ = sql_query(
            "CREATE TABLE IF NOT EXISTS rooms (
                id INTEGER NOT NULL PRIMARY KEY,
                room_id TEXT NOT NULL,
                UNIQUE(room_id)
            )",
        )
        .execute(conn);

        let _ = sql_query(
            "CREATE TABLE IF NOT EXISTS events (
                id INTEGER NOT NULL PRIMARY KEY,
                event_id TEXT NOT NULL,
                sender TEXT NOT NULL,
                server_ts DATETIME NOT NULL,
                room_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                msgtype TEXT,
                source TEXT NOT NULL,
                profile_id INTEGER NOT NULL,
                FOREIGN KEY (profile_id) REFERENCES profile (id),
                FOREIGN KEY (room_id) REFERENCES rooms (id),
                UNIQUE(event_id, room_id)
            )",
        )
        .execute(conn);

        let _ = sql_query(
            "CREATE TABLE IF NOT EXISTS uncommitted_events (
                id INTEGER NOT NULL PRIMARY KEY,
                event_id INTEGER NOT NULL,
                content_value TEXT NOT NULL,
                FOREIGN KEY (event_id) REFERENCES events (id),
                UNIQUE(event_id)
            )",
        )
        .execute(conn);

        let _ = sql_query(
            "CREATE TABLE IF NOT EXISTS pending_deletion_events (
                id INTEGER NOT NULL PRIMARY KEY,
                event_id TEXT NOT NULL,
                UNIQUE(event_id)
            )",
        )
        .execute(conn);

        let _ = sql_query(
            "CREATE TABLE IF NOT EXISTS crawlercheckpoints (
                id INTEGER NOT NULL PRIMARY KEY,
                room_id TEXT NOT NULL,
                token TEXT NOT NULL,
                full_crawl BOOLEAN NOT NULL,
                direction TEXT NOT NULL,
                UNIQUE(room_id,token,full_crawl,direction)
            )",
        )
        .execute(conn);

        let _ = sql_query("CREATE INDEX IF NOT EXISTS event_profile_id ON events (profile_id)")
            .execute(conn);

        let _ = sql_query(
            "CREATE INDEX IF NOT EXISTS room_events_by_timestamp ON events (room_id, server_ts DESC, event_id)",
        )
        .execute(conn);

        let _ = sql_query("CREATE INDEX IF NOT EXISTS event_id ON events (event_id)").execute(conn);

        let _ =
            sql_query("CREATE INDEX IF NOT EXISTS room_events ON events (room_id, type, msgtype)")
                .execute(conn);

        let _ = sql_query(
            "CREATE TABLE IF NOT EXISTS user_version (
                id INTEGER NOT NULL PRIMARY KEY CHECK (id = 1),
                version INTEGER NOT NULL
            )",
        )
        .execute(conn);

        let _ =
            sql_query("INSERT OR IGNORE INTO user_version ( version ) VALUES(?1)").execute(conn);

        Ok(())
    }

    pub(crate) fn get_event_count() -> i64 {
        return 0;
        // connection.query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
    }

    pub(crate) fn get_event_count_for_room(
        // conn: &WasmSqliteConnection,
        room_id: &str,
    ) -> i64 {
        let room_id = Database::get_room_id(room_id);
        // connection.query_row(
        //     "SELECT COUNT(*) FROM events WHERE room_id=?1",
        //     [room_id],
        //     |row| row.get(0),
        // )
        0
    }

    pub(crate) fn get_room_count(// conn: &WasmSqliteConnection
    ) -> i64 {
        // TODO once we support upgraded rooms we should return only leaf rooms
        // here, rooms that are not ancestors to another one.
        // connection.query_row("SELECT COUNT(*) FROM rooms", [], |row| row.get(0))
        0
    }

    pub(crate) fn save_profile(
        // conn: &WasmSqliteConnection,
        user_id: &str,
        profile: &Profile,
    ) -> i64 {
        let displayname = profile.displayname.as_ref();
        let avatar_url = profile.avatar_url.as_ref();

        // unwrap_or_default doesn't work on references sadly.
        let displayname = if let Some(d) = displayname { d } else { "" };

        let avatar_url = if let Some(a) = avatar_url { a } else { "" };

        // connection.execute(
        //     "
        //     INSERT OR IGNORE INTO profile (
        //         user_id, displayname, avatar_url
        //     ) VALUES(?1, ?2, ?3)",
        //     [user_id, displayname, avatar_url],
        // )?;

        // let profile_id: i64 = connection.query_row(
        //     "
        //     SELECT id FROM profile WHERE (
        //         user_id=?1
        //         and displayname=?2
        //         and avatar_url=?3)",
        //     [user_id, displayname, avatar_url],
        //     |row| row.get(0),
        // )?;
        let profile_id: i64 = 0;
        profile_id
    }

    #[cfg(test)]
    pub(crate) fn load_profile(
        // conn: &PooledConnection<SqliteConnectionManager>,
        profile_id: i64,
    ) -> Result<Profile> {
        // let profile = connection.query_row(
        //     "SELECT displayname, avatar_url FROM profile WHERE id=?1",
        //     [profile_id],
        //     |row| {
        //         Ok(Profile {
        //             displayname: row.get(0)?,
        //             avatar_url: row.get(1)?,
        //         })
        //     },
        // )?;

        Ok(Profile {
            displayname: Some("".to_string()),
            avatar_url: Some("".to_string()),
        })
    }

    pub(crate) fn get_room_id(
        // conn: &WasmSqliteConnection,
        room: &str,
    ) -> i64 {
        // connection.execute("INSERT OR IGNORE INTO rooms (room_id) VALUES(?1)", [room])?;

        let room_id: i64 = 0;
        // connection.query_row("SELECT id FROM rooms WHERE (room_id=?1)", [room], |row| {
        //     row.get(0)
        // })?;

        room_id
    }

    pub(crate) fn load_pending_deletion_events(// conn: &WasmSqliteConnection,
    ) -> Vec<EventId> {
        // let mut stmt = connection.prepare("SELECT event_id from pending_deletion_events")?;
        // let events = stmt.query_map([], |row| row.get(0))?;

        // events.collect()
        return vec![];
    }

    pub(crate) fn load_uncommitted_events(// conn: &WasmSqliteConnection,
    ) -> Vec<(i64, Event)> {
        // let mut stmt = connection.prepare(
        //         "SELECT uncommitted_events.id, uncommitted_events.event_id, content_value, type, msgtype,
        //          events.event_id, sender, server_ts, rooms.room_id, source
        //          FROM uncommitted_events
        //          INNER JOIN events on events.id = uncommitted_events.event_id
        //          INNER JOIN rooms on rooms.id = events.room_id
        //          ")?;

        // let events = stmt.query_map([], |row| {
        //     Ok((
        //         row.get(0)?,
        //         Event {
        //             event_type: row.get(3)?,
        //             content_value: row.get(2)?,
        //             msgtype: row.get(4)?,
        //             event_id: row.get(5)?,
        //             sender: row.get(6)?,
        //             server_ts: row.get(7)?,
        //             room_id: row.get(8)?,
        //             source: row.get(9)?,
        //         },
        //     ))
        // })?;

        // events.collect()
        return vec![];
    }

    pub(crate) fn save_event_helper(
        // conn: &WasmSqliteConnection,
        event: &mut Event,
        profile_id: i64,
    ) -> i64 {
        let room_id = Database::get_room_id(&event.room_id);
        room_id
        // let mut statement = connection.prepare(
        //     "
        //     INSERT INTO events (
        //         event_id, sender, server_ts, room_id, type,
        //         msgtype, source, profile_id
        //     ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        // )?;

        // let event_id = statement.insert([
        //     &event.event_id,
        //     &event.sender,
        //     &event.server_ts as &dyn ToSql,
        //     &room_id as &dyn ToSql,
        //     &event.event_type as &dyn ToSql,
        //     &event.msgtype,
        //     &event.source,
        //     &profile_id as &dyn ToSql,
        // ])?;

        // let mut stmt = connection.prepare(
        //     "
        //     INSERT OR IGNORE INTO uncommitted_events (
        //         event_id, content_value
        //     ) VALUES (?1, ?2)",
        // )?;

        // let id = 0;
        // stmt.insert([&event_id as &dyn ToSql, &event.content_value])?;

        // id
    }

    pub(crate) fn save_event(
        // conn: &WasmSqliteConnection,
        event: &mut Event,
        profile: &mut Profile,
    ) -> Result<Option<i64>> {
        if Database::event_in_store(event) {
            return Ok(None);
        }

        let ret = Database::save_profile(&event.sender, profile);
        // let profile_id = match ret {
        //     Ok(p) => p,
        //     Err(e) => match e {
        //         rusqlite::Error::NulError(..) => {
        //             // A nul error is thrown because the string contains nul
        //             // bytes and converting it to a C string isn't possible this
        //             // way. This is likely some string containing malicious nul
        //             // bytes so we filter them out.
        //             profile.displayname = profile.displayname.as_mut().map(|d| d.replace('\0', ""));
        //             profile.avatar_url = profile.avatar_url.as_mut().map(|u| u.replace('\0', ""));
        //             Database::save_profile(&event.sender, profile)?
        //         }
        //         _ => return Err(e.into()),
        //     },
        // };
        // let ret = Database::save_event_helper(event, profile_id);

        // let event_id = match ret {
        //     Ok(e) => e,
        //     Err(e) => match e {
        //         rusqlite::Error::NulError(..) => {
        //             // Same deal for the event, but we need to delete the event
        //             // in case a transaction was used. Sqlite will otherwise
        //             // complain about the unique constraint that we have for
        //             // the event id.
        //             Database::delete_event_by_id(&event.event_id);
        //             event.content_value = event.content_value.replace('\0', "");
        //             event.msgtype = event.msgtype.as_mut().map(|m| m.replace('\0', ""));
        //             Database::save_event_helper(event, profile_id)?
        //         }
        //         _ => return Err(e.into()),
        //     },
        // };

        Ok(Some(0))
    }

    pub(crate) fn delete_event_by_id(
        // conn: &WasmSqliteConnection,
        event_id: &str,
    )
    // -> rusqlite::Result<usize>
    {
        // connection.execute("DELETE from events WHERE event_id == ?1", [event_id])
    }

    pub(crate) fn event_in_store(
        // conn: &WasmSqliteConnection,
        event: &Event,
    ) -> bool {
        let room_id = Database::get_room_id(&event.room_id);
        let count: i64 = 0;
        // connection.query_row(
        //     "
        //     SELECT COUNT(*) FROM events WHERE (
        //         event_id=?1
        //         and room_id=?2)",
        //     [&event.event_id, &room_id as &dyn ToSql],
        //     |row| row.get(0),
        // )?;

        match count {
            0 => false,
            1 => true,
            // This is fine becauese event_id and room_id are a unique pair in
            // our events table.
            _ => unreachable!(),
        }
    }

    pub(crate) fn load_all_events(
        // conn: &WasmSqliteConnection,
        limit: usize,
        from_event: Option<&Event>,
    ) -> Vec<SerializedEvent> {
        // match from_event {
        //     Some(event) => {
        //         let mut stmt = connection.prepare(
        //             "SELECT source FROM events
        //              WHERE (
        //                  (type == 'm.room.message') &
        //                  (event_id != ?1) &
        //                  (server_ts <= ?2)
        //              ) ORDER BY server_ts DESC LIMIT ?3
        //              ",
        //         )?;

        //         let events = stmt
        //             .query_map(params![&event.event_id, &event.server_ts, &limit,], |row| {
        //                 row.get(0)
        //             })?;
        //         events.collect()
        //     }
        //     None => {
        //         let mut stmt = connection.prepare(
        //             "SELECT source FROM events
        //              WHERE type == 'm.room.message'
        //              ORDER BY server_ts DESC LIMIT ?1
        //              ",
        //         )?;

        //         let events = stmt.query_map([limit], |row| row.get(0))?;
        //         events.collect()
        //     }
        // }
        return vec![];
    }

    pub(crate) fn load_file_events(
        // conn: &WasmSqliteConnection,
        room_id: &str,
        limit: usize,
        from_event: Option<&str>,
        direction: &LoadDirection,
    ) -> Vec<(SerializedEvent, Profile)> {
        // match from_event {
        //     Some(e) => {
        //         let event = Database::load_event(connection, room_id, e)?;

        //         let (direction, sort) = match direction {
        //             LoadDirection::Backwards => ("<=", "DESC"),
        //             LoadDirection::Forwards => (">=", "ASC"),
        //         };

        //         let mut stmt = connection.prepare(&format!(
        //             "SELECT source, displayname, avatar_url
        //              FROM events
        //              INNER JOIN profile on profile.id = events.profile_id
        //              WHERE (
        //                  (events.room_id == ?1) &
        //                  (type == 'm.room.message') &
        //                  (msgtype in ({})) &
        //                  (event_id != ?2) &
        //                  (server_ts {} ?3)
        //              ) ORDER BY server_ts {} LIMIT ?4
        //              ",
        //             FILE_EVENT_TYPES, direction, sort
        //         ))?;

        //         let room_id = Database::get_room_id(room_id)?;
        //         let events = stmt.query_map(
        //             params![&room_id, &event.event_id, &event.server_ts, &limit,],
        //             |row| {
        //                 Ok((
        //                     row.get(0)?,
        //                     Profile {
        //                         displayname: row.get(1)?,
        //                         avatar_url: row.get(2)?,
        //                     },
        //                 ))
        //             },
        //         )?;
        //         events.collect()
        //     }
        //     None => {
        //         let mut stmt = connection.prepare(&format!(
        //             "SELECT source, displayname, avatar_url
        //              FROM events
        //              INNER JOIN profile on profile.id = events.profile_id
        //              WHERE (
        //                  (events.room_id == ?1) &
        //                  (type == 'm.room.message') &
        //                  (msgtype in ({}))
        //              ) ORDER BY server_ts DESC LIMIT ?2
        //              ",
        //             FILE_EVENT_TYPES
        //         ))?;

        //         let room_id = Database::get_room_id(room_id)?;
        //         let events = stmt.query_map(params![room_id, limit], |row| {
        //             Ok((
        //                 row.get(0)?,
        //                 Profile {
        //                     displayname: row.get(1)?,
        //                     avatar_url: row.get(2)?,
        //                 },
        //             ))
        //         })?;
        //         events.collect()
        //     }
        // }
        vec![]
    }

    /// Load events surounding the given event.
    pub(crate) fn load_event_context(
        // conn: &WasmSqliteConnection,
        event: &Event,
        before_limit: usize,
        after_limit: usize,
    ) -> EventContext {
        let mut profiles: HashMap<String, Profile> = HashMap::new();
        // let room_id = Database::get_room_id(&event.room_id)?;
        let before: Vec<SerializedEvent> = vec![];
        let after: Vec<SerializedEvent> = vec![];
        // let before = if before_limit == 0 {
        //     vec![]
        // } else {
        //     let mut stmt = connection.prepare(
        //         "
        //         WITH room_events AS (
        //             SELECT *
        //             FROM events
        //             WHERE room_id == ?2
        //         )
        //         SELECT source, sender, displayname, avatar_url
        //         FROM room_events
        //         INNER JOIN profile on profile.id = room_events.profile_id
        //         WHERE (
        //             (event_id != ?1) &
        //             (server_ts <= ?3)
        //         ) ORDER BY server_ts DESC LIMIT ?4
        //         ",
        //     )?;
        //     let context = stmt.query_map(
        //         params![&event.event_id, &room_id, &event.server_ts, &after_limit,],
        //         |row| {
        //             Ok((
        //                 row.get(0),
        //                 row.get(1),
        //                 Profile {
        //                     displayname: row.get(2)?,
        //                     avatar_url: row.get(3)?,
        //                 },
        //             ))
        //         },
        //     )?;
        //     let mut ret: Vec<String> = Vec::new();

        //     for row in context {
        //         let (source, sender, profile) = row?;
        //         profiles.insert(sender?, profile);
        //         ret.push(source?)
        //     }

        //     ret
        // };

        // let after = if after_limit == 0 {
        //     vec![]
        // } else {
        //     let mut stmt = connection.prepare(
        //         "
        //         WITH room_events AS (
        //             SELECT *
        //             FROM events
        //             WHERE room_id == ?2
        //         )
        //         SELECT source, sender, displayname, avatar_url
        //         FROM room_events
        //         INNER JOIN profile on profile.id = room_events.profile_id
        //         WHERE (
        //             (event_id != ?1) &
        //             (server_ts >= ?3)
        //         ) ORDER BY server_ts ASC LIMIT ?4
        //         ",
        //     )?;
        //     let context = stmt.query_map(
        //         params![&event.event_id, &room_id, &event.server_ts, &after_limit,],
        //         |row| {
        //             Ok((
        //                 row.get(0),
        //                 row.get(1),
        //                 Profile {
        //                     displayname: row.get(2)?,
        //                     avatar_url: row.get(3)?,
        //                 },
        //             ))
        //         },
        //     )?;

        //     let mut ret: Vec<String> = Vec::new();

        //     for row in context {
        //         let (source, sender, profile) = row?;
        //         profiles.insert(sender?, profile);
        //         ret.push(source?)
        //     }

        //     ret
        // };

        (before, after, profiles)
    }

    pub(crate) fn load_event(
        // conn: &WasmSqliteConnection,
        room_id: &str,
        event_id: &str,
    ) -> Event {
        let room_id = Database::get_room_id(room_id);

        // connection.query_row(
        //     "SELECT type, msgtype, event_id, sender,
        //      server_ts, rooms.room_id, source
        //      FROM events
        //      INNER JOIN rooms on rooms.id = events.room_id
        //      WHERE (events.room_id == ?1) & (event_id == ?2)",
        //     [&room_id as &dyn ToSql, &event_id],
        //     |row| {
        //         Ok(Event {
        //             event_type: row.get(0)?,
        //             content_value: "".to_string(),
        //             msgtype: row.get(1)?,
        //             event_id: row.get(2)?,
        //             sender: row.get(3)?,
        //             server_ts: row.get(4)?,
        //             room_id: row.get(5)?,
        //             source: row.get(6)?,
        //         })
        //     },
        // )
        Event {
            event_type: EventType::Message,
            content_value: "".to_string(),
            msgtype: Some("".to_string()),
            event_id: "".to_string(),
            sender: "".to_string(),
            server_ts: 0,
            room_id: "".to_string(),
            source: "".to_string(),
        }
    }

    pub(crate) fn load_events(
        // conn: &WasmSqliteConnection,
        search_result: &[(f32, EventId)],
        before_limit: usize,
        after_limit: usize,
        order_by_recency: bool,
    ) -> Vec<SearchResult> {
        if search_result.is_empty() {
            return vec![];
        }

        let event_num = search_result.len();
        let parameter_str = ", ?".repeat(event_num - 1);

        // let mut stmt = if order_by_recency {
        //     connection.prepare(&format!(
        //         "SELECT type, msgtype, event_id, sender,
        //          server_ts, rooms.room_id, source, displayname, avatar_url
        //          FROM events
        //          INNER JOIN profile on profile.id = events.profile_id
        //          INNER JOIN rooms on rooms.id = events.room_id
        //          WHERE event_id IN (?{})
        //          ORDER BY server_ts DESC
        //          ",
        //         &parameter_str
        //     ))?
        // } else {
        //     connection.prepare(&format!(
        //         "SELECT type, msgtype, event_id, sender,
        //          server_ts, rooms.room_id, source, displayname, avatar_url
        //          FROM events
        //          INNER JOIN profile on profile.id = events.profile_id
        //          INNER JOIN rooms on rooms.id = events.room_id
        //          WHERE event_id IN (?{})
        //          ",
        //         &parameter_str
        //     ))?
        // };

        let (mut scores, event_ids): (HashMap<String, f32>, Vec<String>) = {
            let mut s = HashMap::new();
            let mut e = Vec::new();

            for (score, event_id) in search_result {
                e.push(event_id.clone());
                s.insert(event_id.clone(), *score);
            }
            (s, e)
        };

        // let db_events = stmt.query_map(params_from_iter(event_ids), |row| {
        //     Ok((
        //         Event {
        //             event_type: row.get(0)?,
        //             content_value: "".to_string(),
        //             msgtype: row.get(1)?,
        //             event_id: row.get(2)?,
        //             sender: row.get(3)?,
        //             server_ts: row.get(4)?,
        //             room_id: row.get(5)?,
        //             source: row.get(6)?,
        //         },
        //         Profile {
        //             displayname: row.get(7)?,
        //             avatar_url: row.get(8)?,
        //         },
        //     ))
        // })?;

        let mut events = Vec::new();
        // for row in db_events {
        //     let (event, profile): (Event, Profile) = row?;
        //     let (before, after, profiles) =
        //         Database::load_event_context(connection, &event, before_limit, after_limit)?;

        //     let mut profiles = profiles;
        //     profiles.insert(event.sender.clone(), profile);

        //     let result = SearchResult {
        //         score: scores.remove(&event.event_id).unwrap(),
        //         event_source: event.source,
        //         events_before: before,
        //         events_after: after,
        //         profile_info: profiles,
        //     };
        //     events.push(result);
        // }

        // Sqlite orders by recency for us, but if we score by rank sqlite will
        // mess up our order, re-sort our events here.
        if !order_by_recency {
            // events.sort_by(|a, b| a.score.partial_cmp(&b.score).unwrap_or(Ordering::Equal));
        }

        events
    }

    pub(crate) fn replace_crawler_checkpoint(
        // conn: &WasmSqliteConnection,
        new: Option<&CrawlerCheckpoint>,
        old: Option<&CrawlerCheckpoint>,
    ) {
        // if let Some(checkpoint) = new {
        //     connection.execute(
        //         "INSERT OR IGNORE INTO crawlercheckpoints
        //         (room_id, token, full_crawl, direction) VALUES(?1, ?2, ?3, ?4)",
        //         [
        //             &checkpoint.room_id,
        //             &checkpoint.token,
        //             &checkpoint.full_crawl as &dyn ToSql,
        //             &checkpoint.direction,
        //         ],
        //     )?;
        // }

        // if let Some(checkpoint) = old {
        //     connection.execute(
        //         "DELETE FROM crawlercheckpoints
        //         WHERE (room_id=?1 AND token=?2 AND full_crawl=?3 AND direction=?4)",
        //         [
        //             &checkpoint.room_id,
        //             &checkpoint.token,
        //             &checkpoint.full_crawl as &dyn ToSql,
        //             &checkpoint.direction,
        //         ],
        //     )?;
        // }

        // Ok(())
    }
}
