#[macro_use]
extern crate lazy_static;

use seshat::{
    CheckpointDirection, Config, CrawlerCheckpoint, Database, Event, EventType, LoadConfig,
    LoadDirection, Profile, SearchConfig,
};

use std::path::Path;
use tempfile::tempdir;

use fake::faker::internet::raw::*;
use fake::locales::*;
use fake::Fake;

pub static EVENT_SOURCE: &str = "{
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
}";

pub static TOPIC_EVENT_SOURCE: &str = "{
    content: {
        topic: Test topic
    },
    event_id: $15163622448EBvZJ:localhost,
    origin_server_ts: 1516362244050,
    sender: @example2:localhost,
    type: m.room.topic,
    unsigned: {age: 43289803098},
    user_id: @example2:localhost,
    age: 43289803098
}";

pub static FILE_SOURCE: &str = "{
    content: {
        body: Test File,
        msgtype: m.file,
    },
    event_id: $15163622445EBvZJ:localhost,
    origin_server_ts: 1516362244026,
    sender: @example2:localhost,
    type: m.room.message,
    unsigned: {age: 43289803095},
    user_id: @example2:localhost,
    age: 43289803095
}";

pub static IMAGE_SOURCE: &str = "{
    content: {
        body: Test image,
        msgtype: m.image,
    },
    event_id: $15163622445EBvZJ:localhost,
    origin_server_ts: 1516362244026,
    sender: @example2:localhost,
    type: m.room.message,
    unsigned: {age: 43289803095},
    user_id: @example2:localhost,
    age: 43289803095
}";

pub static VIDEO_SOURCE: &str = "{
    content: {
        body: Test video,
        msgtype: m.video,
    },
    event_id: $15163622449Ebeoj:localhost,
    origin_server_ts: 1516362244100,
    sender: @example2:localhost,
    type: m.room.message,
    unsigned: {age: 43289803095},
    user_id: @example2:localhost,
    age: 43289803095
}";

lazy_static! {
    pub static ref EVENT: Event = Event::new(
        EventType::Message,
        "Test message",
        Some("m.text"),
        "$15163622445EBvZJ:localhost",
        "@example2:localhost",
        151636_2244026,
        "!test_room:localhost",
        EVENT_SOURCE,
    );
}

lazy_static! {
    pub static ref FILE_EVENT: Event = Event::new(
        EventType::Message,
        "Test file",
        Some("m.file"),
        "$15163622468file:localhost",
        "@example2:localhost",
        151636_2244000,
        "!test_room:localhost",
        FILE_SOURCE,
    );
}

lazy_static! {
    pub static ref IMAGE_EVENT: Event = Event::new(
        EventType::Message,
        "Test image",
        Some("m.image"),
        "$15163622471image:localhost",
        "@example2:localhost",
        151636_2244050,
        "!test_room:localhost",
        IMAGE_SOURCE,
    );
}

lazy_static! {
    pub static ref VIDEO_EVENT: Event = Event::new(
        EventType::Message,
        "Test video",
        Some("m.video"),
        "$15163622449video:localhost",
        "@example2:localhost",
        151636_2244100,
        "!test_room:localhost",
        VIDEO_SOURCE,
    );
}

lazy_static! {
    pub static ref TOPIC_EVENT: Event = Event::new(
        EventType::Topic,
        "Test topic",
        None,
        "$15163622445EBvZE:localhost",
        "@example2:localhost",
        151636_2244038,
        "!test_room:localhost",
        TOPIC_EVENT_SOURCE,
    );
}

fn fake_event() -> Event {
    let domain: String = FreeEmailProvider(EN).fake();

    Event::new(
        EventType::Message,
        "Hello world",
        Some("m.text"),
        &format!("${}:{}", (0..10).fake::<u8>(), &domain),
        &format!(
            "@{}:{}",
            Username(EN).fake::<String>(),
            FreeEmailProvider(EN).fake::<String>()
        ),
        151636_2244026,
        "!test_room:localhost",
        EVENT_SOURCE,
    )
}

#[test]
fn create_db() {
    let tmpdir = tempdir().unwrap();
    let _db = Database::new(tmpdir.path()).unwrap();
}

#[test]
fn save_and_search() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(tmpdir.path()).unwrap();
    let profile = Profile::new("Alice", "");

    db.add_event(EVENT.clone(), profile);
    db.force_commit().unwrap();
    db.reload().unwrap();

    let result = db.search("Test", &Default::default()).unwrap();
    assert!(!result.is_empty());
    assert_eq!(result[0].event_source, EVENT.source);
}

#[test]
fn duplicate_events() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(tmpdir.path()).unwrap();
    let profile = Profile::new("Alice", "");

    db.add_event(EVENT.clone(), profile.clone());
    db.add_event(EVENT.clone(), profile);

    db.force_commit().unwrap();
    db.reload().unwrap();

    let searcher = db.get_searcher();
    let result = searcher.search("Test", &Default::default()).unwrap();
    assert_eq!(result.len(), 1);
}

#[test]
fn save_and_search_historic_events() {
    let tmpdir = tempdir().unwrap();
    let db = Database::new(tmpdir.path()).unwrap();
    let profile = Profile::new("Alice", "");

    let mut events = Vec::new();

    for i in 1..6 {
        let mut event: Event = fake_event();
        event.server_ts = EVENT.server_ts - i;
        event.source = format!("Hello before event {}", i);
        events.push((event, profile.clone()));
    }

    let checkpoint = CrawlerCheckpoint {
        room_id: "!test:room".to_string(),
        token: "1234".to_string(),
        full_crawl: false,
        direction: CheckpointDirection::Backwards,
    };

    let receiver = db.add_historic_events(events, Some(checkpoint.clone()), None);
    let ret = receiver.recv().unwrap();
    assert!(ret.is_ok());
    let connection = db.get_connection().unwrap();

    let checkpoints = connection.load_checkpoints().unwrap();
    assert!(checkpoints.contains(&checkpoint));
}

#[test]
fn get_size() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(tmpdir.path()).unwrap();

    let profile = Profile::new("Alice", "");

    db.add_event(EVENT.clone(), profile.clone());

    let mut before_event = None;

    for i in 1..6 {
        let mut event: Event = fake_event();
        event.server_ts = EVENT.server_ts - i;
        event.source = format!("Hello before event {}", i);

        if before_event.is_none() {
            before_event = Some(event.clone());
        }

        db.add_event(event, profile.clone());
    }
    db.force_commit().unwrap();
    assert!(db.get_size().unwrap() > 0);
}

#[test]
fn add_differing_events() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(tmpdir.path()).unwrap();
    let profile = Profile::new("Alice", "");

    db.add_event(EVENT.clone(), profile.clone());
    db.add_event(TOPIC_EVENT.clone(), profile);
    db.force_commit().unwrap();
    db.reload().unwrap();

    let searcher = db.get_searcher();
    let result = searcher.search("Test", &SearchConfig::new()).unwrap();
    assert_eq!(result.len(), 2);
}

#[test]
fn search_with_specific_key() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(tmpdir.path()).unwrap();
    let profile = Profile::new("Alice", "");
    let searcher = db.get_searcher();

    db.add_event(EVENT.clone(), profile.clone());
    db.force_commit().unwrap();
    db.reload().unwrap();

    let result = searcher
        .search("Test", &SearchConfig::new().with_key(EventType::Topic))
        .unwrap();
    assert!(result.is_empty());

    db.add_event(TOPIC_EVENT.clone(), profile);
    db.force_commit().unwrap();
    db.reload().unwrap();

    let searcher = db.get_searcher();
    let result = searcher
        .search("Test", &SearchConfig::new().with_key(EventType::Topic))
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].event_source, TOPIC_EVENT.source)
}

#[test]
fn delete() {
    let tmpdir = tempdir().unwrap();
    let path: &Path = tmpdir.path();

    assert!(path.exists());

    let db = Database::new(tmpdir.path()).unwrap();
    db.delete().unwrap();

    assert!(!path.exists());
}

#[test]
fn encrypted_save_and_search() {
    let tmpdir = tempdir().unwrap();
    let db_config = Config::new().set_passphrase("wordpass");
    let mut db = Database::new_with_config(tmpdir.path(), &db_config).unwrap();
    let profile = Profile::new("Alice", "");

    db.add_event(EVENT.clone(), profile);
    db.force_commit().unwrap();
    db.reload().unwrap();

    let result = db.search("Test", &Default::default()).unwrap();
    assert!(!result.is_empty());
    assert_eq!(result[0].event_source, EVENT.source);
}

#[test]
fn load_file_events() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(tmpdir.path()).unwrap();
    let profile = Profile::new("Alice", "");

    db.add_event(EVENT.clone(), profile.clone());
    db.add_event(FILE_EVENT.clone(), profile.clone());
    db.add_event(IMAGE_EVENT.clone(), profile);
    db.force_commit().unwrap();
    db.reload().unwrap();

    let connection = db.get_connection().unwrap();

    let mut config = LoadConfig::new(&FILE_EVENT.room_id).limit(10);

    let result = connection
        .load_file_events(&config)
        .expect("Can't load file events");
    assert!(!result.is_empty());
    assert!(result.len() == 2);
    assert_eq!(result[0].0, IMAGE_EVENT.source);
    assert!(result.len() == 2);
    assert_eq!(result[1].0, FILE_EVENT.source);

    config = config.limit(1);

    let result = connection
        .load_file_events(&config)
        .expect("Can't load file events");
    assert!(!result.is_empty());
    assert!(result.len() == 1);
    assert_eq!(result[0].0, IMAGE_EVENT.source);

    config = config.from_event(&IMAGE_EVENT.event_id);

    let result = connection
        .load_file_events(&config)
        .expect("Can't load file events with token");

    assert!(!result.is_empty());
    assert!(result.len() == 1);
    assert_eq!(result[0].0, FILE_EVENT.source);
}

#[test]
fn load_file_events_directions() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(tmpdir.path()).unwrap();
    let profile = Profile::new("Alice", "");

    db.add_event(EVENT.clone(), profile.clone());
    db.add_event(FILE_EVENT.clone(), profile.clone());
    db.add_event(IMAGE_EVENT.clone(), profile.clone());
    db.add_event(VIDEO_EVENT.clone(), profile);
    db.force_commit().unwrap();
    db.reload().unwrap();

    let connection = db.get_connection().unwrap();

    // Get the newest event.
    let mut config = LoadConfig::new(&FILE_EVENT.room_id).limit(1);
    let result = connection.load_file_events(&config).unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, VIDEO_EVENT.source);

    // Get the next two.
    config = config.from_event(&VIDEO_EVENT.event_id).limit(10);
    let result = connection.load_file_events(&config).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].0, IMAGE_EVENT.source);
    assert_eq!(result[1].0, FILE_EVENT.source);

    // Try to get a newer one than the last one.
    config = config.direction(LoadDirection::Forwards);
    let result = connection.load_file_events(&config).unwrap();
    assert!(result.is_empty());

    // Get the two newer events than the last one.
    config = config.from_event(&FILE_EVENT.event_id);
    let result = connection.load_file_events(&config).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].0, IMAGE_EVENT.source);
    assert_eq!(result[1].0, VIDEO_EVENT.source);
}
