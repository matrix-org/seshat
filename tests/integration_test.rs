#[macro_use]
extern crate lazy_static;

use seshat::{
    CheckpointDirection, CrawlerCheckpoint, Database, Event, EventType, LoadConfig, LoadDirection,
    Profile, SearchConfig,
};

#[cfg(feature = "encryption")]
use seshat::Config;

use std::path::Path;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{mpsc, Once};
use std::time::Duration;
use std::{fs, iter, thread};
use tempfile::tempdir;

use fake::{faker::internet::raw::*, locales::*, Fake};
use log::{info, warn};
use rand::Rng;
use serde_json::json;

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
        &format!("${}:{}", (0..10).fake::<u8>(), domain),
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

    let result = db.search("Test", &Default::default()).unwrap().results;
    assert!(!result.is_empty());
    assert_eq!(result[0].event_source, EVENT.source);
}

#[test]
fn search_with_room() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(tmpdir.path()).unwrap();
    let profile = Profile::new("Alice", "");

    db.add_event(EVENT.clone(), profile);
    db.force_commit().unwrap();
    db.reload().unwrap();

    let cases = [
        ("\"Test message\"", true),
        ("Test message", true),
        ("Test anything", true),
        ("anything message", true),
        ("Test", true),
        ("message", true),
        ("massage", false),
        ("\"Test massage\"", false),
    ];

    for (phrase, should_match) in cases.iter() {
        let result = db
            .search(phrase, SearchConfig::new().for_room("!test_room:localhost"))
            .unwrap()
            .results;
        assert!(
            should_match == &!result.is_empty(),
            "searching for '{}' should not return a result, but found {}",
            phrase,
            result[0].event_source
        );
        if *should_match {
            assert_eq!(result[0].event_source, EVENT.source);
        }
    }
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
    let result = searcher
        .search("Test", &Default::default())
        .unwrap()
        .results;
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

fn make_random_string(length: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut rng = rand::thread_rng();
    let one_char = || CHARSET[rng.gen_range(0..CHARSET.len())] as char;
    iter::repeat_with(one_char).take(length).collect()
}

/// A test which adds some events (including replacesments) to the database, and meanwhile
/// messes with the access permissions on the database files to try to simulate Windows Defender
/// or similar.
///
/// Regression test for https://github.com/matrix-org/seshat/issues/173.
#[test]
fn race_indexer_and_virus_scanner() {
    init_logging();

    let tmpdir = tempdir().unwrap();
    let indexdir = tmpdir.path().to_owned();
    let mut db = Database::new(&indexdir).unwrap();

    // A thread which runs every few milliseconds, and removes write access to all the '.idx' and
    // '.pos' files.
    //
    // The thread automatically exits when the test completes.
    let (_tx, rx) = mpsc::channel::<()>();
    thread::spawn(move || loop {
        // Sleep for a while, but if the test thread disconnects, abort immediately and terminate
        // the thread.
        if let Err(RecvTimeoutError::Disconnected) = rx.recv_timeout(Duration::from_millis(50)) {
            break;
        }

        // Find all the .pos and .idx files, and mark them as readonly.
        for path in fs::read_dir(&indexdir)
            .expect("unable to open index directory")
            .map(|f| f.expect("unable to read index directory entry").path())
            .filter(|p| {
                p.extension()
                    .is_some_and(|e| e.to_string_lossy() == "pos" || e.to_string_lossy() == "idx")
            })
        {
            if let Err(e) = set_readonly(&path, true) {
                // Likely the file moved under us
                warn!(
                    "failed to set file {} readonly: {}",
                    path.to_string_lossy(),
                    e
                );
            }
        }
    });

    let profile = Profile::new("Alice", "");
    let sender = format!(
        "@{}:{}",
        Username(EN).fake::<String>(),
        FreeEmailProvider(EN).fake::<String>()
    );

    for cycle in 0..10 {
        let mut events = Vec::new();

        for ev in 0..10 {
            // A unique event id, because duplicates are ignored
            let event_id = format!("${}_{}:domain", cycle, ev);

            // A reasonably unique string for the body
            let body = make_random_string(8);

            let mut event_source = json!({ "event_id": event_id });

            // Some events replace earlier ones
            if cycle > 0 && rand::random::<u8>() > 200 {
                let original_event_id = format!("${}_{}:domain", cycle - 1, ev);
                info!("{} replaces {}", event_id, original_event_id);
                event_source.as_object_mut().unwrap().insert(
                    "content".to_owned(),
                    json!({ "m.relates_to": { "rel_type": "m.replace", "event_id": original_event_id }}),
                );
            }

            let event = Event::new(
                EventType::Message,
                &body,
                Some("m.text"),
                &event_id,
                &sender,
                1516362244026,
                &format!("!test_room_{}:localhost", cycle % 10),
                &event_source.to_string(),
            );

            db.add_event(event.clone(), profile.clone());
            events.push(event);
        }

        db.force_commit()
            .unwrap_or_else(|e| warn!("Failed to commit event batch: {}", e));
    }
}

/// Set readonly status of the given file.
///
/// On Windows, uses the `icacls` command to deny our user access to the files.
#[cfg(windows)]
fn set_readonly(path: &std::path::PathBuf, readonly: bool) -> std::io::Result<()> {
    use std::io::ErrorKind;
    use std::process::Command;

    let user = std::env::var("USERNAME").unwrap();
    let mut args = Vec::new();
    args.push(path.to_str().unwrap().to_owned());
    if readonly {
        args.extend_from_slice(&["/deny".to_owned(), format!("{user}:(W)")])
    } else {
        args.extend_from_slice(&["/remove:d".to_owned(), user])
    }
    let status = Command::new("icacls").args(args).status()?;
    if !status.success() {
        return Err(std::io::Error::new(ErrorKind::Other, "icacls failed"));
    }

    Ok(())
}

/// Set readonly status of the given file.
///
/// On unix-like OSes, just does chmod a-w.
#[cfg(not(windows))]
fn set_readonly(path: &std::path::PathBuf, readonly: bool) -> std::io::Result<()> {
    let mut perms = fs::metadata(path)?.permissions();

    if perms.readonly() != readonly {
        info!("Setting {} readonly {}", path.to_string_lossy(), readonly);
        perms.set_readonly(readonly);
        fs::set_permissions(path, perms)?;
    }

    Ok(())
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
    let result = searcher
        .search("Test", &SearchConfig::new())
        .unwrap()
        .results;
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
        .search("Test", SearchConfig::new().with_key(EventType::Topic))
        .unwrap()
        .results;
    assert!(result.is_empty());

    db.add_event(TOPIC_EVENT.clone(), profile);
    db.force_commit().unwrap();
    db.reload().unwrap();

    let searcher = db.get_searcher();
    let result = searcher
        .search("Test", SearchConfig::new().with_key(EventType::Topic))
        .unwrap()
        .results;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].event_source, TOPIC_EVENT.source)
}

#[test]
#[cfg(not(windows))] // Fails with "The process cannot access the file because it is being used by another process."
fn delete() {
    let tmpdir = tempdir().unwrap();
    let path: &Path = tmpdir.path();

    assert!(path.exists());

    let db = Database::new(tmpdir.path()).unwrap();
    db.delete().unwrap();

    assert!(!path.exists());
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_save_and_search() {
    let tmpdir = tempdir().unwrap();
    let db_config = Config::new().set_passphrase("wordpass");
    let mut db = Database::new_with_config(tmpdir.path(), &db_config).unwrap();
    let profile = Profile::new("Alice", "");

    db.add_event(EVENT.clone(), profile);
    db.force_commit().unwrap();
    db.reload().unwrap();

    let result = db.search("Test", &Default::default()).unwrap().results;
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

#[test]
fn delete_events() {
    let tmpdir = tempdir().unwrap();
    let mut db = Database::new(tmpdir.path()).unwrap();
    let profile = Profile::new("Alice", "");

    db.add_event(EVENT.clone(), profile.clone());
    db.add_event(TOPIC_EVENT.clone(), profile);
    db.force_commit().unwrap();
    db.reload().unwrap();

    let searcher = db.get_searcher();
    let result = searcher
        .search("Test", &SearchConfig::new())
        .unwrap()
        .results;
    assert_eq!(result.len(), 2);

    let receiver = db.delete_event(&EVENT.event_id);
    let result = receiver.recv().unwrap();
    result.unwrap();
    db.force_commit().unwrap();
    db.reload().unwrap();

    let result = searcher
        .search("Test", &SearchConfig::new())
        .unwrap()
        .results;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].event_source, TOPIC_EVENT.source);
}

fn init_logging() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = env_logger::try_init_from_env(env_logger::Env::default().default_filter_or("warn"));
    });
}
