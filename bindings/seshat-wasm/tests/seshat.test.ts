import { expect, test, beforeEach } from 'vitest'
import init, {
    initThreadPool,
    Language,
    Database,
    new_seshat_db,
    Config,
    Event,
    EventType,
    Profile,
} from '../pkg/seshat.js';


beforeEach(async () => {
    await init();
    // await initThreadPool(navigator.hardwareConcurrency);
});

test('adds 1 + 2 to equal 3', async () => {
    let config = new Config(Language.Arabic, "password")

    let db = new_seshat_db("", config)

    let event = new Event(EventType.Message, "message", "m.room.message", "1", "dave.blah", 9007199254740991n, "room123", "")
    let profile = new Profile("dave", undefined)
    // db.add_event(event, profile)
    expect(Language.Arabic).toBe(Language.Arabic)
})