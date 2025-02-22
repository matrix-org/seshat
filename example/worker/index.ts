import init, {
    initThreadPool,
    Language,
    Database,
    new_seshat_db,
    Config,
    Event,
    EventType,
    Profile,
    SearchConfig,
} from '../../bindings/seshat-wasm/pkg/seshat.js';

(async () => {
    const wasm = await init()
    await initThreadPool(navigator.hardwareConcurrency)
    let config = new Config(Language.Arabic, "password")
    let db = await new_seshat_db("", config)

    try {

        let event = new Event(EventType.Message, "The quick brown fox jumps over the lazy dog", "m.text", `${Math.random()}:localhost`, "@alice:example.org", 1516362244026n, "!TESTROOM", "")
        let event2 = new Event(EventType.Message, "Lorum ipsum blah jumps the blah blah", "m.text", `${Math.random()}:localhost`, "@alice:example.org", 1516362212345n, "!TESTROOM", "")
        let event3 = new Event(EventType.Message, "My name is david and i'm going to jump too.", "m.text", `${Math.random()}:localhost`, "@alice:example.org", 151636221999n, "!TESTROOM", "")
        let profile = new Profile("Alice", undefined)
        let profile2 = new Profile("Alice2", undefined)
        let profile3 = new Profile("Alice3", undefined)
        db.add_event(event, profile)
        db.force_commit()
        db.add_event(event2, profile2)
        db.force_commit()
        db.add_event(event3, profile3)
        db.force_commit()
        // db.commit()
        db.reload();
    }
    catch (e) {
        console.log("Error");
        console.log((e as Error).message);
    }


    self.onmessage = async (event) => {
        console.log("onmessage")
        if (event.data == "search") {
            console.log("calling search")
            let batch = await db.search("jumps")
            console.log("batch")
            console.log(batch)
            console.log(batch.count)
            console.log(batch.next_batch)
            for (const result of batch.results) {
                console.log("result.score", result.score)
                console.log("result.event_source", result.event_source)
                console.log("result.profile_info", result.profile_info)
                console.log("result.events_after", result.events_after)
                console.log("result.events_after", result.events_after)
            }

        }
    };
})();