const fs = require('fs');
const os = require('os');
const path = require('path');

const {Seshat, ReindexError, SeshatRecovery} = require('../');

const matrixEvent = {
    type: 'm.room.message',
    event_id: '$15163622445EBvZB:localhost',
    room_id: '!TESTROOM',
    sender: '@alice:example.org',
    content: {
        body: 'Test message',
        msgtype: 'm.text',
    },
    origin_server_ts: 1516362244026,
};

const fileEvent = {
    type: 'm.room.message',
    event_id: '$15163622476EBvZB:localhost',
    room_id: '!TESTROOM',
    sender: '@alice:example.org',
    content: {
        body: 'Test file',
        msgtype: 'm.file',
    },
    origin_server_ts: 1516362244028,
};

const imageEvent = {
    type: 'm.room.message',
    event_id: '$15163622481EBvZB:localhost',
    room_id: '!TESTROOM',
    sender: '@alice:example.org',
    content: {
        body: 'Test image',
        msgtype: 'm.image',
    },
    origin_server_ts: 1516362244048,
};

const videoEvent = {
    type: 'm.room.message',
    event_id: '$15163622481Evideo:localhost',
    room_id: '!TESTROOM',
    sender: '@alice:example.org',
    content: {
        body: 'Test video',
        msgtype: 'm.video',
    },
    origin_server_ts: 1516362244100,
};

const beforeMatrixEvent = {
    type: 'm.room.message',
    event_id: '$15163622445EBvFA:localhost',
    room_id: '!TESTROOM',
    sender: '@alice:example.org',
    content: {
        body: 'Another test message before on',
        msgtype: 'm.text',
    },
    origin_server_ts: 1516352244100,
};

const laterMatrixEvent = {
    type: 'm.room.message',
    event_id: '$15163622445EBvFC:localhost',
    room_id: '!TESTROOM',
    sender: '@alice:example.org',
    content: {
        body: 'Another test message later on',
        msgtype: 'm.text',
    },
    origin_server_ts: 1516372244100,
};

const topicEvent = {
    type: 'm.room.topic',
    event_id: '$15163622445EBvZE:localhost',
    room_id: '!TESTROOM',
    sender: '@alice:example.org',
    content: {
        topic: 'Test topic',
    },
    origin_server_ts: 1516362244026,
};

const nameEvent = {
    type: 'm.room.name',
    event_id: '$15163622445EBvZN:localhost',
    room_id: '!TESTROOM',
    sender: '@alice:example.org',
    content: {
        name: 'Test room',
    },
    origin_server_ts: 1516362244068,
};

const matrixEventRoom2 = {
    type: 'm.room.message',
    event_id: '$15163622515EBvZJ:localhost',
    room_id: '!TESTROOM2',
    sender: '@alice:example.org',
    content: {
        body: 'Test message',
        msgtype: 'm.text',
    },
    origin_server_ts: 1516362244064,
};

const matrixProfile = {
    displayname: 'Alice (from wonderland)',
    avatar_url: '',
};

const matrixProfileOnlyDisplayName = {
    displayname: 'Alice (from wonderland)',
};

const badEvent = {
    event_id: '$15163622445EBvZJ:localhost',
    room_id: '!TESTROOM',
    sender: '@alice:example.org',
    content: {
        body: 'Test message',
        msgtype: 'm.text',
    },
    origin_server_ts: '1516362244026',
};

function createDb() {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'seshat-'));
    const db = new Seshat(tempDir);

    return db;
}

const exampleEvents = [
  {event: matrixEvent, profile: matrixProfileOnlyDisplayName}
]

const checkPoint = {
    roomId: '!TESTROOM',
    token: '1234',
    fullCrawl: false,
    direction: "f",
}

describe('Database', function() {
    it('should be created succesfully.', function() {
        const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'seshat-'));
        const db = new Seshat(tempDir);
    });

    const db = createDb();

    it('should allow the addition of events.', function() {
        db.addEvent(matrixEvent, matrixProfile);
    });

    it('should allow the addition of an event without a profile.', function() {
        db.addEvent(matrixEvent);
    });

    it('should allow the addition of an event with a profile that only contains a display name.', function() {
        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);
    });

    it('should allow events to be commited', function() {
        const db = createDb();
        db.commitSync(true, true);

        ret = db.commitSync(false);
        expect(ret).toBeUndefined();

        ret = db.commitSync();
        expect(ret).toBeUndefined();
    });

    it('should allow events to be commited using a promise', async function() {
        const db = createDb();
        await db.commit(true);
    });

    it('should return a search result for the stored event', async function() {
        const db = createDb();
        db.addEvent(matrixEvent);

        await db.commit(true);
        db.reload();

        const results = db.searchSync({search_term:'Test'});
        expect(results.count).not.toBe(0);
        expect(results.results[0].result).toEqual(matrixEvent);
    });

    it('should return a search result for the stored event using promises', async function() {
        const db = createDb();
        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);

        await db.commit(true);
        db.reload();

        const results = await db.search({search_term: 'Test'});
        // console.log(results)
        // console.log(results[0].context.profile_info)
        // console.log(results[0].result.content)
        expect(results.count).not.toBe(0);
        expect(results.results[0].result).toEqual(matrixEvent);
    });

    it('should allow messages from the backlog to be added in a batched way', async function() {
        const db = createDb();
        let ret = db.addHistoricEventsSync(exampleEvents, checkPoint);
        expect(ret).toBeFalsy();

        db.reload();
        const results = await db.search({search_term: 'Test'});
        expect(Object.entries(results).length).not.toBe(0);

        let ret2 = db.addHistoricEventsSync(exampleEvents, checkPoint);
        expect(ret2).toBeTruthy();
    });

    it('should allow messages from the backlog to be added using a promise', async function() {
        const db = createDb();
        let ret = await db.addHistoricEvents(exampleEvents, checkPoint)
        expect(ret).toBeFalsy();
        db.reload();

        const results = await db.search({search_term: 'Test'});
        expect(Object.entries(results).length).not.toBe(0);

        const checkpoints = await db.loadCheckpoints();
        expect(checkpoints[0]).toEqual(checkPoint);

        let ret2 = await db.addHistoricEvents(exampleEvents, checkPoint)
        expect(ret2).toBeTruthy();
    });

    it('should allow to search events in a specific room', async function() {
        const db = createDb();
        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);
        db.addEvent(matrixEventRoom2, matrixProfileOnlyDisplayName);

        await db.commit(true);
        db.reload();

        const results = await db.search({
            search_term: 'Test',
            room_id: '!TESTROOM',
        });
        expect(results.count).toBe(1);
        expect(results.results[0].result).toEqual(matrixEvent);
    });

    it('should allow us to sort the search results by recency', async function() {
        const db = createDb();
        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);
        db.addEvent(laterMatrixEvent, matrixProfileOnlyDisplayName);
        db.addEvent(beforeMatrixEvent, matrixProfileOnlyDisplayName);

        await db.commit(true);
        db.reload();

        const results = await db.search({
            search_term: 'Test',
            order_by_recency: true
        });
        expect(results.count).toBe(3);
        expect(results.results[0].result).toEqual(laterMatrixEvent);
        expect(results.results[1].result).toEqual(matrixEvent);
        expect(results.results[2].result).toEqual(beforeMatrixEvent);
    });

    it('should sort the search results by rank by default', async function() {
        const db = createDb();
        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);
        db.addEvent(laterMatrixEvent, matrixProfileOnlyDisplayName);
        db.addEvent(beforeMatrixEvent, matrixProfileOnlyDisplayName);

        await db.commit(true);
        db.reload();

        const results = await db.search({search_term: 'Test'});

        expect(results.count).toBe(3);
        expect(results.results[0].rank).toBeLessThanOrEqual(results.results[1].rank);
        expect(results.results[1].rank).toBeLessThanOrEqual(results.results[2].rank);
    });

    it('should allow us to get the size of the database', async function() {
        const db = createDb();
        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);
        db.addEvent(matrixEventRoom2, matrixProfileOnlyDisplayName);

        await db.commit(true);
        let size = await db.getSize();
        expect(size).toBeGreaterThan(0)
    });

    it('should allow us to add different event types', async function() {
        const db = createDb();
        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);
        db.addEvent(topicEvent, matrixProfileOnlyDisplayName);
        db.addEvent(nameEvent, matrixProfileOnlyDisplayName);

        await db.commit(true);
        db.reload();
    });

    it('should allow us to search with a specific key', async function() {
        const db = createDb();
        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);

        await db.commit(true);
        db.reload();

        let results = await db.search({
            search_term: 'Test',
            keys: ["content.topic"]
        });
        expect(results.count).toBe(0);

        db.addEvent(topicEvent);
        await db.commit(true);
        db.reload();

        results = await db.search({
            search_term: 'Test',
            keys: ["content.topic"]
        });
        expect(results.count).toBe(1);
        expect(results.results[0].result).toEqual(topicEvent);

        results = await db.search({
            search_term: 'Test',
        });
        expect(results.count).toBe(2);
    });

    it('should allow us to create a db with a specific language', async function() {
        const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'seshat-'));
        expect(() => new Seshat(tempDir, {language: "unknown"})).toThrow('Unsuported language: unknown');

        const db = new Seshat(tempDir, {language: "german"});

        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);
        await db.commit(true);
        db.reload();

        results = await db.search({
            search_term: 'Test',
        });
        expect(results.count).toBe(1);
    });

    it('should allow us to delete the db', async function() {
        const db = createDb();
        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);

        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);
        await db.commit(true);
        db.reload();

        await db.delete()

        expect(() => db
            .addEvent(matrixEvent, matrixProfileOnlyDisplayName))
            .toThrow('Database has been closed or deleted');
    });

    it('should allow us to check if the db is empty', async function() {
        const db = createDb();
        expect(await db.isEmpty()).toBeTruthy();

        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);
        await db.commit(true);

        expect(await db.isEmpty()).toBeFalsy();
    });

    it('should allow us to create an encrypted db', async function() {
        const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'seshat-'));
        let db = new Seshat(tempDir, {passphrase: "wordpass"});

        expect(await db.isEmpty()).toBeTruthy();
        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);
        await db.commit(true);
        expect(await db.isEmpty()).toBeFalsy();

        expect(() => db = new Seshat(tempDir)).toThrow('');
    });

    it('should allow us to load events that contain files from the db', async function() {
        const db = createDb();
        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);
        db.addEvent(fileEvent, matrixProfileOnlyDisplayName);
        db.addEvent(imageEvent, matrixProfileOnlyDisplayName);

        await db.commit(true);
        let events = await db.loadFileEvents({roomId: fileEvent.room_id, limit: 10})
        expect(events.length).toBe(2);

        events = await db.loadFileEvents({roomId: fileEvent.room_id, limit: 1})
        expect(events.length).toBe(1);
        expect(events[0].event).toEqual(imageEvent);

        events = await db.loadFileEvents({roomId: fileEvent.room_id, limit: 10, fromEvent: imageEvent.event_id})
        expect(events.length).toBe(1);
        expect(events[0].event).toEqual(fileEvent);
    });

    it('should allow us to continue loading file events in both directions', async function() {
        const db = createDb();
        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);
        db.addEvent(fileEvent, matrixProfileOnlyDisplayName);
        db.addEvent(imageEvent, matrixProfileOnlyDisplayName);
        db.addEvent(videoEvent, matrixProfileOnlyDisplayName);

        await db.commit(true);

        // Get the first event.
        let events = await db.loadFileEvents({roomId: fileEvent.room_id, limit: 1})
        expect(events.length).toBe(1);

        // Get the next two.
        events = await db.loadFileEvents({roomId: fileEvent.room_id, limit: 10, fromEvent: videoEvent.event_id})
        expect(events.length).toBe(2);
        expect(events[0].event).toEqual(imageEvent);
        expect(events[1].event).toEqual(fileEvent);

        // Try to get a newer one than the last one.
        events = await db.loadFileEvents({roomId: fileEvent.room_id, limit: 10, fromEvent: videoEvent.event_id, direction: "forwards"})
        expect(events.length).toBe(0);

        // Get the two newer events than the last one.
        events = await db.loadFileEvents({roomId: fileEvent.room_id, limit: 10, fromEvent: fileEvent.event_id, direction: "forwards"})
        expect(events.length).toBe(2);
        expect(events[0].event).toEqual(imageEvent);
        expect(events[1].event).toEqual(videoEvent);
    });

    it('should allow us query the database for statistics', async function() {
        const db = createDb();

        let stats = await db.getStats(true);
        expect(stats.eventCount).toBe(0);
        expect(stats.roomCount).toBe(0);

        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);
        db.addEvent(fileEvent, matrixProfileOnlyDisplayName);
        db.addEvent(imageEvent, matrixProfileOnlyDisplayName);
        db.addEvent(videoEvent, matrixProfileOnlyDisplayName);

        await db.commit(true);
        stats = await db.getStats(true);
        expect(stats.eventCount).toBe(4);
        expect(stats.roomCount).toBe(1);
        expect(stats.size).toBeGreaterThan(0);
    });

    it('should allow us to delete events from the database/index', async function() {
        const db = createDb();
        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);

        await db.commit(true);
        db.reload();

        let results = await db.search({search_term: 'Test'});
        expect(results.count).toBe(1);
        expect(results.results[0].result).toEqual(matrixEvent);

        let deleted = await db.deleteEvent(matrixEvent.event_id);
        expect(deleted).toBeFalsy();
        await db.commit(true);
        db.reload();

        results = await db.search({search_term: 'Test'});
        expect(results.count).toBe(0);


        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);
        db.addEvent(fileEvent, matrixProfileOnlyDisplayName);

        await db.commit(true);
        db.reload();

        results = await db.search({search_term: 'Test'});
        expect(results.count).toBe(2);

        deleted = await db.deleteEvent(matrixEvent.event_id);
        expect(deleted).toBeFalsy();
        await db.commit(true);
        db.reload();

        results = await db.search({search_term: 'Test'});
        expect(results.count).toBe(1);
        expect(results.results[0].result).toEqual(fileEvent);
    });

    it('should throw an error when adding events with missing fields.', function() {
        delete matrixEvent.content;
        expect(() => db.addEvent(matrixEvent, matrixProfile)).toThrow('Event doesn\'t contain any content');

        delete matrixEvent.room_id;
        expect(() => db.addEvent(matrixEvent, matrixProfile)).toThrow('Event doesn\'t contain a valid room id');

        delete matrixEvent.origin_server_ts;
        expect(() => db.addEvent(matrixEvent, matrixProfile)).toThrow('Event doesn\'t contain a valid timestamp');

        delete matrixEvent.event_id;
        expect(() => db.addEvent(matrixEvent, matrixProfile)).toThrow('Event doesn\'t contain a valid event id');

        delete matrixEvent.sender;
        expect(() => db.addEvent(matrixEvent, matrixProfile)).toThrow('Event doesn\'t contain a valid sender');
    });

    it('should throw an error when adding events with fields that don\'t typecheck.', function() {
        expect(() => db.addEvent(badEvent, matrixProfile)).toThrow('Event doesn\'t contain a valid timestamp');
    });

    it('should allow us to reindex a database', async function() {
        const dir = '../data/database/v2';
        expect(() => new Seshat(dir)).toThrow(ReindexError);

        const recovery = new SeshatRecovery(dir);
        await recovery.reindex();

        const db = new Seshat(dir);
        const results = await db.search({search_term: 'Hello'});
        expect(results.count).not.toBe(0);
    });
});
