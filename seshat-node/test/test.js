const fs = require('fs');
const os = require('os');
const path = require('path');
const expect = require('chai').expect;
const assert = require('chai').assert;

const Seshat = require('../');

const matrixEvent = {
    type: 'm.room.message',
    event_id: '$15163622445EBvZJ:localhost',
    room_id: '!TESTROOM',
    sender: '@alice:example.org',
    content: {
        body: 'Test message',
    },
    origin_server_ts: 1516362244026,
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
        db.commitSync(true);

        ret = db.commitSync(false);
        assert.equal(ret, undefined);

        ret = db.commitSync();
        assert.equal(ret, undefined);
    });

    it('should allow events to be commited asynchronously', function(done) {
        const db = createDb();

        db.commitAsync(function(err, value) {
        if (err) done(err);
        else done();
        });
    });

    it('should allow events to be commited using a promise', async function() {
        const db = createDb();
        await db.commit();
    });

    it('should return a search result for the stored event', async function() {
        const db = createDb();
        db.addEvent(matrixEvent);

        await db.commit();
        await db.commit();
        db.reload();

        const results = db.searchSync({search_term:'Test'});
        assert.notEqual(results.count, 0);
        assert.deepEqual(results.results[0].result, matrixEvent);
    });

    it('should return a search result for the stored event using promises', async function() {
        const db = createDb();
        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);

        await db.commit();
        await db.commit();

        const results = await db.search({search_term: 'Test'});
        // console.log(results)
        // console.log(results[0].context.profile_info)
        // console.log(results[0].result.content)
        assert.notEqual(results.count, 0);
        assert.deepEqual(results.results[0].result, matrixEvent);
    });

    it('should allow messages from the backlog to be added in a batched way', async function() {
        const db = createDb();
        let ret = db.addHistoricEventsSync(exampleEvents, checkPoint);
        assert.equal(ret, false);

        db.reload();
        const results = await db.search({search_term: 'Test'});
        assert.notEqual(Object.entries(results).length, 0);

        let ret2 = db.addHistoricEventsSync(exampleEvents, checkPoint);
        assert.equal(ret2, true);
    });

    it('should allow messages from the backlog to be added using a promise', async function() {
        const db = createDb();
        let ret = await db.addHistoricEvents(exampleEvents, checkPoint)
        assert.equal(ret, false);
        db.reload();

        const results = await db.search({search_term: 'Test'});
        assert.notEqual(Object.entries(results).length, 0);

        const checkpoints = await db.loadCheckpoints();
        assert.deepEqual(checkpoints[0], checkPoint);

        let ret2 = await db.addHistoricEvents(exampleEvents, checkPoint)
        assert.equal(ret2, true);
    });

    it('should allow to search events in a specific room', async function() {
        const db = createDb();
        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);
        db.addEvent(matrixEventRoom2, matrixProfileOnlyDisplayName);

        await db.commit();
        await db.commit();
        await db.commit();

        const results = await db.search({
            search_term: 'Test',
            roomId: '!TESTROOM',
        });
        assert.equal(results.count, 1);
        assert.deepEqual(results.results[0].result, matrixEvent);
    });

    it('should allow us to get the size of the database', async function() {
        const db = createDb();
        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);
        db.addEvent(matrixEventRoom2, matrixProfileOnlyDisplayName);

        await db.commit();
        let size = await db.getSize();
        assert.isAbove(size, 0)
    });

    it('should allow us to add different event types', async function() {
        const db = createDb();
        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);
        db.addEvent(topicEvent, matrixProfileOnlyDisplayName);
        db.addEvent(nameEvent, matrixProfileOnlyDisplayName);

        await db.commit();
        db.reload();
    });

    it('should allow us to search with a specific key', async function() {
        const db = createDb();
        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);

        await db.commit();
        db.reload();

        let results = await db.search({
            search_term: 'Test',
            keys: ["content.topic"]
        });
        assert.equal(results.count, 0);

        db.addEvent(topicEvent);
        await db.commit();
        db.reload();

        results = await db.search({
            search_term: 'Test',
            keys: ["content.topic"]
        });
        assert.equal(results.count, 1);
        assert.deepEqual(results.results[0].result, topicEvent);

        results = await db.search({
            search_term: 'Test',
        });
        assert.equal(results.count, 2);
    });

    it('should allow us to create a db with a specific language', async function() {
        const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'seshat-'));
        expect(() => new Seshat(tempDir, {language: "unknown"})).to.throw('Unsuported language: unknown');

        const db = new Seshat(tempDir, {language: "german"});

        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);
        await db.commit();
        db.reload();

        results = await db.search({
            search_term: 'Test',
        });
        assert.equal(results.count, 1);
    });

    it('should allow us to delete the db', async function() {
        const db = createDb();
        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);

        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);
        await db.commit();
        db.reload();

        await db.delete()

        expect(() => db.addEvent(matrixEvent, matrixProfileOnlyDisplayName)).to.throw('Database has been deleted');
    });

    it('should allow us to check if the db is empty', async function() {
        const db = createDb();
        assert(await db.isEmpty());

        db.addEvent(matrixEvent, matrixProfileOnlyDisplayName);
        await db.commit();

        assert(!await db.isEmpty());
    });

    it('should throw an error when adding events with missing fields.', function() {
        delete matrixEvent.content;
        expect(() => db.addEvent(matrixEvent, matrixProfile)).to.throw('Event doesn\'t contain any content');

        delete matrixEvent.room_id;
        expect(() => db.addEvent(matrixEvent, matrixProfile)).to.throw('Event doesn\'t contain a valid room id');

        delete matrixEvent.origin_server_ts;
        expect(() => db.addEvent(matrixEvent, matrixProfile)).to.throw('Event doesn\'t contain a valid timestamp');

        delete matrixEvent.event_id;
        expect(() => db.addEvent(matrixEvent, matrixProfile)).to.throw('Event doesn\'t contain a valid event id');

        delete matrixEvent.sender;
        expect(() => db.addEvent(matrixEvent, matrixProfile)).to.throw('Event doesn\'t contain a valid sender');
    });

    it('should throw an error when adding events with fields that don\'t typecheck.', function() {
        expect(() => db.addEvent(badEvent, matrixProfile)).to.throw('Event doesn\'t contain a valid timestamp');
    });
});
