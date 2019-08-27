const fs = require('fs');
const os = require('os');
const path = require('path');
const expect = require('chai').expect;
const assert = require('assert');

const Seshat = require('../');

const matrixEvent = {
  event_id: '$15163622445EBvZJ:localhost',
  room_id: '!TESTROOM',
  sender: '@alice:example.org',
  content: {
    body: 'Test message',
  },
  origin_server_ts: 1516362244026,
};

const matrixProfile = {
  display_name: 'Alice (from wonderland)',
  avatar_url: '',
};

const matrixProfileOnlyDisplayName = {
  display_name: 'Alice (from wonderland)',
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
    let ret = db.commitSync(true);
    assert.equal(ret, 1);

    ret = db.commitSync(false);
    assert.equal(ret, undefined);

    ret = db.commitSync();
    assert.equal(ret, undefined);
  });

  it('should allow events to be commited asynchronously', function(done) {
    const db = createDb();

    db.commitAsync(function(err, value) {
      if (err) done(err);
      else {
        assert.equal(value, 1);
        done();
      }
    });
  });

  it('should allow events to be commited using a promise', async function() {
    const db = createDb();
    const opstamp = await db.commit();
    assert.equal(opstamp, 1);
  });

  it('should return a search result for the stored event', async function() {
    const db = createDb();
    db.addEvent(matrixEvent);

    const opstamp = await db.commit();
    assert.equal(opstamp, 1);
    await db.commit();
    db.reload();

    const results = db.searchSync('Test');
    assert.notEqual(Object.entries(results).length, 0);
    assert.deepEqual(results[0].result, matrixEvent);
  });

  it('should return a search result for the stored event using promises', async function() {
    const db = createDb();
    db.addEvent(matrixEvent);

    const opstamp = await db.commit();
    assert.equal(opstamp, 1);
    await db.commit();
    await db.commit();

    const results = await db.search('Test');
    assert.notEqual(Object.entries(results).length, 0);
    assert.deepEqual(results[0].result, matrixEvent);
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
