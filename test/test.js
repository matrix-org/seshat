const fs = require('fs');
const os = require('os');
const path = require('path');
const expect = require('chai').expect
const assert = require('assert');

const Seshat = require('../');

var matrixEvent = {
  event_id: "$15163622445EBvZJ:localhost",
  room_id: "!TESTROOM",
  sender: "@alice:example.org",
  content: {
    body: "Test message",
  },
  origin_server_ts: 1516362244026,
};

var matrixProfile = {
  display_name: "Alice (from wonderland)",
  avatar_url: "",
}

var matrixProfileOnlyDisplayName =  {
  display_name: "Alice (from wonderland)",
}

var badEvent = {
  event_id: "$15163622445EBvZJ:localhost",
  room_id: "!TESTROOM",
  sender: "@alice:example.org",
  content: {
    body: "Test message",
  },
  origin_server_ts: "1516362244026",
};

function create_db() {
  var tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'seshat-'));
  var db = new Seshat(tempDir);

  return db
}

describe('Database', function() {
  it('should be created succesfully.', function() {
    var tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'seshat-'));
    var db = new Seshat(tempDir);
  });

  var db = create_db()

  it('should allow the addition of events.', function() {
    db.add_event(matrixEvent, matrixProfile);
  });

  it('should allow the addition of an event without a profile.', function() {
    db.add_event(matrixEvent);
  });

  it('should allow the addition of an event with a profile that only contains a display name.', function() {
    db.add_event(matrixEvent, matrixProfileOnlyDisplayName);
  });

  it('should allow events to be commited', function() {
    var db = create_db()
    var ret = db.commit(true)
    assert.equal(ret, 1)

    var ret = db.commit(false)
    assert.equal(ret, undefined)

    var ret = db.commit()
    assert.equal(ret, undefined)
  });

  it('should allow events to be commited asynchronously', function(done) {
    var db = create_db()

    db.commit_async(function(err, value) {
      if (err) done(err);
      else {
        assert.equal(value, 1);
        done();
      }
    })
  });

  it('should allow events to be commited using a promise', async function() {
    var db = create_db()
    var opstamp = await db.commit_promise();
    assert.equal(opstamp, 1);
  });

  it('should throw an error when adding events with missing fields.', function() {
    delete matrixEvent.content;
    expect(() => db.add_event(matrixEvent, matrixProfile)).to.throw("Event doesn't contain any content");

    delete matrixEvent.room_id;
    expect(() => db.add_event(matrixEvent, matrixProfile)).to.throw("Event doesn't contain a valid room id");

    delete matrixEvent.origin_server_ts;
    expect(() => db.add_event(matrixEvent, matrixProfile)).to.throw("Event doesn't contain a valid timestamp");

    delete matrixEvent.event_id;
    expect(() => db.add_event(matrixEvent, matrixProfile)).to.throw("Event doesn't contain a valid event id");

    delete matrixEvent.sender;
    expect(() => db.add_event(matrixEvent, matrixProfile)).to.throw("Event doesn't contain a valid sender");
  });

  it('should throw an error when adding events with fields that don\'t typecheck.', function() {
    expect(() => db.add_event(badEvent, matrixProfile)).to.throw("Event doesn't contain a valid timestamp");
  });

});
