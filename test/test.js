const fs = require('fs');
const os = require('os');
const path = require('path');
const expect = require('chai').expect

const seshat = require('../');

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

var badEvent = {
  event_id: "$15163622445EBvZJ:localhost",
  room_id: "!TESTROOM",
  sender: "@alice:example.org",
  content: {
    body: "Test message",
  },
  origin_server_ts: "1516362244026",
};

describe('Database', function() {
  it('should be created succesfully.', function() {
    var tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'seshat-'));
    var db = new seshat.Seshat(tempDir);
  });

  var tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'seshat-'));
  var db = new seshat.Seshat(tempDir);

  it('should allow the addition of events.', function() {
      db.add_event(matrixEvent, matrixProfile);
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
