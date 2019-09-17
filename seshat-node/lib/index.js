// Copyright 2019 The Matrix.org Foundation CIC
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

const seshat = require('../native');


/**
 * @typedef searchResult
 * @type {Object}
 * @property {number} rank The rank of the search result.
 * @property {Object} matrixEvent The full event of the search result.
 */


/**
 * @typedef matrixEvent
 * @type {Object}
 * @property {string} event_id The unique ID of the event.
 * @property {string} sender The MXID of the user who sent this event.
 * @property {string} room_id The ID of the room where the event was sent.
 * @property {number} origin_server_ts The timestamp in milliseconds of the
 * originating homeserver when this event was sent.
 * @property {Object} content The content of the event. The content needs to
 * have either a body, topic or name key.
 */


/**
 * @typedef matrixProfile
 * @type {Object}
 * @property {string} display_name The users display name, if one is set.
 * @property {string} avatar_url The users avatar url, if one is set.
 */


/**
 * @typedef checkpoint
 * @type {Object}
 * @property {string} room_id The unique id of the room that this checkpoint
 * belongs to.
 * @property {string} token The token that can be used to fetch more events for
 * the given room.
 */


/**
 * Seshat database.<br>
 *
 * A Seshat database can be used to store and index Matrix events. A full-text
 * search can be done on the database retrieving events that match a search
 * query.
 *
 * @param {string} path The path where the database should be stored. If a
 * database already exist in the given folder the database will be reused.
 *
 * @constructor
 *
 * @example
 * // create a Seshat database in the given folder
 * let db = new Seshat("/home/example/database_dir");
 * // Add a Matrix event to the database.
 * db.addEvent(textEvent, profile);
 * // Commit events waiting in the queue to the database.
 * await db.commit();
 * // Search the database for messages containing the word 'Test'
 * let results = await db.search('Test');
 */
class Seshat extends seshat.Seshat {
  /**
   * Add an event to the database.
   *
   * This method adds an event only to a queue. To write the events to the
   * database the <code>commit()</code> methods needs to be called.
   *
   * @param  {matrixEvent} matrixEvent A Matrix event that should be added to
   * the database.
   * @param  {matrixProfile} profile The user profile of the sender at the time
   * the event was sent.
   *
   * @return {Array.<searchResult>} The array of events that matched the search
   * term.
   */
  addEvent(matrixEvent, profile = {}) {
    return super.addEvent(matrixEvent, profile);
  };

  /**
   * Commit the queued up events to the database.
   *
   * This is the asynchronous equivalent of the <code>commitSync()</code>
   * method.
   *
   * @return {Promise<number>} The latest stamp of the commit. The stamp is
   * a unique incrementing number that identifies the commit.
   */
  async commit() {
    return new Promise((resolve, reject) => {
      this.commitAsync((err, res) => {
        resolve(res);
      });
    });
  }

  /**
   * Commit the queued up events to the database.
   *
   * @param  {boolean} wait Wait for the events to be commited. If true will
   * block until the events are commited.
   *
   * @return {number} The latest stamp of the commit. The stamp is a unique
   * incrementing number that identifies the commit.
   */
  commitSync(wait = false) {
    return super.commitSync(wait);
  }

  /**
   * Reload the indexer of the database to reflect the changes of the last
   * commit. A reload will happen automatically, this method is mainly useful
   * for unit testing purposes to force a reload before a search.
   */
  reload() {
    super.reload();
  };

  /**
   * Search the database for events using the given search term.
   * This is the asynchronous equivalent of the <code>searchSync()</code>
   * method.
   *
   * @param  {string} term The term that is used to search the database.
   * @param  {number} limit The maximum number of events that the search should
   * return.
   * @param  {number} before_limit The number of events to fetch that preceded
   * the event that matched the search term.
   * @param  {number} after_limit The number of events to fetch that followed
   * the event that matched the search term.
   *
   * @return {Promise<Array.<searchResult>>} The array of events that matched
   * the search term.
   */
  async search(term, limit = 10, before_limit = 0, after_limit = 0) {
    return new Promise((resolve) => {
      this.searchAsync(term, limit, before_limit, after_limit, (err, res) => {
        resolve(res);
      });
    });
  }

  /**
   * Search the database for events using the given search term.
   * @param  {string} term The term that is used to search the database.
   * @param  {number} limit The maximum number of events that the search should
   * return.
   * @param  {number} before_limit The number of events to fetch that preceded
   * the event that matched the search term.
   * @param  {number} after_limit The number of events to fetch that followed
   * the event that matched the search term.
   *
   * @return {Array.<searchResult>} The array of events that matched the search
   * term.
   */
  searchSync(term, limit = 10, before_limit = 0, after_limit = 0) {
    return super.searchSync(term, limit, before_limit, after_limit);
  }

  /**
   * Add a batch of events from the room history to the database.
   * @param  {array<matrixEvent>} events An array of events that will be added
   * to the database.
   * @param  {checkpoint} newCheckpoint
   * @param  {checkpoint} oldCheckPoint
   *
   * @return {Array.<searchResult>} The array of events that matched the search
   * term.
   */
  addBacklogEventsSync(events, newCheckpoint = null, oldCheckPoint = null) {
    return super.addBacklogEventsSync(events, newCheckpoint, oldCheckPoint);
  }

  async addBacklogEvents(events, newCheckpoint = null, oldCheckPoint = null) {
    return new Promise((resolve, reject) => {
      super.addBacklogEvents(
          events,
          newCheckpoint,
          oldCheckPoint,
          (err, res) => {
            if (err) reject(err);
            else resolve(res);
          }
      );
    });
  }

  async addBacklogCheckpoint(checkpoint) {
    return this.addBacklogEvents([], checkpoint);
  }

  async removeBacklogCheckpoint(checkpoint) {
    return this.addBacklogEvents([], null, checkpoint);
  }

  async loadCheckpoints() {
    return new Promise((resolve) => {
      super.loadCheckpoints((err, res) => {
        if (err) reject(err);
        else resolve(res);
      });
    });
  }
}

module.exports = Seshat;
