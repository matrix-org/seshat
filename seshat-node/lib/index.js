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
 * @property {string} displayname The users display name, if one is set.
 * @property {string} avatar_url The users avatar url, if one is set.
 */


/**
 * @typedef checkpoint
 * @type {Object}
 * @property {string} roomId The unique id of the room that this checkpoint
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
 * @param {object} config Additional configuration for the database.
 * database already exist in the given folder the database will be reused.
 * @param  {string} config.language The language that the database should use
 * for indexing. Picking the correct indexing language may improve the search.
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
     * @param  {matrixProfile} profile The user profile of the sender at the
     * time the event was sent.
     *
     * @return {Array.<searchResult>} The array of events that matched the
     * search term.
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
     * @param  {object} args Arguments object for the search.
     * @param  {string} args.searchTerm The term that is used to search the
     * database.
     * @param  {number} args.limit The maximum number of events that the search
     * should return.
     * @param  {number} args.before_limit The number of events to fetch that
     * preceded the event that matched the search term.
     * @param  {number} args.after_limit The number of events to fetch that
     * followed the event that matched the search term.
     * @param  {boolean} args.order_by_recency Should the search results be
     * ordered by event recency.
     *
     * @return {Promise<Array.<searchResult>>} The array of events that matched
     * the search term.
     */
    async search(args) {
        return new Promise((resolve, reject) => {
            this.searchAsync(args, (err, res) => {
                if (err) reject(err);
                else resolve(res);
            });
        });
    }

    /**
     * Search the database for events using the given search term.
     *
     * @param  {string} term The term that is used to search the database.
     * @param  {number} limit The maximum number of events that the search
     * should return.
     * @param  {number} before_limit The number of events to fetch that
     * preceded the event that matched the search term.
     * @param  {number} after_limit The number of events to fetch that followed
     * the event that matched the search term.
     * @param  {boolean} order_by_recency Should the search results be ordered
     * by event recency.
     *
     * @return {Array.<searchResult>} The array of events that matched the
     * search term.
     */
    searchSync(term, limit = 10, before_limit = 0, after_limit = 0,
        order_by_recency = false) {
        return super.searchSync(term, limit, before_limit, after_limit,
            order_by_recency);
    }

    /**
     * Add a batch of events from the room history to the database.
     *
     * @param  {array<matrixEvent>} events An array of events that will be
     * added to the database.
     * @param  {checkpoint} newCheckpoint
     * @param  {checkpoint} oldCheckPoint
     *
     * @return {boolean} True if the added events were already in the store,
     * false otherwise.
     */
    addHistoricEventsSync(events, newCheckpoint = null, oldCheckPoint = null) {
        return super.addHistoricEventsSync(events, newCheckpoint,
            oldCheckPoint);
    }

    /**
     * Add a batch of events from the room history to the database.
     *
     * @param  {array<matrixEvent>} events An array of events that will be
     * added to the database.
     * @param  {checkpoint} newCheckpoint
     * @param  {checkpoint} oldCheckPoint
     *
     * @return {Promise<boolean>} A promise that will resolve to true if all
     * the events have already been added to the database, false otherwise.
     */
    async addHistoricEvents(events, newCheckpoint = null,
        oldCheckPoint = null) {
        return new Promise((resolve, reject) => {
            super.addHistoricEvents(
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

    /**
     * Add a message crawler checkpoint.
     *
     * @param  {checkpoint} checkpoint
     *
     * @return {Promise} A promise that will resolve when the checkpoint has
     * been stored in the database.
     */
    async addCrawlerCheckpoint(checkpoint) {
        return this.addHistoricEvents([], checkpoint);
    }

    /**
     * Remove a message crawler checkpoint.
     * @param  {checkpoint} checkpoint
     *
     * @return {Promise} A promise that will resolve when the checkpoint has
     * been removed from the database.
     */
    async removeCrawlerCheckpoint(checkpoint) {
        return this.addHistoricEvents([], null, checkpoint);
    }

    /**
     * Load the stored crawler checkpoints.
     * @param  {checkpoint} checkpoint
     *
     * @return {Promise<Array.<checkpoint>>} A promise that will resolve to an
     * array of checkpoints when they are loaded from the database.
     */
    async loadCheckpoints() {
        return new Promise((resolve, reject) => {
            super.loadCheckpoints((err, res) => {
                if (err) reject(err);
                else resolve(res);
            });
        });
    }

    /**
     * Load the stored crawler checkpoints.
     * @param  {checkpoint} checkpoint
     *
     * @return {Promise<Array.<checkpoint>>} A promise that will resolve to an
     * array of checkpoints when they are loaded from the database.
     */
    async getSize() {
        return new Promise((resolve, reject) => {
            super.getSize((err, res) => {
                if (err) reject(err);
                else resolve(res);
            });
        });
    }

    /**
     * Delete the Seshat database.
     *
     * @return {Promise} A promise that will resolve when the database has
     * been deleted.
     */
    async delete() {
        return new Promise((resolve, reject) => {
            super.delete((err, res) => {
                if (err) reject(err);
                else resolve(res);
            });
        });
    }

    /**
     * Delete the Seshat database.
     *
     * @return {Promise<boolean>} A promise that will resolve to true if the
     * database is empty, that is it doesn't contain any events, false
     * otherwise.
     */
    async isEmpty() {
        return new Promise((resolve, reject) => {
            super.isEmpty((err, res) => {
                if (err) reject(err);
                else resolve(res);
            });
        });
    }
}

module.exports = Seshat;
