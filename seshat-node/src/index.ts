// Copyright 2019 The Matrix.org Foundation C.I.C.
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
import { promisify } from "node:util";

const seshatNative = require("../index.node");

interface MatrixEvent {
  /**
   * The unique ID of the event.
   */
  event_id: string;
  /**
   * The MXID of the user who sent this event.
   */
  sender: string;
  /**
   * The ID of the room where the event was sent.
   */
  room_id: string;
  /**
   * The timestamp in milliseconds of the originating homeserver when this event was sent.
   */
  origin_server_ts: number;
  /**
   * The content of the event. The content needs to have either a body, topic or name key.
   */
  content: Record<string, unknown>;
}

interface MatrixProfile {
  /**
   * The users display name, if one is set.
   */
  displayname?: string;
  /**
   * The users avatar url, if one is set.
   */
  avatar_url?: string;
}

interface SearchContext {
  /**
   * Events that happened before the search result.
   */
  events_before: Array<MatrixEvent>;
  /**
   * Events that happened after the search result.
   */
  events_after: Array<MatrixEvent>;
  /**
   * The historic profile information of the users that sent the events returned.
   */
  profile_info: { [userId: string]: MatrixProfile };
}

interface SingleResult {
  /**
   * The rank of the search result.
   */
  rank: number;
  /**
   * The full event of the search result.
   */
  result: MatrixEvent;
  /**
   * The context of the result, containing events before and after the result.
   */
  searchContext: SearchContext;
}

interface SearchResult {
  /**
   * A token that can be used to grab more results.
   */
  next_batch: string;
  /**
   * The total number of results that were found.
   */
  count: number;
  /**
   * The list of results that was found.
   */
  results: Array<SingleResult>;
}

interface SearchArgs {
  /**
   * The term that is used to search the database.
   */
  searchTerm: string;
  /**
   * The maximum number of events that the search should return.
   */
  limit: number;
  /**
   * The number of events to fetch that
   * preceded the event that matched the search term.
   */
  before_limit: number;
  /**
   * The number of events to fetch that
   * followed the event that matched the search term.
   */
  after_limit: number;
  /**
   * Should the search results be ordered by event recency.
   */
  order_by_recency: boolean;
  /**
   * The token to request the next page of results.
   */
  next_batch: string;
}

interface Checkpoint {
  /**
   * The unique id of the room that this checkpoint belongs to.
   */
  roomId: string;
  /**
   * The token that can be used to fetch more events for the given room.
   */
  token: string;
  /**
   * Is this checkpoint of a crawl that should re-crawl the complete room history.
   */
  fullCrawl: boolean;
  /**
   * The crawl direction of the checkpoint.
   */
  direction: "b" | "f";
}

interface LoadResult {
  /**
   * A matrix event that was loaded from the database.
   */
  matrixEvent: MatrixEvent;
  /**
   * The profile of the sender at the time the event was sent.
   */
  matrixProfile: MatrixProfile;
}

interface DatabaseStats {
  /**
   * The number of bytes that the database is consuming on the disk.
   */
  size: number;
  /**
   * The number events that are stored in the database.
   */
  eventCount: number;
  /**
   * The number of rooms the database knows about.
   */
  roomCount: number;
}

interface RecoveryInfo {
  /**
   * The total number of events that the database holds.
   */
  totalEvents: number;
  /**
   * The number of events that have been reindexed.
   */
  reindexedEvents: number;
  /**
   * The percentage showing the re-index progress.
   */
  done: number;
}

interface LoadFileEventsArgs {
  /**
   * The ID of the room for which the events should be loaded.
   */
  roomId: string;
  /**
   * The maximum number of events to return.
   */
  limit: number;
  /**
   * An event id of a previous event returned
   * by this method. If set events that are older than the event with the
   * given event ID will be returned.
   */
  fromEvent: string;
  /**
   * The direction that we are going to continue lading events to.
   */
  direction: "b" | "f";
}

/**
 * Seshat re-index error.
 *
 * This error will be thrown if a Seshat database can't be opened because it
 * needs to be re-indexed.
 *
 * The database can be opened as a recovery database with the SeshatRecovery
 * class. This class provides method to re-index the database.
 *
 */
export class ReindexError extends Error {
  /**
   * Create a new ReindexError
   */
  constructor(...params: any[]) {
    super(...params);

    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, ReindexError);
    }
    this.name = "ReindexError";
    this.message = "The Seshat database needs to be reindexed.";
  }
}

interface SeshatConfig {
  /**
   * The language that the database should use for indexing.
   * Picking the correct indexing language may improve the search.
   */
  language?: string;
  /**
   * The passphrase that should be used to
   * encrypt the database. The database is left unencrypted if no passphrase is
   * set.
   */
  passphrase?: string;
  /**
   * The tokenizer mode to use for indexing. Can be "ngram" or "language" (default).
   * The "ngram" mode is useful for languages without clear word boundaries (e.g., Japanese,
   * Chinese).
   */
  tokenizerMode?: "language" | "ngram";
  /**
   * The minimum n-gram size when using "ngram" tokenizer mode. Defaults to 2.
   */
  ngramMinSize?: number;
  /**
   * The maximum n-gram size when using "ngram" tokenizer mode. Defaults to 4.
   */
  ngramMaxSize?: number;
}

/**
 * Seshat database.<br>
 *
 * A Seshat database can be used to store and index Matrix events. A full-text
 * search can be done on the database retrieving events that match a search
 * query.
 */
export class Seshat {
  private readonly inner: any;
  /**
   * Open an existing or create a new Seshat database.
   *
   * @param path The path where the database should be stored. If a
   * database already exist in the given folder the database will be reused.
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
  public constructor(path: string, config: SeshatConfig = {}) {
    try {
      this.inner = seshatNative.createDb(path, config);
    } catch (e) {
      // The Rust side throws a RangeError, this is a bit silly so convert
      // it to a custom error.
      if (e instanceof Error && e.constructor.name === "RangeError") {
        throw new ReindexError();
      } else {
        throw e;
      }
    }
  }
  /**
   * Add an event to the database.
   *
   * This method adds an event only to a queue. To write the events to the
   * database the <code>commit()</code> methods needs to be called.
   *
   * @param matrixEvent A Matrix event that should be added to
   * the database.
   * @param profile The user profile of the sender at the
   * time the event was sent.
   */
  public addEvent(matrixEvent: MatrixEvent, profile: MatrixProfile = {}): void {
    return seshatNative.addEvent(this.inner, matrixEvent, profile);
  }

  /**
   * Delete an event from the database.
   *
   * This method adds an event only to a queue. To write the events to the
   * database the <code>commit()</code> methods needs to be called.
   *
   * @param eventId The unique id of the event that should be
   * deleted from the database.
   *
   * @return A boolean indicating if the event was removed
   * from the index or if a commit later on will be needed.
   */
  public async deleteEvent(eventId: string): Promise<boolean> {
    const deleteEvent = promisify(seshatNative.deleteEvent);
    return deleteEvent(this.inner, eventId);
  }

  /**
   * Commit the queued up events to the database.
   *
   * This is the asynchronous equivalent of the <code>commitSync()</code>
   * method.
   *
   * @param force Force the commit, commits to the index are
   * usually rate limited. This gets around the limit and forces the
   * documents to be added to the index. This should only be used for testing
   * purposes.
   *
   * @return The latest stamp of the commit. The stamp is
   * a unique incrementing number that identifies the commit.
   */
  public async commit(force = false): Promise<number> {
    const commit = promisify(seshatNative.commit);
    return commit(this.inner, force);
  }

  /**
   * Commit the queued up events to the database.
   *
   * @param wait Wait for the events to be committed. If true will
   * block until the events are committed.
   * @param force Force the commit, commits to the index are
   * usually rate limited. This gets around the limit and forces the
   * documents to be added to the index. This should only be used for testing
   * purposes.
   *
   * @return The latest stamp of the commit. The stamp is a unique
   * incrementing number that identifies the commit.
   */
  public commitSync(wait = false, force = false): number {
    return seshatNative.commitSync(this.inner, wait, force);
  }

  /**
   * Reload the indexer of the database to reflect the changes of the last
   * commit. A reload will happen automatically, this method is mainly useful
   * for unit testing purposes to force a reload before a search.
   */
  public reload(): void {
    seshatNative.reload(this.inner);
  }

  /**
   * Search the database for events using the given search term.
   * This is the asynchronous equivalent of the <code>searchSync()</code>
   * method.
   *
   * @param args Arguments object for the search.
   * @return The array of events that matched the search term.
   */
  async search(args: SearchArgs): Promise<SearchResult> {
    const search = promisify(seshatNative.search);
    return search(this.inner, args);
  }

  /**
   * Search the database for events using the given search term.
   *
   * @param term The term that is used to search the database.
   * @param limit The maximum number of events that the search should return.
   * @param before_limit The number of events to fetch that preceded the event that matched the search term.
   * @param after_limit The number of events to fetch that followed the event that matched the search term.
   * @param order_by_recency Should the search results be ordered by event recency.
   *
   * @return The array of events that matched the search term.
   */
  public searchSync(
    term: string,
    limit = 10,
    before_limit = 0,
    after_limit = 0,
    order_by_recency = false,
  ): SearchResult {
    return seshatNative.searchSync(
      this.inner,
      term,
      limit,
      before_limit,
      after_limit,
      order_by_recency,
    );
  }

  /**
   * Add a batch of events from the room history to the database.
   *
   * @param events An array of events that will be added to the database.
   *
   * @return True if the added events were already in the store,
   * false otherwise.
   */
  public addHistoricEventsSync(
    events: MatrixEvent[],
    newCheckpoint: Checkpoint | null = null,
    oldCheckPoint: Checkpoint | null = null,
  ): boolean {
    return seshatNative.addHistoricEventsSync(this.inner, events, newCheckpoint, oldCheckPoint);
  }

  /**
   * Add a batch of events from the room history to the database.
   *
   * @param events An array of events that will be added to the database.
   * @param newCheckpoint
   * @param oldCheckPoint
   *
   * @return A promise that will resolve to true if all the events have already been added to the
   *         database, false otherwise.
   */
  public async addHistoricEvents(
    events: MatrixEvent[],
    newCheckpoint: Checkpoint | null = null,
    oldCheckPoint: Checkpoint | null = null,
  ): Promise<boolean> {
    const addHistoricEvents = promisify(seshatNative.addHistoricEvents);

    return addHistoricEvents(this.inner, events, newCheckpoint, oldCheckPoint);
  }

  /**
   * Add a message crawler checkpoint.
   *
   * @param checkpoint
   *
   * @return A promise that will resolve when the checkpoint has
   * been stored in the database.
   */
  public async addCrawlerCheckpoint(checkpoint: Checkpoint): Promise<boolean> {
    return this.addHistoricEvents([], checkpoint);
  }

  /**
   * Remove a message crawler checkpoint.
   * @param checkpoint
   *
   * @return A promise that will resolve when the checkpoint has
   * been removed from the database.
   */
  public async removeCrawlerCheckpoint(checkpoint: Checkpoint): Promise<boolean> {
    return this.addHistoricEvents([], null, checkpoint);
  }

  /**
   * Load the stored crawler checkpoints.
   *
   * @return A promise that will resolve to an array of checkpoints when
   * they are loaded from the database.
   */
  public async loadCheckpoints(): Promise<Checkpoint[]> {
    const loadCheckpoints = promisify(seshatNative.loadCheckpoints);
    return loadCheckpoints(this.inner);
  }

  /**
   * Get the size of the database.
   * This returns the number of bytes the database is using on disk.
   *
   * @return A promise that will resolve to the database size in bytes.
   */
  public async getSize(): Promise<number> {
    const getSize = promisify(seshatNative.getSize);
    return getSize(this.inner);
  }

  /**
   * Get statistical information of the database.
   *
   * @return A promise that will resolve to an object containing statistical
   * information of the database.
   */
  public async getStats(): Promise<DatabaseStats> {
    const getStats = promisify(seshatNative.getStats);
    return getStats(this.inner);
  }

  /**
   * Delete the Seshat database.
   *
   * @return A promise that will resolve when the database has been deleted.
   */
  public async delete(): Promise<void> {
    const deleteDb = promisify(seshatNative.deleteDb);
    return deleteDb(this.inner);
  }

  /**
   * Shutdown and close the Seshat database.
   *
   * @return A promise that will resolve when the database has been closed.
   */
  public async shutdown(): Promise<void> {
    const shutdown = promisify(seshatNative.shutdown);
    return shutdown(this.inner);
  }

  /**
   * Change the passphrase of the database
   *
   * This will also close the database, just like shutdown does.
   *
   * @param newPassphrase The new passphrase that should from now on
   * be used to encrypt the database.
   *
   * @return A promise that will resolve when the passphrase has been changed.
   */
  public async changePassphrase(newPassphrase: string): Promise<void> {
    const changePassphrase = promisify(seshatNative.changePassphrase);
    return changePassphrase(this.inner, newPassphrase);
  }

  /**
   * Check if the database is completely empty.
   *
   * @return A promise that will resolve to true if the
   * database is empty, that is, it doesn't contain any events, false
   * otherwise.
   */
  public async isEmpty(): Promise<boolean> {
    const isEmpty = promisify(seshatNative.isEmpty);
    return isEmpty(this.inner);
  }

  /**
   * Check if the room with the given id is already indexed.
   *
   * @param roomId The ID of the room which we want to check if it
   * has been already indexed.
   *
   * @return A promise that will resolve to true if the
   * database contains events for the given room, false otherwise.
   */
  public async isRoomIndexed(roomId: string): Promise<boolean> {
    const isRoomIndexed = promisify(seshatNative.isRoomIndexed);
    return isRoomIndexed(this.inner, roomId);
  }

  /**
   * Get the custom user specific version from the database.
   *
   * @return A promise that will resolve to a number that
   * represents the user version of the database.
   */
  public async getUserVersion(): Promise<number> {
    const getUserVersion = promisify(seshatNative.getUserVersion);
    return getUserVersion(this.inner);
  }

  /**
   * Set the custom user version to the given value.
   *
   * @param version The new version that should be stored in the database.
   *
   * @return A promise that will resolve once the new version has been stored in the database.
   */
  public async setUserVersion(version: number): Promise<void> {
    const setUserVersion = promisify(seshatNative.setUserVersion);
    return setUserVersion(this.inner, version);
  }

  /**
   * Load events that contain an mxc URL to a file.
   *
   * @param args Arguments object for the method.
   *
   * @return A promise that will resolve to an array
   * of Matrix events that contain mxc URLs.
   */
  async loadFileEvents(args: LoadFileEventsArgs): Promise<LoadResult[]> {
    const loadFileEvents = promisify(seshatNative.loadFileEvents);
    return loadFileEvents(this.inner, args);
  }
}

/**
 * Seshat recovery database.
 *
 * A Seshat recovery database can be used to re-index a Seshat database.
 *
 * This will be needed if schema changes to the index were required and the
 * library has been upgraded.
 *
 * The recovery database uses the same parameters in the constructor like the
 * normal Seshat database.
 *
 * @example
 * // open a Seshat recovery database in the given folder
 * let recovery = new SeshatRecovery("/home/example/database_dir");
 * // reindex the database
 * await recovery.reindex();
 */
export class SeshatRecovery {
  private readonly inner: any;
  /**
   * @param path The path where the database should be stored. If a
   * database already exist in the given folder the database will be reused.
   * @param config Additional configuration for the database.
   */
  public constructor(path: string, config: SeshatConfig = {}) {
    this.inner = seshatNative.createRecoveryDb(path, config);
  }

  /**
   * Get info about the re-index status.
   *
   * A object that holds the number of total events,
   * re-indexed events and the done percentage.
   */
  public info(): RecoveryInfo {
    return seshatNative.getInfoRecoveryDb(this.inner);
  }

  /**
   * Get the custom user specific version from the database.
   *
   * @return A promise that will resolve to a number that
   * represents the user version of the database.
   */
  public async getUserVersion(): Promise<number> {
    const getUserVersion = promisify(seshatNative.getUserVersionRecoveryDb);
    return getUserVersion(this.inner);
  }

  /**
   * Shutdown and close the Seshat recovery database.
   *
   * @return A promise that will resolve when the database has been closed.
   */
  public async shutdown() {
    const shutdown = promisify(seshatNative.shutdownRecoveryDb);
    return shutdown(this.inner);
  }

  /**
   * Re-index the database.
   *
   * @return A promise that will resolve once the database has been re-indexed.
   */
  public async reindex(): Promise<void> {
    const reindex = promisify(seshatNative.reindexRecoveryDb);
    return reindex(this.inner);
  }
}
