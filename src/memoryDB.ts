import EventEmitter from 'events';
import util from 'util';

import async from 'async';
import _ from 'underscore';

import { createSyncStorage } from './createSyncStorage';
import Cursor from './cursor';
import customUtils from './customUtils';
import Executor from './executor';
import Index from './indexes';
import { MemoryDBDocument, Methods, Options, UpdateCallback } from './interfaces';
import model from './model';
import Persistence from './persistence';
import { maybePromise } from './util';

export class MemoryDB<Doc extends MemoryDBDocument = MemoryDBDocument> extends EventEmitter implements Methods<Doc> {
  public inMemoryOnly: any;
  public autoload: any;
  public timestampData: any;
  public storage: any;
  public filename: any;
  public compareStrings: any;
  public persistence: Persistence;
  public executor: any;
  public indexes: any;
  public ttlIndexes: any;
  public sync: boolean;

  constructor(
    options: Options<any> = { sync: true, inMemoryOnly: true, autoload: true, storage: createSyncStorage() }
  ) {
    super();

    this.sync = !!options.sync;

    let filename;

    // Retrocompatibility with v0.6 and before
    if (typeof options === 'string') {
      filename = options;
      this.inMemoryOnly = false; // Default
    } else {
      options = options || {};
      filename = options.filename;
      this.inMemoryOnly = options.inMemoryOnly || false;
      this.autoload = options.autoload || false;
      this.timestampData = options.timestampData || false;
      this.storage = options.storage;

      if (!(this.storage && this.storage.getItem && this.storage.setItem && this.storage.removeItem)) {
        throw new Error(
          `expected options.storage to be defined. \n--> received ${
            this.storage ? `object with keys: [${Object.getOwnPropertyNames(this.storage).join(', ')}]` : this.storage
          }`
        );
      }
    }

    // Determine whether in memory or persistent
    if (!filename || typeof filename !== 'string' || filename.length === 0) {
      this.filename = null;
      this.inMemoryOnly = true;
    } else {
      this.filename = filename;
    }

    // String comparison function
    this.compareStrings = options.compareStrings;

    // Persistence handling
    this.persistence = new Persistence({
      db: this,
      nodeWebkitAppName: options.nodeWebkitAppName,
      afterSerialization: options.afterSerialization,
      beforeDeserialization: options.beforeDeserialization,
      corruptAlertThreshold: options.corruptAlertThreshold,
    });

    // This new executor is ready if we don't use persistence
    // If we do, it will only be ready once loadDatabase is called
    this.executor = new Executor();
    if (this.inMemoryOnly) {
      this.executor.ready = true;
    }

    // Indexed by field name, dot notation can be used
    // _id is always indexed and since _ids are generated randomly the underlying
    // binary is always well-balanced
    this.indexes = {};
    this.indexes._id = new Index({ fieldName: '_id', unique: true });
    this.ttlIndexes = {};

    // Queue a load of the database right away and call the onload handler
    // By default (no onload handler), if there is an error there, no operation will be possible so warn the user by throwing an exception
    if (this.autoload) {
      this.loadDatabase(
        // @ts-ignore // FIXME
        options.onload ||
          function (err) {
            if (err) {
              throw err;
            }
          }
      );
    }
  }

  loadDatabase(...args) {
    return maybePromise(
      this,
      args
    )((args) => {
      return this.executor.push(
        {
          this: this.persistence,
          fn: this.persistence.loadDatabase,
          arguments: args,
        },
        true
      );
    });
  }

  getAllData() {
    return this.indexes._id.getAll();
  }

  resetIndexes(newData) {
    let self = this;

    Object.keys(this.indexes).forEach(function (i) {
      self.indexes[i].reset(newData);
    });
  }

  ensureIndex(options, cb) {
    return maybePromise(this, [cb])(([cb]) => {
      let err,
        callback = cb || function () {};

      options = options || {};

      if (!options.fieldName) {
        err = new Error('Cannot create an index without a fieldName');
        err.missingFieldName = true;
        return callback(err);
      }
      if (this.indexes[options.fieldName]) {
        return callback(null);
      }

      this.indexes[options.fieldName] = new Index(options);
      if (options.expireAfterSeconds !== undefined) {
        this.ttlIndexes[options.fieldName] = options.expireAfterSeconds;
      } // With this implementation index creation is not necessary to ensure TTL but we stick with MongoDB's API here

      try {
        this.indexes[options.fieldName].insert(this.getAllData());
      } catch (e) {
        delete this.indexes[options.fieldName];
        return callback(e);
      }

      // We may want to force all options to be persisted including defaults, not just the ones passed the index creation function
      this.persistence.persistNewState([{ $$indexCreated: options }], function (err) {
        if (err) {
          return callback(err);
        }
        return callback(null);
      });
    });
  }

  removeIndex(fieldName, cb) {
    return maybePromise(this, [cb])(([cb]) => {
      let callback = cb || function () {};

      delete this.indexes[fieldName];

      this.persistence.persistNewState([{ $$indexRemoved: fieldName }], function (err) {
        if (err) {
          return callback(err);
        }
        return callback(null);
      });
    });
  }

  addToIndexes(doc) {
    let i,
      failingIndex,
      error,
      keys = Object.keys(this.indexes);

    for (i = 0; i < keys.length; i += 1) {
      try {
        this.indexes[keys[i]].insert(doc);
      } catch (e) {
        failingIndex = i;
        error = e;
        break;
      }
    }

    // If an error happened, we need to rollback the insert on all other indexes
    if (error) {
      for (i = 0; i < failingIndex; i += 1) {
        this.indexes[keys[i]].remove(doc);
      }

      throw error;
    }
  }

  removeFromIndexes(doc) {
    let self = this;

    Object.keys(this.indexes).forEach(function (i) {
      self.indexes[i].remove(doc);
    });
  }

  updateIndexes(oldDoc, newDoc?) {
    let i,
      failingIndex,
      error,
      keys = Object.keys(this.indexes);

    for (i = 0; i < keys.length; i += 1) {
      try {
        this.indexes[keys[i]].update(oldDoc, newDoc);
      } catch (e) {
        failingIndex = i;
        error = e;
        break;
      }
    }

    // If an error happened, we need to rollback the update on all other indexes
    if (error) {
      for (i = 0; i < failingIndex; i += 1) {
        this.indexes[keys[i]].revertUpdate(oldDoc, newDoc);
      }

      throw error;
    }
  }

  getCandidates(query, dontExpireStaleDocs, callback?) {
    return maybePromise(this, [callback])(([callback]) => {
      let indexNames = Object.keys(this.indexes),
        self = this,
        usableQueryKeys;

      if (typeof dontExpireStaleDocs === 'function') {
        callback = dontExpireStaleDocs;
        dontExpireStaleDocs = false;
      }

      async.waterfall([
        // STEP 1: get candidates list by checking indexes from most to least frequent usecase
        function (cb) {
          // For a basic match
          usableQueryKeys = [] as any[];
          Object.keys(query).forEach(function (k) {
            if (
              typeof query[k] === 'string' ||
              typeof query[k] === 'number' ||
              typeof query[k] === 'boolean' ||
              util.isDate(query[k]) ||
              query[k] === null
            ) {
              usableQueryKeys.push(k);
            }
          });
          usableQueryKeys = _.intersection(usableQueryKeys, indexNames);
          if (usableQueryKeys.length > 0) {
            return cb(null, self.indexes[usableQueryKeys[0]].getMatching(query[usableQueryKeys[0]]));
          }

          // For a $in match
          usableQueryKeys = [] as any[];
          Object.keys(query).forEach(function (k) {
            if (query[k] && query[k].hasOwnProperty('$in')) {
              usableQueryKeys.push(k);
            }
          });
          usableQueryKeys = _.intersection(usableQueryKeys, indexNames);
          if (usableQueryKeys.length > 0) {
            return cb(null, self.indexes[usableQueryKeys[0]].getMatching(query[usableQueryKeys[0]].$in));
          }

          // For a comparison match
          usableQueryKeys = [] as any[];
          Object.keys(query).forEach(function (k) {
            if (
              query[k] &&
              (query[k].hasOwnProperty('$lt') ||
                query[k].hasOwnProperty('$lte') ||
                query[k].hasOwnProperty('$gt') ||
                query[k].hasOwnProperty('$gte'))
            ) {
              usableQueryKeys.push(k);
            }
          });
          usableQueryKeys = _.intersection(usableQueryKeys, indexNames);
          if (usableQueryKeys.length > 0) {
            return cb(null, self.indexes[usableQueryKeys[0]].getBetweenBounds(query[usableQueryKeys[0]]));
          }

          // By default, return all the DB data
          return cb(null, self.getAllData());
        },
        // STEP 2: remove all expired documents
        function (docs) {
          if (dontExpireStaleDocs) {
            return callback(null, docs);
          }

          let expiredDocsIds: any = [] as any[],
            validDocs: any = [] as any[],
            ttlIndexesFieldNames = Object.keys(self.ttlIndexes);

          docs.forEach(function (doc) {
            let valid = true;
            ttlIndexesFieldNames.forEach(function (i) {
              if (
                doc[i] !== undefined &&
                util.isDate(doc[i]) &&
                Date.now() > doc[i].getTime() + self.ttlIndexes[i] * 1000
              ) {
                valid = false;
              }
            });
            if (valid) {
              validDocs.push(doc);
            } else {
              expiredDocsIds.push(doc._id);
            }
          });

          async.eachSeries(
            expiredDocsIds,
            function (_id, cb) {
              self._remove({ _id: _id }, {}, function (err) {
                if (err) {
                  return callback(err);
                }
                return cb();
              });
            },
            function (err) {
              return callback(null, validDocs);
            }
          );
        },
      ]);
    });
  }

  _insert(newDoc, cb?) {
    return maybePromise(this, [cb])(([cb]) => {
      let callback = cb || function () {},
        preparedDoc;

      try {
        preparedDoc = this.prepareDocumentForInsertion(newDoc);
        this._insertInCache(preparedDoc);
      } catch (e) {
        return callback(e);
      }

      this.persistence.persistNewState(util.isArray(preparedDoc) ? preparedDoc : [preparedDoc], function (err) {
        if (err) {
          return callback(err);
        }
        return callback(null, model.deepCopy(preparedDoc));
      });
    });
  }

  createNewId() {
    let tentativeId = customUtils.uid(16);
    // Try as many times as needed to get an unused _id. As explained in customUtils, the probability of this ever happening is extremely small, so this is O(1)
    if (this.indexes._id.getMatching(tentativeId).length > 0) {
      tentativeId = this.createNewId();
    }
    return tentativeId;
  }

  prepareDocumentForInsertion(newDoc) {
    let preparedDoc,
      self = this;

    if (Array.isArray(newDoc)) {
      preparedDoc = [] as any[];
      newDoc.forEach(function (doc) {
        preparedDoc.push(self.prepareDocumentForInsertion(doc));
      });
    } else {
      preparedDoc = model.deepCopy(newDoc);
      if (preparedDoc._id === undefined) {
        preparedDoc._id = this.createNewId();
      }
      let now = new Date();
      if (this.timestampData && preparedDoc.createdAt === undefined) {
        preparedDoc.createdAt = now;
      }
      if (this.timestampData && preparedDoc.updatedAt === undefined) {
        preparedDoc.updatedAt = now;
      }
      model.checkObject(preparedDoc);
    }

    return preparedDoc;
  }

  _insertInCache(preparedDoc) {
    if (Array.isArray(preparedDoc)) {
      this._insertMultipleDocsInCache(preparedDoc);
    } else {
      this.addToIndexes(preparedDoc);
    }
  }

  _insertMultipleDocsInCache(preparedDocs) {
    let i, failingI, error;

    for (i = 0; i < preparedDocs.length; i += 1) {
      try {
        this.addToIndexes(preparedDocs[i]);
      } catch (e) {
        error = e;
        failingI = i;
        break;
      }
    }

    if (error) {
      for (i = 0; i < failingI; i += 1) {
        this.removeFromIndexes(preparedDocs[i]);
      }

      throw error;
    }
  }

  insert(...args) {
    return maybePromise(
      this,
      args
    )((args) => {
      return this.executor.push({ this: this, fn: this._insert, arguments: args });
    });
  }

  count(query, callback) {
    return maybePromise(this, [callback])(([callback]) => {
      let cursor = new Cursor(this, query, function (err, docs, callback) {
        if (err) {
          return callback(err);
        }
        return callback(null, docs.length);
      });

      if (typeof callback === 'function') {
        return cursor.exec(callback);
      } else {
        return cursor;
      }
    });
  }

  find(...args) {
    let [query, projection, callback] = args;

    switch (args.length) {
      case 1:
        projection = {};
        // callback is undefined, will return a cursor
        break;
      case 2:
        if (typeof projection === 'function') {
          callback = projection;
          projection = {};
        } // If not assume projection is an object and callback undefined
        break;
    }

    return maybePromise(this, [callback])(([callback]) => {
      let cursor = new Cursor(this, query, function (err, docs, callback) {
        let res: any = [] as any[],
          i;

        if (err) {
          return callback(err);
        }

        for (i = 0; i < docs.length; i += 1) {
          res.push(model.deepCopy(docs[i]));
        }

        return callback(null, res);
      });

      cursor.projection(projection);

      if (typeof callback === 'function') {
        return cursor.exec(callback);
      } else {
        return cursor;
      }
    });
  }

  findOne(...args) {
    let [query, projection, callback] = args;

    switch (args.length) {
      case 1:
        projection = {};
        // callback is undefined, will return a cursor
        break;
      case 2:
        if (typeof projection === 'function') {
          callback = projection;
          projection = {};
        } // If not assume projection is an object and callback undefined
        break;
    }

    return maybePromise(this, [callback])(([callback]) => {
      let cursor = new Cursor(this, query, function (err, docs, callback) {
        if (err) {
          return callback(err);
        }
        if (docs.length === 1) {
          return callback(null, model.deepCopy(docs[0]));
        } else {
          return callback(null, null);
        }
      });

      cursor.projection(projection).limit(1);

      if (typeof callback === 'function') {
        return cursor.exec(callback);
      } else {
        return cursor;
      }
    });
  }

  _update(query, updateQuery?, options?, cb?) {
    const self = this;

    if (typeof options === 'function') {
      cb = options;
      options = {};
    }

    const callback = cb || function () {};

    return maybePromise(this, [options, callback])(([options, _callback]) => {
      const callback = _callback as UpdateCallback;
      const { multi, upsert } = options;
      let numReplaced = 0;

      async.waterfall([
        function (cb) {
          // If upsert option is set, check whether we need to insert the doc
          if (!upsert) {
            return cb();
          }

          // Need to use an internal function not tied to the executor to avoid deadlock
          let cursor = new Cursor(self, query);
          cursor.limit(1)._exec(function (err, docs) {
            if (err) {
              return callback(err);
            }
            if (docs.length === 1) {
              return cb();
            } else {
              let toBeInserted;

              try {
                model.checkObject(updateQuery);
                // updateQuery is a simple object with no modifier, use it as the document to insert
                toBeInserted = updateQuery;
              } catch (e) {
                // updateQuery contains modifiers, use the find query as the base,
                // strip it from all operators and update it according to updateQuery
                try {
                  toBeInserted = model.modify(model.deepCopy(query, true), updateQuery);
                } catch (err: any) {
                  return callback(err);
                }
              }

              return self._insert(toBeInserted, function (err, newDoc) {
                if (err) {
                  return callback(err);
                }
                return callback(null, { numAffected: 1, updated: newDoc, upsert: true });
              });
            }
          });
        },
        function () {
          // Perform the update
          let modifiedDoc,
            modifications: any = [] as any[],
            createdAt;

          self.getCandidates(query, function (err, candidates) {
            if (err) {
              return callback(err);
            }

            // Preparing update (if an error is thrown here neither the datafile nor
            // the in-memory indexes are affected)
            try {
              for (let i = 0; i < candidates.length; i += 1) {
                if (model.match(candidates[i], query) && (multi || numReplaced === 0)) {
                  numReplaced += 1;
                  if (self.timestampData) {
                    createdAt = candidates[i].createdAt;
                  }
                  modifiedDoc = model.modify(candidates[i], updateQuery);
                  if (self.timestampData) {
                    modifiedDoc.createdAt = createdAt;
                    modifiedDoc.updatedAt = new Date();
                  }
                  modifications.push({
                    oldDoc: candidates[i],
                    newDoc: modifiedDoc,
                  });
                }
              }
            } catch (err: any) {
              return callback(err);
            }

            // Change the docs in memory
            try {
              self.updateIndexes(modifications);
            } catch (err: any) {
              return callback(err);
            }

            // Update the datafile
            let updatedDocs = _.pluck(modifications, 'newDoc');
            self.persistence.persistNewState(updatedDocs, function (err) {
              if (err) {
                return callback(err);
              }
              if (!options.returnUpdatedDocs) {
                return callback(null, { numAffected: numReplaced, upsert, updated: undefined });
              } else {
                let updatedDocsDC: any = [] as any[];
                updatedDocs.forEach(function (doc) {
                  updatedDocsDC.push(model.deepCopy(doc));
                });
                if (!multi) {
                  updatedDocsDC = updatedDocsDC[0];
                }
                return callback(null, { numAffected: numReplaced, updated: updatedDocsDC, upsert });
              }
            });
          });
        },
      ]);
    });
  }

  update(...args) {
    return maybePromise(
      this,
      args
    )((args) => {
      this.executor.push({ this: this, fn: this._update, arguments: args });
    });
  }

  _remove(query, options?, cb?) {
    return maybePromise(this, [cb])(([cb]) => {
      let callback,
        self = this,
        numRemoved = 0,
        removedDocs: any = [] as any[],
        multi;

      if (typeof options === 'function') {
        cb = options;
        options = {};
      }
      callback = cb || function () {};
      multi = options.multi !== undefined ? options.multi : false;

      this.getCandidates(query, true, function (err, candidates) {
        if (err) {
          return callback(err);
        }

        try {
          candidates.forEach(function (d) {
            if (model.match(d, query) && (multi || numRemoved === 0)) {
              numRemoved += 1;
              removedDocs.push({ $$deleted: true, _id: d._id });
              self.removeFromIndexes(d);
            }
          });
        } catch (err) {
          return callback(err);
        }

        self.persistence.persistNewState(removedDocs, function (err) {
          if (err) {
            return callback(err);
          }
          return callback(null, numRemoved);
        });
      });
    });
  }

  remove(...args) {
    return maybePromise(
      this,
      args
    )((args) => {
      this.executor.push({ this: this, fn: this._remove, arguments: args });
    });
  }
}

export function createMemoryDB<O extends Options | undefined, Doc extends MemoryDBDocument = MemoryDBDocument>(
  options?: O
): MemoryDB<Doc> {
  return new MemoryDB(options);
}
