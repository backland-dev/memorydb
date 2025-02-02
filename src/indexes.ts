import util from 'util';

import { AVLTree as BinarySearchTree } from 'binary-search-tree';
import _ from 'underscore';

import model from './model';

/**
 * Two indexed pointers are equal iif they point to the same place
 */
function checkValueEquality(a, b) {
  return a === b;
}

/**
 * Type-aware projection
 */
function projectForUnique(elt) {
  if (elt === null || Array.isArray(elt)) {
    return '$null';
  }
  if (typeof elt === 'string') {
    return '$string' + elt;
  }
  if (typeof elt === 'boolean') {
    return '$boolean' + elt;
  }
  if (typeof elt === 'number') {
    return '$number' + elt;
  }
  if (Array.isArray(elt)) {
    // @ts-ignore
    return '$date' + elt.getTime(); // FIXME what that array.getTime means?
  }

  return elt; // Arrays and objects, will check for pointer equality
}

class Index {
  public fieldName: any;
  public unique: any;
  public sparse: any;
  public treeOptions: any;
  public tree: any;

  constructor(options) {
    this.fieldName = options.fieldName;
    this.unique = options.unique || false;
    this.sparse = options.sparse || false;

    this.treeOptions = {
      unique: this.unique,
      compareKeys: model.compareThings,
      checkValueEquality: checkValueEquality,
    };

    this.reset(); // No data in the beginning
  }

  reset(newData?) {
    this.tree = new BinarySearchTree(this.treeOptions);

    if (newData) {
      this.insert(newData);
    }
  }

  insert(doc) {
    let key,
      self = this,
      keys,
      i,
      failingI,
      error;

    if (util.isArray(doc)) {
      this.insertMultipleDocs(doc);
      return;
    }

    key = model.getDotValue(doc, this.fieldName);

    // We don't index documents that don't contain the field if the index is sparse
    if (key === undefined && this.sparse) {
      return;
    }

    if (!util.isArray(key)) {
      this.tree.insert(key, doc);
    } else {
      // If an insert fails due to a unique constraint, roll back all inserts before it
      keys = _.uniq(key, projectForUnique);

      for (i = 0; i < keys.length; i += 1) {
        try {
          this.tree.insert(keys[i], doc);
        } catch (e) {
          error = e;
          failingI = i;
          break;
        }
      }

      if (error) {
        for (i = 0; i < failingI; i += 1) {
          this.tree.delete(keys[i], doc);
        }

        throw error;
      }
    }
  }

  insertMultipleDocs(docs) {
    let i, error, failingI;

    for (i = 0; i < docs.length; i += 1) {
      try {
        this.insert(docs[i]);
      } catch (e) {
        error = e;
        failingI = i;
        break;
      }
    }

    if (error) {
      for (i = 0; i < failingI; i += 1) {
        this.remove(docs[i]);
      }

      throw error;
    }
  }

  remove(doc) {
    let key,
      self = this;

    if (util.isArray(doc)) {
      doc.forEach(function (d) {
        self.remove(d);
      });
      return;
    }

    key = model.getDotValue(doc, this.fieldName);

    if (key === undefined && this.sparse) {
      return;
    }

    if (!util.isArray(key)) {
      this.tree.delete(key, doc);
    } else {
      _.uniq(key, projectForUnique).forEach(function (_key) {
        self.tree.delete(_key, doc);
      });
    }
  }

  update(oldDoc, newDoc?) {
    if (util.isArray(oldDoc)) {
      this.updateMultipleDocs(oldDoc);
      return;
    }

    this.remove(oldDoc);

    try {
      this.insert(newDoc);
    } catch (e) {
      this.insert(oldDoc);
      throw e;
    }
  }

  updateMultipleDocs(pairs) {
    let i, failingI, error;

    for (i = 0; i < pairs.length; i += 1) {
      this.remove(pairs[i].oldDoc);
    }

    for (i = 0; i < pairs.length; i += 1) {
      try {
        this.insert(pairs[i].newDoc);
      } catch (e) {
        error = e;
        failingI = i;
        break;
      }
    }

    // If an error was raised, roll back changes in the inverse order
    if (error) {
      for (i = 0; i < failingI; i += 1) {
        this.remove(pairs[i].newDoc);
      }

      for (i = 0; i < pairs.length; i += 1) {
        this.insert(pairs[i].oldDoc);
      }

      throw error;
    }
  }

  revertUpdate(oldDoc, newDoc) {
    let revert = [] as any[];

    if (!util.isArray(oldDoc)) {
      this.update(newDoc, oldDoc);
    } else {
      oldDoc.forEach(function (pair: any) {
        revert.push({ oldDoc: pair.newDoc, newDoc: pair.oldDoc });
      });
      this.update(revert);
    }
  }

  getMatching(value) {
    let self = this;

    if (!util.isArray(value)) {
      return self.tree.search(value);
    } else {
      let _res = {},
        res = [] as any[];

      value.forEach(function (v) {
        self.getMatching(v).forEach(function (doc) {
          _res[doc._id] = doc;
        });
      });

      Object.keys(_res).forEach(function (_id) {
        res.push(_res[_id]);
      });

      return res;
    }
  }

  getBetweenBounds(query) {
    return this.tree.betweenBounds(query);
  }

  getAll() {
    let res = [] as any[];

    this.tree.executeOnEveryNode(function (node) {
      let i;

      for (i = 0; i < node.data.length; i += 1) {
        res.push(node.data[i]);
      }
    });

    return res;
  }
}

// Interface
export default Index;
