import { Hope } from 'hoper';

import { SyncStorage } from './createSyncStorage';

export interface AsyncStorage {
  getItem(key: string, callback?: (error?: Error, result?: string) => void): Promise<string | null>;
  setItem(key: string, value: string, callback?: (error?: Error) => void): Promise<void>;
  removeItem(key: string, callback?: (error?: Error) => void): Promise<void>;
}

export interface Options<Sync extends boolean | undefined = undefined> {
  sync?: Sync;
  filename?: string;
  inMemoryOnly?: boolean;
  timestampData?: boolean;
  autoload?: boolean;
  onload?: Function;
  afterSerialization?: Function;
  beforeDeserialization?: Function;
  corruptAlertThreshold?: number;
  compareStrings?: Function;
  nodeWebkitAppName?: string;
  storage?: [Sync] extends [true] ? SyncStorage : AsyncStorage;
}

export interface IndexOptions {
  fieldName: string;
  unique?: boolean;
  sparse?: boolean;
  expireAfterSeconds?: number;
}

export interface UpdateOptions {
  multi?: boolean;
  upsert?: boolean;
  returnUpdatedDocs?: boolean;
}

export type UpdateResult<Doc = MemoryDBDocument | MemoryDBDocument[]> = {
  numAffected: number;
  upsert: boolean;
  updated: Doc | undefined;
};

export interface RemoveOptions {
  multi?: boolean;
}

export interface Cursor<Doc> extends Hope<Doc> {
  exec(): Promise<Doc>;
  skip(value: number): Cursor<Doc>;
  limit(value: number): Cursor<Doc>;
  sort(doc: { [K: string]: 1 | -1 }): Cursor<Doc>;
}

export type Query = Record<string, any>;
export type Projection = Record<string, any>;

export type MemoryDBDocument = {
  id: string;
};

export type DocInput<T> = Omit<T, 'id'> & { id?: string } extends infer R
  ? {
      [K in keyof R]: R[K];
    }
  : any;

export type Callback<T = void> = (err: Error | null, value: T) => void;
export type InsertCallback<Doc extends MemoryDBDocument = MemoryDBDocument> = (err: Error | null, doc: Doc) => void;

export type CountCallback = (err: Error | null, count: number) => void;

export type FindCallback<Doc extends MemoryDBDocument = MemoryDBDocument> =
  //
  (err: Error | null, docs: Doc[]) => void;

export type FindOneCallback<Doc extends MemoryDBDocument = MemoryDBDocument> = (
  err: Error | null,
  doc: Doc | null
) => void;

export type UpdateCallback<Doc extends MemoryDBDocument = MemoryDBDocument> = (
  err: Error | null,
  result?: UpdateResult<Doc>
) => void;

export type Methods<Doc extends MemoryDBDocument> = {
  loadDatabase(cb?: Callback): void;
  resetIndexes(newData: DocInput<Doc>[], cb?: Callback): void;
  ensureIndex(options: IndexOptions, cb?: Callback): void;
  removeIndex(fieldName: string, cb?: Callback): void;
  addToIndexes(doc: Doc, cb?: Callback): void;
  removeFromIndexes(doc: Doc, cb?: Callback): void;
  updateIndexes(oldDoc: Doc, newDoc: Doc, cb?: Callback): void;
  getCandidates(query: Query, dontExpireStaleDocs: boolean, cb?: Callback): void;

  createNewId(): string;

  getAllData(cb?: Callback<Doc[]>): Doc[];

  count(query: Query, cb?: CountCallback): number;

  find(query: Query, cb?: FindCallback<Doc>): Doc[];
  find(query: Query, projection: Projection, cb?: FindCallback<Doc>): Doc[];

  findOne(query: Query, cb?: FindOneCallback<Doc>): Doc;
  findOne(query: Query, projection: Projection, cb?: FindOneCallback<Doc>): Doc | null;

  update(query: Query, doc: Doc, options?: UpdateOptions, cb?: UpdateCallback<Doc>): UpdateResult<Doc>;

  remove(query: Query, options?: RemoveOptions, cb?: FindOneCallback<Doc>): number;

  insert(newDoc: DocInput<Doc>, cb?: InsertCallback<Doc>): Doc;
};
