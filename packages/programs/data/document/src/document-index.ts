import {
    Constructor,
    deserialize,
    field,
    serialize,
    variant,
    vec,
} from "@dao-xyz/borsh";
import { asString, Hashable } from "./utils.js";
import { BORSH_ENCODING, Encoding, Entry } from "@dao-xyz/ipfs-log";
import { Log } from "@dao-xyz/ipfs-log";
import { arraysEqual } from "@dao-xyz/peerbit-borsh-utils";
import { ComposableProgram } from "@dao-xyz/peerbit-program";
import {
    FieldBigIntCompareQuery,
    FieldByteMatchQuery,
    FieldStringMatchQuery,
    MemoryCompareQuery,
    DocumentQueryRequest,
    Query,
    ResultWithSource,
    StateFieldQuery,
    CreatedAtQuery,
    ModifiedAtQuery,
    compare,
    Context,
} from "./query.js";
import { AccessError, PublicSignKey } from "@dao-xyz/peerbit-crypto";
import { CanRead, RPC, QueryContext, RPCOptions } from "@dao-xyz/peerbit-rpc";
import { Results } from "./query.js";
import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
const logger = loggerFn({ module: "document-index" });

@variant(0)
export class Operation<T> {}

export const encoding = BORSH_ENCODING(Operation);

@variant(0)
export class PutOperation<T> extends Operation<T> {
    @field({ type: "string" })
    key: string;

    @field({ type: Uint8Array })
    data: Uint8Array;

    _value?: T;

    constructor(props?: { key: string; data: Uint8Array; value?: T }) {
        super();
        if (props) {
            this.key = props.key;
            this.data = props.data;
            this._value = props.value;
        }
    }

    get value(): T | undefined {
        if (!this._value) {
            throw new Error("Unexpected");
        }
        return this._value;
    }

    getValue(encoding: Encoding<T>): T {
        if (this._value) {
            return this._value;
        }
        this._value = encoding.decoder(this.data);
        return this._value;
    }
}

@variant(1)
export class PutAllOperation<T> extends Operation<T> {
    @field({ type: vec(PutOperation) })
    docs: PutOperation<T>[];

    constructor(props?: { docs: PutOperation<T>[] }) {
        super();
        if (props) {
            this.docs = props.docs;
        }
    }
}

@variant(2)
export class DeleteOperation extends Operation<any> {
    @field({ type: "string" })
    key: string;

    @field({ type: "bool" })
    permanently: boolean;

    constructor(props?: { key: string; permanently?: boolean }) {
        super();
        if (props) {
            this.key = props.key;
            this.permanently = props.permanently || false;
        }
    }
}

export interface IndexedValue<T> {
    key: string;
    value: T; // decrypted, decoded
    entry: Entry<Operation<T>>;
    context: Context;
}

@variant("documents_index")
export class DocumentIndex<T> extends ComposableProgram {
    @field({ type: RPC })
    _query: RPC<DocumentQueryRequest, Results<T>>;

    @field({ type: "string" })
    indexBy: string;

    _sync: (result: Results<T>) => Promise<void>;
    _index: Map<string, IndexedValue<T>>;
    type: Constructor<T>;

    constructor(properties: {
        query?: RPC<DocumentQueryRequest, Results<T>>;
        indexBy: string;
    }) {
        super();
        this._query = properties.query || new RPC();
        this.indexBy = properties.indexBy;
    }

    async setup(properties: {
        type: Constructor<T>;
        canRead: CanRead;
        sync: (result: Results<T>) => Promise<void>;
    }) {
        this._index = new Map();
        this.type = properties.type;
        this._sync = properties.sync;
        await this._query.setup({
            context: this,
            canRead: properties.canRead,
            responseHandler: async (query, context) => {
                const results = await this.queryHandler(query, context);
                if (results.length > 0) {
                    return new Results({
                        results: results.map(
                            (r) =>
                                new ResultWithSource({
                                    source: serialize(r.value),
                                    context: r.context,
                                })
                        ),
                    });
                }
                return undefined;
            },
            responseType: Results,
            queryType: DocumentQueryRequest,
        });
    }

    public get(key: Hashable): IndexedValue<T> | undefined {
        const stringKey = asString(key);
        return this._index.get(stringKey);
    }

    get size(): number {
        return this._index.size;
    }

    async updateIndex(oplog: Log<Operation<T>>) {
        if (!this.type) {
            throw new Error("Not initialized");
        }

        const handled: { [key: string]: boolean } = {};
        const values = oplog.values;
        for (let i = values.length - 1; i >= 0; i--) {
            try {
                const item = values[i];
                const payload = await item.getPayloadValue();
                if (payload instanceof PutAllOperation) {
                    for (const doc of payload.docs) {
                        if (doc && handled[doc.key] !== true) {
                            handled[doc.key] = true;
                            this._index.set(doc.key, {
                                key: asString(doc.key),
                                value: this.deserializeOrPass(doc),
                                entry: item,
                                context: new Context({
                                    created:
                                        this._index.get(doc.key)?.context
                                            .created ||
                                        item.metadata.clock.timestamp.wallTime,
                                    modified:
                                        item.metadata.clock.timestamp.wallTime,
                                    head: item.hash,
                                }),
                            });
                        }
                    }
                } else if (payload instanceof PutOperation) {
                    const key = payload.key;
                    if (handled[key] !== true) {
                        handled[key] = true;
                        this._index.set(key, {
                            entry: item,
                            key: payload.key,
                            value: this.deserializeOrPass(payload),
                            context: new Context({
                                created:
                                    this._index.get(key)?.context.created ||
                                    item.metadata.clock.timestamp.wallTime,
                                modified:
                                    item.metadata.clock.timestamp.wallTime,
                                head: item.hash,
                            }),
                        });
                    }
                } else if (payload instanceof DeleteOperation) {
                    const key = payload.key;
                    if (handled[key] !== true) {
                        handled[key] = true;
                        this._index.delete(key);
                    }
                } else {
                    // Unknown operation
                }
            } catch (error) {
                if (error instanceof AccessError) {
                    continue;
                }
                throw error;
            }
        }
    }

    deserializeOrPass(value: PutOperation<T>): T {
        if (value._value) {
            return value._value;
        } else {
            value._value = deserialize(value.data, this.type);
            return value._value!;
        }
    }

    _queryDocuments(
        filter: (doc: IndexedValue<T>) => boolean
    ): IndexedValue<T>[] {
        // Whether we return the full operation data or just the db value
        const results: IndexedValue<T>[] = [];
        for (const value of this._index.values()) {
            if (filter(value)) {
                results.push(value);
            }
        }
        return results;
    }

    queryHandler(
        query: DocumentQueryRequest,
        context?: QueryContext
    ): Promise<IndexedValue<T>[]> {
        const queries: Query[] = query.queries;
        const results = this._queryDocuments((doc) =>
            queries?.length > 0
                ? queries
                      .map((f) => {
                          if (f instanceof StateFieldQuery) {
                              let fv: any = doc.value;
                              for (let i = 0; i < f.key.length; i++) {
                                  fv = fv[f.key[i]];
                              }

                              if (f instanceof FieldStringMatchQuery) {
                                  if (typeof fv !== "string") return false;
                                  return (
                                      fv
                                          .toLowerCase()
                                          .indexOf(f.value.toLowerCase()) !== -1
                                  );
                              }
                              if (f instanceof FieldByteMatchQuery) {
                                  if (!Array.isArray(fv)) return false;
                                  return arraysEqual(fv, f.value);
                              }
                              if (f instanceof FieldBigIntCompareQuery) {
                                  const value: bigint | number = fv;

                                  if (
                                      typeof value !== "bigint" &&
                                      typeof value !== "number"
                                  ) {
                                      return false;
                                  }

                                  return compare(value, f.compare, f.value);
                              }
                          } else if (f instanceof MemoryCompareQuery) {
                              const operation =
                                  doc.entry.payload.getValue(encoding);
                              if (!operation) {
                                  throw new Error(
                                      "Unexpected, missing cached value for payload"
                                  );
                              }
                              if (operation instanceof PutOperation) {
                                  const bytes = operation.data;
                                  for (const compare of f.compares) {
                                      const offsetn = Number(compare.offset); // TODO type check

                                      for (
                                          let b = 0;
                                          b < compare.bytes.length;
                                          b++
                                      ) {
                                          if (
                                              bytes[offsetn + b] !==
                                              compare.bytes[b]
                                          ) {
                                              return false;
                                          }
                                      }
                                  }
                              } else {
                                  // TODO add implementations for PutAll
                                  return false;
                              }
                              return true;
                          } else if (f instanceof CreatedAtQuery) {
                              for (const created of f.created) {
                                  if (
                                      !compare(
                                          doc.context.created,
                                          created.compare,
                                          created.value
                                      )
                                  ) {
                                      return false;
                                  }
                              }
                              return true;
                          } else if (f instanceof ModifiedAtQuery) {
                              for (const modified of f.modified) {
                                  if (
                                      !compare(
                                          doc.context.modified,
                                          modified.compare,
                                          modified.value
                                      )
                                  ) {
                                      return false;
                                  }
                              }
                              return true;
                          }

                          logger.info(
                              "Unsupported query type: " + f.constructor.name
                          );
                          return false;
                      })
                      .reduce((prev, current) => prev && current)
                : true
        );

        return Promise.resolve(results);
    }
    public query(
        queryRequest: DocumentQueryRequest,
        responseHandler: (response: Results<T>, from?: PublicSignKey) => void,
        options?: {
            sync?: boolean;
            remote?: boolean | RPCOptions;
            local?: boolean;
        }
    ): Promise<void[]> {
        const handler = async (response: Results<T>, from?: PublicSignKey) => {
            response.results.forEach((r) => r.init(this.type));
            if (options?.sync) {
                await this._sync(response);
            }
            responseHandler(response, from);
        };
        const promises: Promise<void>[] = [];
        const local =
            typeof options?.local == "boolean" ? options?.local : true;
        let remote: RPCOptions | undefined;
        if (typeof options?.remote === "boolean") {
            if (options?.remote) {
                remote = {};
            } else {
                remote = undefined;
            }
        } else {
            remote = options?.remote;
        }

        if (!local && !remote) {
            throw new Error(
                "Expecting either 'options.remote' or 'options.local' to be true"
            );
        }

        if (local) {
            promises.push(
                this.queryHandler(queryRequest, {
                    address: this.address.toString(),
                    from: this._identity.publicKey,
                }).then((results) => {
                    if (results.length > 0) {
                        responseHandler(
                            new Results({
                                results: results.map(
                                    (r) =>
                                        new ResultWithSource({
                                            context: r.context,
                                            value: r.value,
                                        })
                                ),
                            })
                        );
                    }
                })
            );
        }
        if (remote) {
            promises.push(this._query.send(queryRequest, handler, remote));
        }
        return Promise.all(promises);
    }
}
