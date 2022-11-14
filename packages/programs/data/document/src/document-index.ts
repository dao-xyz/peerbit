import { Constructor, deserialize, field, variant, vec } from "@dao-xyz/borsh";
import { asString, Hashable } from "./utils";
import { BORSH_ENCODING, Encoding, Entry, Payload } from "@dao-xyz/ipfs-log";
import { Log } from "@dao-xyz/ipfs-log";
import {
    arraysEqual,
    UInt8ArraySerializer,
} from "@dao-xyz/peerbit-borsh-utils";
import { ComposableProgram, Program } from "@dao-xyz/peerbit-program";
import {
    Compare,
    AnySearch,
    FieldBigIntCompareQuery,
    FieldByteMatchQuery,
    FieldStringMatchQuery,
    MemoryCompareQuery,
    PageQueryRequest,
    Query,
    QueryType,
    Result,
    ResultWithSource,
    SortDirection,
    StateFieldQuery,
} from "@dao-xyz/peerbit-anysearch";
import { AccessError } from "@dao-xyz/peerbit-crypto";
import { BinaryPayload } from "@dao-xyz/peerbit-bpayload";
import { CanRead, DQuery } from "@dao-xyz/peerbit-query";
import pino from "pino";
const logger = pino().child({ module: "document-index" });

@variant(0)
export class Operation<T> {}

export const encoding = BORSH_ENCODING(Operation);

@variant(0)
export class PutOperation<T> extends Operation<T> {
    @field({ type: "string" })
    key: string;

    @field(UInt8ArraySerializer)
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
}

@variant("documents_index")
export class DocumentIndex<T extends BinaryPayload> extends ComposableProgram {
    @field({ type: AnySearch })
    search: AnySearch<Operation<T>>;

    @field({ type: "string" })
    indexBy: string;

    _index: Map<string, IndexedValue<T>>;

    type: Constructor<T>;

    constructor(properties?: {
        search?: AnySearch<Operation<T>>;
        indexBy: string;
    }) {
        super();
        if (properties) {
            this.search =
                properties.search || new AnySearch({ query: new DQuery() });
            this.indexBy = properties.indexBy;
        }
        this._index = new Map();
    }

    async setup(properties: { type: Constructor<T>; canRead: CanRead }) {
        this.type = properties.type;
        await this.search.setup({
            context: this,
            canRead: properties.canRead,
            queryHandler: this.queryHandler.bind(this),
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
            return value._value;
        }
    }

    deserializeOrItem(
        entry: Entry<Operation<T>>,
        operation: PutOperation<T>
    ): IndexedValue<T> {
        const item: IndexedValue<T> = {
            entry,
            key: operation.key,
            value: this.deserializeOrPass(operation),
        };
        return item;
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

    queryHandler(query: QueryType): Promise<Result[]> {
        if (query instanceof PageQueryRequest) {
            const queries: Query[] = query.queries;
            let results = this._queryDocuments((doc) =>
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
                                              .indexOf(
                                                  f.value.toLowerCase()
                                              ) !== -1
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

                                      switch (f.compare) {
                                          case Compare.Equal:
                                              return value == f.value; // == because with want bigint == number at some cases
                                          case Compare.Greater:
                                              return value > f.value;
                                          case Compare.GreaterOrEqual:
                                              return value >= f.value;
                                          case Compare.Less:
                                              return value < f.value;
                                          case Compare.LessOrEqual:
                                              return value <= f.value;
                                          default:
                                              console.warn(
                                                  "Unexpected compare"
                                              );
                                              return false;
                                      }
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
                                          const offsetn = Number(
                                              compare.offset
                                          ); // TODO type check

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
                              }
                              logger.info(
                                  "Unsupported query type: " +
                                      f.constructor.name
                              );
                              return false;
                          })
                          .reduce((prev, current) => prev && current)
                    : true
            ).map((x) => x.value);

            /* if (query.sort) { bad implementation
        const sort = query.sort;
        const resolveField = (obj: T) => {
          let v = obj;
          for (let i = 0; i < sort.key.length; i++) {
            v = (v as any)[sort.key[i]]
          }
          return v
        }
        let direction = 1;
        if (query.sort.direction == SortDirection.Descending) {
          direction = -1;
        }

        results.sort((a, b) => {
          const af = resolveField(a)
          const bf = resolveField(b)
          if (af < bf) {
            return -direction;
          }
          else if (af > bf) {
            return direction;
          }
          return 0;
        })
      } */
            // TODO check conversions
            if (query.offset) {
                results = results.slice(Number(query.offset));
            }

            if (query.size) {
                results = results.slice(0, Number(query.size));
            }
            return Promise.resolve(
                results.map(
                    (r) =>
                        new ResultWithSource({
                            source: r,
                        })
                )
            );
        }

        // TODO diagnostics for other query types
        return Promise.resolve([]);
    }
}
