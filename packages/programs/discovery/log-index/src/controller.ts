import { field, option, variant, vec } from "@dao-xyz/borsh";
import { Entry, EntryEncryptionTemplate } from "@dao-xyz/ipfs-log";
import { ComposableProgram } from "@dao-xyz/peerbit-program";
import {
  CanRead,
  DQuery,
  DQueryInitializationOptions,
  QueryTopicOption,
} from "@dao-xyz/peerbit-query";
import { Store } from "@dao-xyz/peerbit-store";
import { EncryptedThing, X25519PublicKey } from "@dao-xyz/peerbit-crypto";
import pino from "pino";
const logger = pino().child({ module: "log-index" });

@variant(0)
export class HeadsMessage {
  @field({ type: vec(Entry) })
  heads: Entry<any>[];

  constructor(properties?: { heads: Entry<any>[] }) {
    if (properties) {
      this.heads = properties.heads;
    }
  }
}

@variant(0)
export class LogQuery {}

/**
 * Find logs that can be decrypted by certain keys
 */

@variant(0)
export class LogEntryEncryptionQuery
  extends LogQuery
  implements
    EntryEncryptionTemplate<
      X25519PublicKey[],
      X25519PublicKey[],
      X25519PublicKey[]
    >
{
  @field({ type: vec(X25519PublicKey) })
  clock: X25519PublicKey[];

  @field({ type: vec(X25519PublicKey) })
  payload: X25519PublicKey[];

  @field({ type: vec(X25519PublicKey) })
  signature: X25519PublicKey[];

  constructor(properties?: {
    clock: X25519PublicKey[];
    payload: X25519PublicKey[];
    signature: X25519PublicKey[];
  }) {
    super();
    if (properties) {
      this.clock = properties.clock;
      this.payload = properties.payload;
      this.signature = properties.signature;
    }
  }
}

@variant(0)
export class LogQueryRequest {
  @field({ type: vec(LogQuery) })
  queries!: LogQuery[]; // emptry array means pass all

  @field({ type: option("u64") })
  offset?: bigint;

  @field({ type: option("u64") })
  size?: bigint;

  @field({ type: "u8" })
  sort: 0 = 0; // reserved for sort functionality

  constructor(props?: { offset?: bigint; size?: bigint; queries: LogQuery[] }) {
    if (props) {
      this.offset = props.offset;
      this.size = props.size;
      this.queries = props.queries;
    }
  }
}

/**
 * Index for querying entries in any stores log (meta info of entries)
 */
@variant("logindex")
export class LogIndex extends ComposableProgram {
  @field({ type: DQuery })
  query: DQuery<LogQueryRequest, HeadsMessage>;

  _store: Store<any>;
  constructor(props?: { query?: DQuery<LogQueryRequest, HeadsMessage> }) {
    super();
    this.query = props?.query || new DQuery();
  }

  get store(): Store<any> {
    return this._store;
  }

  async setup(properties: {
    store: Store<any>;
    canRead?: CanRead;
    queryTopic?: QueryTopicOption;
  }) {
    this._store = properties?.store;
    await this.query.setup({
      queryType: LogQueryRequest,
      responseType: HeadsMessage,
      responseHandler: this.responseHandler.bind(this),
      canRead: properties.canRead || (() => Promise.resolve(true)),
      queryTopic: properties.queryTopic,
    });
  }

  _queryEntries(filter: (entry: Entry<any>) => boolean): Entry<any>[] {
    // Whether we return the full operation data or just the db value
    return this._store.oplog.values.filter((doc) => filter(doc));
  }

  responseHandler(query: LogQueryRequest): HeadsMessage | undefined {
    if (!this._store.replicate) {
      return undefined; // we do this because we might not have all the heads
    }
    let results = this._queryEntries((entry) => {
      if (query.queries.length === 0) {
        return true;
      }
      for (const q of query.queries) {
        if (q instanceof LogEntryEncryptionQuery) {
          if (entry._payload instanceof EncryptedThing) {
            const check = (
              encryptedThing: EncryptedThing<any>,
              keysToFind: X25519PublicKey[]
            ) => {
              for (const k of encryptedThing._envelope._ks) {
                for (const s of keysToFind) {
                  if (k._recieverPublicKey.equals(s)) {
                    return true;
                  }
                }
              }
              return false;
            };

            if (q.clock.length > 0) {
              if (!check(entry._payload as EncryptedThing<any>, q.clock)) {
                return false;
              }
            }

            if (q.payload.length > 0) {
              if (!check(entry._payload as EncryptedThing<any>, q.payload)) {
                return false;
              }
            }

            if (q.signature.length > 0) {
              if (!check(entry._payload as EncryptedThing<any>, q.signature)) {
                return false;
              }
            }
          } else {
            return (
              q.signature.length == 0 &&
              q.payload.length == 0 &&
              q.clock.length == 0
            );
          }
        } else {
          logger.warn("Unsupported query type: " + q.constructor.name);
          return false;
        }
      }
      return true;
    });

    if (query.offset) {
      results = results.slice(Number(query.offset));
    }

    if (query.size) {
      results = results.slice(0, Number(query.size));
    }
    return new HeadsMessage({
      heads: results,
    });
  }
}
