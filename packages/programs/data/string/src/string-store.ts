import { StringOperation, StringIndex, encoding } from "./string-index.js";
import {
    RangeCoordinate,
    RangeCoordinates,
    StringMatchQuery,
    StringQueryRequest,
    StringResult,
} from "./query.js";

import { Range } from "./range.js";
import { field, variant } from "@dao-xyz/borsh";
import { AddOperationOptions, Store } from "@dao-xyz/peerbit-store";
import { CanAppend, Entry } from "@dao-xyz/ipfs-log";
import { SignatureWithKey } from "@dao-xyz/peerbit-crypto";
import { Program } from "@dao-xyz/peerbit-program";
import { RPCOptions, CanRead, RPC } from "@dao-xyz/peerbit-rpc";

import pino from "pino";
const logger = pino().child({ module: "string" });

export const STRING_STORE_TYPE = "string_store";
const findAllOccurrences = (str: string, substr: string): number[] => {
    str = str.toLowerCase();

    const result: number[] = [];

    let idx = str.indexOf(substr);

    while (idx !== -1) {
        result.push(idx);
        idx = str.indexOf(substr, idx + 1);
    }
    return result;
};

export type StringStoreOptions = {
    canRead?: (key: SignatureWithKey) => Promise<boolean>;
};

@variant("dstring")
export class DString extends Program {
    @field({ type: Store })
    store: Store<StringOperation>;

    @field({ type: RPC })
    query: RPC<StringQueryRequest, StringResult>;

    @field({ type: StringIndex })
    _index: StringIndex;

    _optionCanAppend?: CanAppend<StringOperation>;

    constructor(properties: { query?: RPC<StringQueryRequest, StringResult> }) {
        super();
        if (properties) {
            this.query = properties.query || new RPC();
            this.store = new Store();
            this._index = new StringIndex();
        }
    }

    async setup(options?: {
        canRead?: CanRead;
        canAppend?: CanAppend<StringOperation>;
    }) {
        this._optionCanAppend = options?.canAppend;
        this.store.setup({
            encoding,
            canAppend: this.canAppend.bind(this),
            onUpdate: this._index.updateIndex.bind(this._index),
        });
        if (options?.canAppend) {
            this.store.canAppend = options.canAppend;
        }

        await this.query.setup({
            ...options,
            context: () => this.address,
            canRead: options?.canRead,
            responseHandler: this.queryHandler.bind(this),
            queryType: StringQueryRequest,
            responseType: StringResult,
        });
    }

    async canAppend(entry: Entry<StringOperation>): Promise<boolean> {
        if (!(await this._canAppend(entry))) {
            return false;
        }
        if (this._optionCanAppend && !(await this._optionCanAppend(entry))) {
            return false;
        }
        return true;
    }

    async _canAppend(entry: Entry<StringOperation>): Promise<boolean> {
        if (this.store.oplog.length === 0) {
            return true;
        } else {
            for (const next of entry.next) {
                if (this.store.oplog.has(next)) {
                    return true;
                }
            }
        }
        return false;
    }

    add(
        value: string,
        index: Range,
        options?: AddOperationOptions<StringOperation>
    ) {
        return this.store._addOperation(
            new StringOperation({
                index,
                value,
            }),
            { nexts: this.store.oplog.heads, ...options }
        );
    }

    del(index: Range, options?: AddOperationOptions<StringOperation>) {
        const operation = {
            index,
        } as StringOperation;
        return this.store._addOperation(operation, {
            nexts: this.store.oplog.heads,
            ...options,
        });
    }

    async queryHandler(
        query: StringQueryRequest
    ): Promise<StringResult | undefined> {
        logger.debug("Recieved query");
        if (query instanceof StringQueryRequest == false) {
            logger.debug("Recieved query which is not a StringQueryRequest");
            return;
        }
        const stringQuery = query as StringQueryRequest;

        const content = this._index.string;
        const relaventQueries = stringQuery.queries.filter(
            (x) => x instanceof StringMatchQuery
        ) as StringMatchQuery[];
        if (relaventQueries.length == 0) {
            logger.debug("Responding with all");
            return new StringResult({
                string: content,
            });
        }
        const ranges = relaventQueries
            .map((query) => {
                const occurances = findAllOccurrences(
                    query.preprocess(content),
                    query.preprocess(query.value)
                );
                return occurances.map((ix) => {
                    return new RangeCoordinate({
                        offset: BigInt(ix),
                        length: BigInt(query.value.length),
                    });
                });
            })
            .flat(1);

        if (ranges.length == 0) {
            logger.debug("Could not find any matches");
            return;
        }

        return new StringResult({
            string: content,
            coordinates: new RangeCoordinates({
                coordinates: ranges,
            }),
        });
    }

    async toString(options?: {
        remote: {
            callback: (string: string) => any;
            queryOptions: RPCOptions;
        };
    }): Promise<string | undefined> {
        if (options?.remote) {
            const counter: Map<string, number> = new Map();
            await this.query.send(
                new StringQueryRequest({
                    queries: [],
                }),
                (response) => {
                    options?.remote.callback &&
                        options?.remote.callback(response.string);
                    counter.set(
                        response.string,
                        (counter.get(response.string) || 0) + 1
                    );
                },
                options.remote.queryOptions
            );
            let max = -1;
            let ret: string | undefined = undefined;
            counter.forEach((v, k) => {
                if (max < v) {
                    max = v;
                    ret = k;
                }
            });
            return ret;
        } else {
            return this._index.string;
        }
    }
}
