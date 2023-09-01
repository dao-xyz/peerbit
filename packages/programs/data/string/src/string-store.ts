import { field, variant } from "@dao-xyz/borsh";
import { AppendOptions, CanAppend, Change, Entry } from "@peerbit/log";
import { SharedLog, SharedLogOptions } from "@peerbit/shared-log";
import { PublicSignKey, sha256Base64Sync } from "@peerbit/crypto";
import { Program, ProgramEvents } from "@peerbit/program";
import { RPCOptions, RPC, RequestContext } from "@peerbit/rpc";
import { logger as loggerFn } from "@peerbit/logger";
import { StringOperation, StringIndex, encoding } from "./string-index.js";
import {
	AbstractSearchResult,
	NoAccess,
	RangeMetadata,
	RangeMetadatas,
	StringMatch,
	SearchRequest,
	StringResult
} from "./query.js";
import { CustomEvent } from "@libp2p/interfaces/events";
import { concat, fromString } from "uint8arrays";

import { Range } from "./range.js";

const logger = loggerFn({ module: "string" });

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

export type CanRead = (key?: PublicSignKey) => Promise<boolean> | boolean;

export interface StringEvents {
	change: CustomEvent<Change<StringOperation>>;
}

export type CanPerform = (
	operation: StringOperation,
	context: TransactionContext
) => Promise<boolean> | boolean;

export type Args = {
	canRead?: CanRead;
	canPerform?: CanPerform;
	log?: SharedLogOptions;
};

export type TransactionContext = {
	entry: Entry<StringOperation>;
};
@variant("dstring")
export class DString extends Program<Args, StringEvents & ProgramEvents> {
	@field({ type: SharedLog })
	_log: SharedLog<StringOperation>;

	@field({ type: RPC })
	query: RPC<SearchRequest, AbstractSearchResult>;

	@field({ type: StringIndex })
	_index: StringIndex;

	_canRead?: CanRead;

	constructor(properties: {
		id?: Uint8Array;
		query?: RPC<SearchRequest, AbstractSearchResult>;
	}) {
		super();
		this.query = properties.query || new RPC();
		this._log = new SharedLog({ id: properties.id });
		this._index = new StringIndex();
	}

	async open(options?: Args) {
		await this._index.open(this._log.log);

		await this._log.open({
			encoding,
			replicas: {
				min: 0xffffffff // assume a document can not be sharded?
			},
			canAppend: async (entry) => {
				const operation = await entry.getPayloadValue();

				if (!(await this._canPerform(operation, { entry }))) {
					return false;
				}
				return options?.canPerform
					? options.canPerform(operation, { entry })
					: true;
			},
			onChange: async (change) => {
				await this._index.updateIndex(change);
				this.events.dispatchEvent(
					new CustomEvent("change", {
						detail: change
					})
				);
			}
		});

		await this.query.open({
			...options,
			topic: sha256Base64Sync(
				concat([this._log.log.id, fromString("/dstring")])
			),
			responseHandler: this.queryHandler.bind(this),
			queryType: SearchRequest,
			responseType: StringResult
		});
	}

	private async _canPerform(
		operation: StringOperation,
		context: TransactionContext
	): Promise<boolean> {
		if (this._log.log.length === 0 || context.entry.next.length === 0) {
			return true;
		} else {
			for (const next of context.entry.next) {
				if (this._log.log.has(next)) {
					return true;
				}
			}
		}
		return false;
	}

	async add(
		value: string,
		index: Range,
		options?: AppendOptions<StringOperation>
	) {
		return this._log.append(
			new StringOperation({
				index,
				value
			}),
			{
				...options,
				meta: { ...options?.meta, next: await this._log.log.getHeads() }
			}
		);
	}

	async del(index: Range, options?: AppendOptions<StringOperation>) {
		return this.add("", index, options);
	}

	async queryHandler(
		query: SearchRequest,
		ctx: RequestContext
	): Promise<AbstractSearchResult | undefined> {
		logger.debug("Recieved query");
		if (query instanceof SearchRequest == false) {
			logger.debug("Recieved query which is not a StringQueryRequest");
			return;
		}

		const stringQuery = query as SearchRequest;
		if (this._canRead && !(await this._canRead(ctx.from))) {
			return new NoAccess();
		}

		const content = this._index.string;
		const relaventQueries = stringQuery.query.filter(
			(x) => x instanceof StringMatch
		) as StringMatch[];
		if (relaventQueries.length == 0) {
			logger.debug("Responding with all");
			return new StringResult({
				string: content
			});
		}
		const ranges = relaventQueries
			.map((query) => {
				const occurances = findAllOccurrences(
					query.preprocess(content),
					query.preprocess(query.value)
				);
				return occurances.map((ix) => {
					return new RangeMetadata({
						offset: BigInt(ix),
						length: BigInt(query.value.length)
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
			metadatas: new RangeMetadatas({
				metadatas: ranges
			})
		});
	}

	async getValue(options?: {
		remote: {
			callback: (string: string) => any;
			queryOptions: RPCOptions<AbstractSearchResult>;
		};
	}): Promise<string | undefined> {
		if (options?.remote) {
			const counter: Map<string, number> = new Map();
			const responses = await this.query.request(
				new SearchRequest({
					query: []
				}),
				options.remote.queryOptions
			);
			for (const response of responses) {
				if (response.response instanceof NoAccess) {
					logger.error("Missing access");
					continue;
				} else if (response.response instanceof StringResult) {
					options?.remote.callback &&
						options?.remote.callback(response.response.string);
					counter.set(
						response.response.string,
						(counter.get(response.response.string) || 0) + 1
					);
				} else {
					throw new Error("Unsupported type: " + response?.constructor?.name);
				}
			}

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
