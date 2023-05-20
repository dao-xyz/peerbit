import { field, variant } from "@dao-xyz/borsh";
import { AppendOptions, CanAppend, Entry, Log } from "@dao-xyz/peerbit-log";
import { SignatureWithKey } from "@dao-xyz/peerbit-crypto";
import { Program } from "@dao-xyz/peerbit-program";
import { RPCOptions, CanRead, RPC } from "@dao-xyz/peerbit-rpc";
import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
import { StringOperation, StringIndex, encoding } from "./string-index.js";
import {
	RangeMetadata,
	RangeMetadatas,
	StringMatch,
	StringQueryRequest,
	StringResult,
} from "./query.js";

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

export type StringStoreOptions = {
	canRead?: (key: SignatureWithKey) => Promise<boolean>;
};

@variant("dstring")
export class DString extends Program {
	@field({ type: Log })
	_log: Log<StringOperation>;

	@field({ type: RPC })
	query: RPC<StringQueryRequest, StringResult>;

	@field({ type: StringIndex })
	_index: StringIndex;

	_optionCanAppend?: CanAppend<StringOperation>;

	constructor(properties: { query?: RPC<StringQueryRequest, StringResult> }) {
		super();
		this.query = properties.query || new RPC();
		this._log = new Log();
		this._index = new StringIndex();
	}

	async setup(options?: {
		canRead?: CanRead;
		canAppend?: CanAppend<StringOperation>;
	}) {
		this._optionCanAppend = options?.canAppend;
		this._log.setup({
			encoding,
			canAppend: this.canAppend.bind(this),
			onChange: this._index.updateIndex.bind(this._index),
		});

		await this._index.setup(this._log);
		await this.query.setup({
			...options,
			topic: this._log.idString + "/" + "dstring",
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
		if (this._log.length === 0) {
			return true;
		} else {
			for (const next of entry.next) {
				if (this._log.has(next)) {
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
				value,
			}),
			{ nexts: await this._log.getHeads(), ...options }
		);
	}

	async del(index: Range, options?: AppendOptions<StringOperation>) {
		const operation = {
			index,
		} as StringOperation;
		return this._log.append(operation, {
			nexts: await this._log.getHeads(),
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
			(x) => x instanceof StringMatch
		) as StringMatch[];
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
					return new RangeMetadata({
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
			metadatas: new RangeMetadatas({
				metadatas: ranges,
			}),
		});
	}

	async toString(options?: {
		remote: {
			callback: (string: string) => any;
			queryOptions: RPCOptions<StringResult>;
		};
	}): Promise<string | undefined> {
		if (options?.remote) {
			const counter: Map<string, number> = new Map();
			const responses = await this.query.send(
				new StringQueryRequest({
					queries: [],
				}),
				options.remote.queryOptions
			);
			for (const response of responses) {
				options?.remote.callback &&
					options?.remote.callback(response.response.string);
				counter.set(
					response.response.string,
					(counter.get(response.response.string) || 0) + 1
				);
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
