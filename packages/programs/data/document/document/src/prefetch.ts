import { TypedEventEmitter } from "@libp2p/interface";
import { Cache } from "@peerbit/cache";
import type * as types from "@peerbit/document-interface";
import type { RPCResponse } from "@peerbit/rpc/dist/src";
import { idAgnosticQueryKey } from "./most-common-query-predictor";

// --- typed helper ---------------------------------------------------------
type AddEvent = {
	consumable: RPCResponse<types.PredictedSearchRequest<any>>;
};

const prefetchKey = (
	request: types.SearchRequest | types.SearchRequestIndexed,
	keyHash: string,
) => `${idAgnosticQueryKey(request)} - ${keyHash}`;

// --------------------------------------------------------------------------
export class Prefetch extends TypedEventEmitter<{
	add: CustomEvent<AddEvent>;
}> {
	constructor(
		private prefetch: Cache<
			RPCResponse<types.PredictedSearchRequest<any>>
		> = new Cache({
			max: 100,
			ttl: 1e4,
		}),
		private searchIdTranslationMap: Map<
			string,
			Map<string, Uint8Array>
		> = new Map(),
	) {
		super();
	}

	/** Store the prediction **and** notify listeners */
	public add(
		request: RPCResponse<types.PredictedSearchRequest<any>>,
		keyHash: string,
	): void {
		const key = prefetchKey(request.response.request, keyHash);
		this.prefetch.add(key, request);
		this.dispatchEvent(
			new CustomEvent("add", { detail: { consumable: request } }),
		);
	}

	public consume(
		request: types.SearchRequest | types.SearchRequestIndexed,
		keyHash: string,
	): RPCResponse<types.PredictedSearchRequest<any>> | undefined {
		const key = prefetchKey(request, keyHash);
		const pre = this.prefetch.get(key);
		if (!pre) return;

		this.prefetch.del(key);

		let peerMap = this.searchIdTranslationMap.get(request.idString);
		if (!peerMap) {
			peerMap = new Map();
			this.searchIdTranslationMap.set(request.idString, peerMap);
		}
		peerMap.set(keyHash, pre.response.request.id);
		return pre;
	}

	clear(request: types.SearchRequest | types.SearchRequestIndexed) {
		this.searchIdTranslationMap.delete(request.idString);
	}

	getTranslationMap(
		request: types.SearchRequest | types.SearchRequestIndexed,
	): Map<string, Uint8Array> | undefined {
		return this.searchIdTranslationMap.get(request.idString);
	}

	get size() {
		return this.prefetch.size;
	}
}
