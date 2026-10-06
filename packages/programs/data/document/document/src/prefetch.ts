import { TypedEventEmitter } from "@libp2p/interface";
import { Cache } from "@peerbit/cache";
import * as types from "@peerbit/document-interface";
import type { RPCResponse } from "@peerbit/rpc";
import { equals } from "uint8arrays";
import { idAgnosticQueryKey } from "./most-common-query-predictor.js";

// --- typed helper ---------------------------------------------------------
type AddEvent = {
	consumable: RPCResponse<types.PredictedSearchRequest<any>>;
};

const prefetchKey = (
	request:
		| types.SearchRequest
		| types.SearchRequestIndexed
		| types.IterationRequest,
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

	/** Store and notify; return a displaced unused prediction for retirement. */
	public add(
		request: RPCResponse<types.PredictedSearchRequest<any>>,
		keyHash: string,
	): RPCResponse<types.PredictedSearchRequest<any>> | void {
		const key = prefetchKey(request.response.request, keyHash);
		const previous = this.prefetch.del(key)?.value;
		const wasConsumed =
			previous && this.isConsumed(previous.response.request.id, keyHash);
		this.prefetch.add(key, request);
		this.dispatchEvent(
			new CustomEvent("add", { detail: { consumable: request } }),
		);
		if (!previous) return;
		const previousRequest = previous.response.request;
		if (equals(previousRequest.id, request.response.request.id)) return;
		// Push snapshots reuse active cursor IDs, not just speculative ones.
		if (
			previousRequest instanceof types.IterationRequest &&
			(previousRequest.pushUpdates != null ||
				previousRequest.keepAliveTtl != null)
		)
			return;
		// A duplicate may re-enter the cache after consumption, and listeners
		// can change translations synchronously while accepting the replacement.
		if (wasConsumed || this.isConsumed(previousRequest.id, keyHash)) return;
		return previous;
	}

	private isConsumed(id: Uint8Array, keyHash: string): boolean {
		for (const peers of this.searchIdTranslationMap.values()) {
			const consumed = peers.get(keyHash);
			if (consumed && equals(consumed, id)) return true;
		}
		return false;
	}

	public consume(
		request:
			| types.SearchRequest
			| types.SearchRequestIndexed
			| types.IterationRequest,
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
		request:
			| types.SearchRequest
			| types.SearchRequestIndexed
			| types.IterationRequest,
	): Map<string, Uint8Array> | undefined {
		return this.searchIdTranslationMap.get(request.idString);
	}

	get size() {
		return this.prefetch.size;
	}
}
