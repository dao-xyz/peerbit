import { Cache } from "@peerbit/cache";
import {
	type CloseIteratorRequest,
	type CollectNextRequest,
	SearchRequest,
} from "@peerbit/document-interface";
import type * as indexerTypes from "@peerbit/indexer-interface";

export class ResumableIterators<T extends Record<string, any>> {
	constructor(
		readonly index: indexerTypes.Index<T>,
		readonly queues = new Cache<{
			iterator: indexerTypes.IndexIterator<T, undefined>;
		}>({ max: 1e4 }),
	) {
		// TODO choose upper limit better
	}

	iterateAndFetch(request: SearchRequest) {
		const iterator = this.index.iterate(request);
		const cachedIterator = {
			iterator,
		};
		this.queues.add(request.idString, cachedIterator);
		return this.next(request, cachedIterator);
	}

	async next(
		request: SearchRequest | CollectNextRequest,
		iterator = this.queues.get(request.idString),
	) {
		if (!iterator) {
			throw new Error(
				"Missing iterator for request with id: " + request.idString,
			);
		}

		const next = await iterator.iterator.next(
			request instanceof SearchRequest ? request.fetch : request.amount,
		);
		if (iterator.iterator.done() === true) {
			this.clear(request.idString);
		}
		return next;
	}

	async close(close: CloseIteratorRequest) {
		this.clear(close.idString);
	}

	private clear(id: string) {
		this.queues.del(id);
	}

	getPending(id: string) {
		return this.queues.get(id)?.iterator.pending();
	}
}
