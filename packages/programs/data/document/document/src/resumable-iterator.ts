import { Cache } from "@peerbit/cache";
import {
	type CollectNextRequest,
	SearchRequest,
	SearchRequestIndexed,
} from "@peerbit/document-interface";
import type * as indexerTypes from "@peerbit/indexer-interface";

export class ResumableIterators<T extends Record<string, any>> {
	constructor(
		readonly index: indexerTypes.Index<T>,
		readonly queues = new Cache<{
			iterator: indexerTypes.IndexIterator<T, undefined>;
			request: SearchRequest | SearchRequestIndexed;
		}>({ max: 1e4 }),
	) {
		// TODO choose upper limit better
	}

	async iterateAndFetch(request: SearchRequest | SearchRequestIndexed) {
		const iterator = this.index.iterate(request);
		const firstResult = await iterator.next(request.fetch);
		if (iterator.done() === true) {
			return firstResult;
		}
		const cachedIterator = {
			iterator,
			request,
		};
		this.queues.add(request.idString, cachedIterator);
		return firstResult;
	}

	async next(
		request: SearchRequest | SearchRequestIndexed | CollectNextRequest,
		iterator = this.queues.get(request.idString),
	) {
		if (!iterator) {
			throw new Error(
				"Missing iterator for request with id: " + request.idString,
			);
		}

		const next = await iterator.iterator.next(
			request instanceof SearchRequest ||
				request instanceof SearchRequestIndexed
				? request.fetch
				: request.amount,
		);

		if (iterator.iterator.done() === true) {
			this.clear(request.idString);
		}
		return next;
	}

	close(close: { idString: string }) {
		this.clear(close.idString);
	}

	private clear(id: string) {
		this.queues.del(id);
	}

	async getPending(id: string) {
		let iterator = this.queues.get(id);
		if (!iterator) {
			return undefined;
		}
		const pending = await iterator.iterator.pending();
		if (pending === 0 && iterator.iterator.done()) {
			this.clear(id);
		}
		return pending;
	}
}
