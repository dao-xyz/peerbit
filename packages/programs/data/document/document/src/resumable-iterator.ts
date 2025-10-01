import { Cache } from "@peerbit/cache";
import {
	CollectNextRequest,
	type IterationRequest,
	type SearchRequest,
	type SearchRequestIndexed,
} from "@peerbit/document-interface";
import type * as indexerTypes from "@peerbit/indexer-interface";

export class ResumableIterators<T extends Record<string, any>> {
	constructor(
		readonly index: indexerTypes.Index<T>,
		readonly queues = new Cache<{
			iterator: indexerTypes.IndexIterator<T, undefined>;
			request: SearchRequest | SearchRequestIndexed | IterationRequest;
			keepAlive: boolean;
		}>({ max: 1e4 }),
	) {
		// TODO choose upper limit better
	}

	async iterateAndFetch(
		request: SearchRequest | SearchRequestIndexed | IterationRequest,
		options?: { keepAlive?: boolean },
	) {
		const iterator = this.index.iterate(request);
		const firstResult = await iterator.next(request.fetch);
		const keepAlive = options?.keepAlive === true;
		if (keepAlive || iterator.done() !== true) {
			const cachedIterator = {
				iterator,
				request,
				keepAlive,
			};
			this.queues.add(request.idString, cachedIterator);
		}
		/* console.debug(
			"[ResumableIterators] iterateAndFetch",
			request.idString,
			{ keepAlive },
		); */
		return firstResult;
	}

	async next(
		request:
			| SearchRequest
			| SearchRequestIndexed
			| IterationRequest
			| CollectNextRequest,
		options?: { keepAlive?: boolean },
	) {
		const iterator = this.queues.get(request.idString);
		if (!iterator) {
			throw new Error(
				"Missing iterator for request with id: " + request.idString,
			);
		}

		if (options?.keepAlive && !iterator.keepAlive) {
			iterator.keepAlive = true;
			this.queues.add(request.idString, iterator);
		}

		const fetchAmount =
			request instanceof CollectNextRequest
				? request.amount
				: iterator.request.fetch;
		const next = await iterator.iterator.next(fetchAmount);
		/* console.debug(
			"[ResumableIterators] next",
			request.idString,
			{ keepAlive: iterator.keepAlive, resultLength: next.length },
		); */

		if (!iterator.keepAlive && iterator.iterator.done() === true) {
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

	has(id: string) {
		return this.queues.has(id);
	}

	async getPending(id: string) {
		const iterator = this.queues.get(id);
		if (!iterator) {
			return undefined;
		}
		const pending = await iterator.iterator.pending();
		if (pending === 0 && iterator.iterator.done() && !iterator.keepAlive) {
			this.clear(id);
		}
		return pending;
	}
}
