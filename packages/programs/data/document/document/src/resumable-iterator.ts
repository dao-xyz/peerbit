import { Cache } from "@peerbit/cache";
import {
	CollectNextRequest,
	type IterationRequest,
	type SearchRequest,
	type SearchRequestIndexed,
} from "@peerbit/document-interface";
import type * as indexerTypes from "@peerbit/indexer-interface";
import { logger as loggerFn } from "@peerbit/logger";

const iteratorLogger = loggerFn("peerbit:document:index:iterate");

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
		iteratorLogger("iterate:start", {
			id: request.idString,
			fetch: request.fetch,
			keepAlive: Boolean(options?.keepAlive),
		});
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
		iteratorLogger("iterate:queued", {
			id: request.idString,
			keepAlive,
			done: iterator.done() === true,
			batch: firstResult.length,
		});
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
			iteratorLogger.error("Iterator missing", {
				id: request.idString,
			});
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
		let pending: number | undefined;
		try {
			pending = await iterator.iterator.pending();
		} catch {
			pending = undefined;
		}
		iteratorLogger("iterate:next", {
			id: request.idString,
			fetch: fetchAmount,
			keepAlive: iterator.keepAlive,
			resultLength: next.length,
			pending,
			done: iterator.iterator.done?.(),
		});
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
		iteratorLogger("iterate:close", {
			id: close.idString,
		});
		this.clear(close.idString);
	}

	private clear(id: string) {
		iteratorLogger("iterate:clear", {
			id,
		});
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
		// debug
		// console.error("[resumable:getPending]", { id, pending, keepAlive: iterator.keepAlive });
		if (pending === 0 && iterator.iterator.done() && !iterator.keepAlive) {
			this.clear(id);
		}
		iteratorLogger("iterate:pending", {
			id,
			pending,
			keepAlive: iterator.keepAlive,
		});
		return pending;
	}
}
