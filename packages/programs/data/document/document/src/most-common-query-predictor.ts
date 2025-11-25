import { type AbstractType, deserialize, serialize } from "@dao-xyz/borsh";
import { type PublicSignKey, randomBytes, toBase64 } from "@peerbit/crypto";
import type * as types from "@peerbit/document-interface";

/* ───────────────────── helpers ───────────────────── */

const nullifyQuery = (
	query:
		| types.SearchRequest
		| types.SearchRequestIndexed
		| types.IterationRequest,
) => {
	const cloned = deserialize(serialize(query), query.constructor) as
		| types.SearchRequest
		| types.SearchRequestIndexed
		| types.IterationRequest;
	cloned.id = new Uint8Array(32);
	return cloned;
};

export const idAgnosticQueryKey = (
	query:
		| types.SearchRequest
		| types.SearchRequestIndexed
		| types.IterationRequest,
) => toBase64(serialize(nullifyQuery(query)));

/* ───────────────────── predictor ───────────────────── */

export interface QueryPredictor {
	onRequest: (
		request:
			| types.SearchRequest
			| types.SearchRequestIndexed
			| types.IterationRequest,
		ctx: { from: PublicSignKey },
	) => { ignore: boolean };

	predictedQuery: (
		from: PublicSignKey,
	) =>
		| types.SearchRequest
		| types.SearchRequestIndexed
		| types.IterationRequest
		| undefined;
}

interface QueryStats {
	count: number;
	lastSeen: number;
	queryBytes: Uint8Array;
	queryClazz: AbstractType<types.SearchRequest | types.SearchRequestIndexed>;
}

/**
 * Learns the most common recent queries and predicts the most frequent one.
 * If we just pre-empted a peer with some query, the *first* matching request
 * that arrives from that peer within `ignoreWindow` ms is ignored.
 */
export default class MostCommonQueryPredictor implements QueryPredictor {
	private readonly queries = new Map<string, QueryStats>();

	/**
	 * predicted:
	 * requestKey  →  Map<peerHash, timestamp>
	 */
	private readonly predicted = new Map<string, Map<string, number>>();

	constructor(
		private readonly threshold: number,
		private readonly ttl = 10 * 60 * 1000, // 10 min
		private readonly ignoreWindow = 5_000, // 5 s
	) {}

	/* ───────── housekeeping ───────── */
	private cleanupQueries(now: number) {
		for (const [key, stats] of this.queries) {
			if (now - stats.lastSeen > this.ttl) {
				this.queries.delete(key);
			}
		}
	}

	private cleanupPredictions(now: number) {
		for (const [key, peerMap] of this.predicted) {
			for (const [peer, ts] of peerMap) {
				if (now - ts > this.ignoreWindow) {
					peerMap.delete(peer);
				}
			}
			if (peerMap.size === 0) {
				this.predicted.delete(key);
			}
		}
	}

	/* ───────── public API ───────── */

	onRequest(
		request: types.SearchRequest | types.SearchRequestIndexed,
		{ from }: { from: PublicSignKey },
	): { ignore: boolean } {
		const now = Date.now();
		const peerHash = from.hashcode();
		const key = idAgnosticQueryKey(request);

		/* — 1. Ignore if this (key, peer) pair was just predicted — */
		const peerMap = this.predicted.get(key);
		const ts = peerMap?.get(peerHash);
		let ignore = false;
		if (ts !== undefined && now - ts <= this.ignoreWindow) {
			peerMap!.delete(peerHash); // one-shot
			if (peerMap!.size === 0) {
				this.predicted.delete(key);
			}
			ignore = true;
		}

		/* — 2. Learn from the request — */
		const stats = this.queries.get(key);
		if (stats) {
			stats.count += 1;
			stats.lastSeen = now;
		} else {
			this.queries.set(key, {
				queryBytes: serialize(request),
				queryClazz: request.constructor,
				count: 1,
				lastSeen: now,
			});
		}

		/* — 3. Maintenance — */
		this.cleanupQueries(now);
		this.cleanupPredictions(now);

		return { ignore };
	}

	predictedQuery(
		from: PublicSignKey,
	): types.SearchRequest | types.SearchRequestIndexed | undefined {
		const now = Date.now();
		this.cleanupQueries(now);
		this.cleanupPredictions(now);

		/* pick the most frequent query meeting the threshold */
		let winnerKey: string | undefined;
		let winnerCount = 0;
		for (const [key, { count }] of this.queries) {
			if (count > winnerCount) {
				winnerKey = key;
				winnerCount = count;
			}
		}
		if (!winnerKey || winnerCount < this.threshold) return undefined;

		const winner = this.queries.get(winnerKey)!;
		const cloned = deserialize(winner.queryBytes, winner.queryClazz) as
			| types.SearchRequest
			| types.SearchRequestIndexed;
		cloned.id = randomBytes(32);

		/* remember that we pre-empted `from` with this query */
		const peerHash = from.hashcode();
		let peerMap = this.predicted.get(winnerKey);
		if (!peerMap) {
			peerMap = new Map<string, number>();
			this.predicted.set(winnerKey, peerMap);
		}
		peerMap.set(peerHash, now);

		return cloned;
	}
}
