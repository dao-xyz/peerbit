import { field, fixedArray, serialize, variant } from "@dao-xyz/borsh";
import {
	Documents,
	DocumentIndex,
	IndexedValue,
	DocumentQueryRequest,
	MemoryCompare,
	MemoryCompareQuery,
} from "@dao-xyz/peerbit-document";
import { PublicSignKey } from "@dao-xyz/peerbit-crypto";
import { concat } from "uint8arrays";
import { RPC } from "@dao-xyz/peerbit-rpc";
import { sha256Sync } from "@dao-xyz/peerbit-crypto";

export type RelationResolver = {
	resolve: (
		key: PublicSignKey,
		db: Documents<IdentityRelation>
	) => Promise<IdentityRelation[]>;
	next: (relation: IdentityRelation) => PublicSignKey;
};

export const OFFSET_TO_KEY = 74;
export const KEY_OFFSET = 2 + 32; // Relation discriminator + IdentityRelation discriminator + id
export const getFromByTo: RelationResolver = {
	resolve: async (to: PublicSignKey, db: Documents<IdentityRelation>) => {
		const ser = serialize(to);
		return (
			await db.index.queryHandler(
				new DocumentQueryRequest({
					queries: [
						new MemoryCompareQuery({
							compares: [
								new MemoryCompare({
									bytes: ser,
									offset: BigInt(KEY_OFFSET + OFFSET_TO_KEY),
								}),
							],
						}),
					],
				})
			)
		).map((x) => x.value);
	},
	next: (relation) => relation.from,
};

export const getToByFrom: RelationResolver = {
	resolve: async (from: PublicSignKey, db: Documents<IdentityRelation>) => {
		const ser = serialize(from);
		return (
			await db.index.queryHandler(
				new DocumentQueryRequest({
					queries: [
						new MemoryCompareQuery({
							compares: [
								new MemoryCompare({
									bytes: ser,
									offset: BigInt(KEY_OFFSET),
								}),
							],
						}),
					],
				})
			)
		).map((x) => x.value);
	},
	next: (relation) => relation.to,
};

export async function* getPathGenerator(
	from: PublicSignKey,
	db: Documents<IdentityRelation>,
	resolver: RelationResolver
) {
	let iter = [from];
	const visited = new Set();
	while (iter.length > 0) {
		const newIter: PublicSignKey[] = [];
		for (const value of iter) {
			const results = await resolver.resolve(value, db);
			for (const result of results) {
				if (result instanceof IdentityRelation) {
					if (visited.has(result.id)) {
						return;
					}
					visited.add(result.id);
					yield result;

					newIter.push(resolver.next(result));
				}
			}
		}
		iter = newIter;
	}
}

/**
 * Get path, to target.
 * @param start
 * @param target
 * @param db
 * @returns
 */
export const hasPathToTarget = async (
	start: PublicSignKey,
	target: (key: PublicSignKey) => boolean,
	db: Documents<IdentityRelation>,
	resolver: RelationResolver
): Promise<boolean> => {
	if (!db) {
		throw new Error("Not initalized");
	}

	const current = start;
	if (target(current)) {
		return true;
	}

	const iterator = getPathGenerator(current, db, resolver);
	for await (const relation of iterator) {
		if (target(relation.from)) {
			return true;
		}
	}
	return false;
};

@variant(0)
export abstract class AbstractRelation {
	@field({ type: fixedArray("u8", 32) })
	id: Uint8Array;
}

@variant(0)
export class IdentityRelation extends AbstractRelation {
	@field({ type: PublicSignKey })
	_from: PublicSignKey;

	@field({ type: Uint8Array })
	padding: Uint8Array;

	@field({ type: PublicSignKey })
	_to: PublicSignKey;

	constructor(properties?: {
		to: PublicSignKey; // signed by truster
		from: PublicSignKey;
	}) {
		super();
		if (properties) {
			this._from = properties.from;
			this._to = properties.to;
			const serFrom = serialize(this._from);
			this.padding = new Uint8Array(OFFSET_TO_KEY - serFrom.length - 4); // -4 comes from u32 describing length the padding array
			this.initializeId();
		}
	}

	get from(): PublicSignKey {
		return this._from;
	}

	get to(): PublicSignKey {
		return this._to;
	}

	initializeId() {
		this.id = IdentityRelation.id(this.to, this.from);
	}

	static id(to: PublicSignKey, from: PublicSignKey) {
		// we do make sure id has fixed length, this is important because we want the byte offest of the `trustee` and `truster` to be fixed
		return sha256Sync(concat([serialize(to), serialize(from)]));
	}
}

export const hasPath = async (
	start: PublicSignKey,
	end: PublicSignKey,
	db: Documents<IdentityRelation>,
	resolver: RelationResolver
): Promise<boolean> => {
	return hasPathToTarget(start, (key) => end.equals(key), db, resolver);
};

export const getRelation = (
	from: PublicSignKey,
	to: PublicSignKey,
	db: Documents<IdentityRelation>
): IndexedValue<IdentityRelation> | undefined => {
	return db.index.get(new IdentityRelation({ from, to }).id);
};

export const createIdentityGraphStore = (props: {
	id: string;
	rpcRegion?: string;
}) =>
	new Documents<IdentityRelation>({
		index: new DocumentIndex({
			indexBy: "id",
			query: new RPC(),
		}),
	});
