import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { PublicSignKey, equals, randomBytes, toBase64 } from "@peerbit/crypto";
import {
	And,
	BoolQuery,
	ByteMatchQuery,
	Compare,
	type Index,
	type IndexIterator,
	type IndexedResult,
	type IndexedResults,
	IntegerCompare,
	Not,
	Or,
	type Query,
	type ReturnTypeFromShape,
	type Shape,
	Sort,
	SortDirection,
	StringMatch,
	iteratorInSeries,
} from "@peerbit/indexer-interface";
import { id } from "@peerbit/indexer-interface";
import { Meta, ShallowMeta } from "@peerbit/log";
import { type ReplicationChanges, type u32 } from "./replication-domain.js";
import { MAX_U32, scaleToU32 } from "./role.js";
import { groupByGidSync } from "./utils.js";

export enum ReplicationIntent {
	NonStrict = 0, // indicates that the segment will be replicated and nearby data might be replicated as well
	Strict = 1, // only replicate data in the segment to the specified replicator, not any other data
}

export const getSegmentsFromOffsetAndRange = (
	offset: number,
	factor: number,
): [[number, number], [number, number]] => {
	let start1 = offset;
	let end1Unscaled = offset + factor; // only add factor if it is not 1 to prevent numerical issues (like (0.9 + 1) % 1 => 0.8999999)
	let end1 = Math.min(end1Unscaled, MAX_U32);
	return [
		[start1, end1],
		end1Unscaled > MAX_U32
			? [0, (factor !== MAX_U32 ? offset + factor : offset) % MAX_U32]
			: [start1, end1],
	];
};

export const shouldAssigneToRangeBoundary = (
	leaders:
		| Map<
				string,
				{
					intersecting: boolean;
				}
		  >
		| false,
	minReplicas: number,
) => {
	let assignedToRangeBoundary = leaders === false || leaders.size < minReplicas;
	if (!assignedToRangeBoundary && leaders) {
		for (const [_, { intersecting }] of leaders) {
			if (!intersecting) {
				assignedToRangeBoundary = true;
				break;
			}
		}
	}
	return assignedToRangeBoundary;
};
export class EntryReplicated {
	@id({ type: "string" })
	id: string; // hash + coordinate

	@field({ type: "string" })
	hash: string;

	@field({ type: "string" })
	gid: string;

	@field({ type: "u32" })
	coordinate: number;

	@field({ type: "u64" })
	wallTime: bigint;

	@field({ type: "bool" })
	assignedToRangeBoundary: boolean;

	@field({ type: Uint8Array })
	private _meta: Uint8Array;

	private _metaResolved: ShallowMeta;

	constructor(properties: {
		coordinate: number;
		hash: string;
		meta: Meta;
		assignedToRangeBoundary: boolean;
	}) {
		this.coordinate = properties.coordinate;
		this.hash = properties.hash;
		this.gid = properties.meta.gid;
		this.id = this.hash + "-" + this.coordinate;
		this.wallTime = properties.meta.clock.timestamp.wallTime;
		const shallow =
			properties.meta instanceof Meta
				? new ShallowMeta(properties.meta)
				: properties.meta;
		this._meta = serialize(shallow);
		this._metaResolved = deserialize(this._meta, ShallowMeta);
		this._metaResolved = properties.meta;
		this.assignedToRangeBoundary = properties.assignedToRangeBoundary;
	}

	get meta(): ShallowMeta {
		if (!this._metaResolved) {
			this._metaResolved = deserialize(this._meta, ShallowMeta);
		}
		return this._metaResolved;
	}
}

@variant(0)
export class ReplicationRange {
	@field({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: "u64" })
	timestamp: bigint;

	@field({ type: "u32" })
	private _offset: number;

	@field({ type: "u32" })
	private _factor: number;

	@field({ type: "u8" })
	mode: ReplicationIntent;

	constructor(properties: {
		id: Uint8Array;
		offset: number;
		factor: number;
		timestamp: bigint;
		mode: ReplicationIntent;
	}) {
		const { id, offset, factor, timestamp, mode } = properties;
		this.id = id;
		this._offset = offset;
		this._factor = factor;
		this.timestamp = timestamp;
		this.mode = mode;
	}

	get factor(): number {
		return this._factor;
	}

	get offset(): number {
		return this._offset;
	}

	toReplicationRangeIndexable(key: PublicSignKey): ReplicationRangeIndexable {
		return new ReplicationRangeIndexable({
			id: this.id,
			publicKeyHash: key.hashcode(),
			offset: this.offset,
			length: this.factor,
			timestamp: this.timestamp,
			mode: this.mode,
		});
	}
}

export class ReplicationRangeIndexable {
	@id({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: "string" })
	hash: string;

	@field({ type: "u64" })
	timestamp: bigint;

	@field({ type: "u32" })
	start1!: number;

	@field({ type: "u32" })
	end1!: number;

	@field({ type: "u32" })
	start2!: number;

	@field({ type: "u32" })
	end2!: number;

	@field({ type: "u32" })
	width!: number;

	@field({ type: "u8" })
	mode: ReplicationIntent;

	constructor(
		properties: {
			id?: Uint8Array;
			normalized?: boolean;
			offset: number;
			length: number;
			mode?: ReplicationIntent;
			timestamp?: bigint;
		} & ({ publicKeyHash: string } | { publicKey: PublicSignKey }),
	) {
		this.id = properties.id ?? randomBytes(32);
		this.hash =
			(properties as { publicKeyHash: string }).publicKeyHash ||
			(properties as { publicKey: PublicSignKey }).publicKey.hashcode();
		if (!properties.normalized) {
			this.transform({ length: properties.length, offset: properties.offset });
		} else {
			this.transform({
				length: scaleToU32(properties.length),
				offset: scaleToU32(properties.offset),
			});
		}

		this.mode = properties.mode ?? ReplicationIntent.NonStrict;
		this.timestamp = properties.timestamp || BigInt(0);
	}

	private transform(properties: { offset: number; length: number }) {
		const ranges = getSegmentsFromOffsetAndRange(
			properties.offset,
			properties.length,
		);
		this.start1 = Math.round(ranges[0][0]);
		this.end1 = Math.round(ranges[0][1]);
		this.start2 = Math.round(ranges[1][0]);
		this.end2 = Math.round(ranges[1][1]);

		this.width =
			this.end1 -
			this.start1 +
			(this.end2 < this.end1 ? this.end2 - this.start2 : 0);

		if (
			this.start1 > 0xffffffff ||
			this.end1 > 0xffffffff ||
			this.start2 > 0xffffffff ||
			this.end2 > 0xffffffff ||
			this.width > 0xffffffff ||
			this.width < 0
		) {
			throw new Error("Segment coordinate out of bounds");
		}
	}

	get idString() {
		return toBase64(this.id);
	}

	contains(point: number) {
		return (
			(point >= this.start1 && point < this.end1) ||
			(point >= this.start2 && point < this.end2)
		);
	}

	overlaps(other: ReplicationRangeIndexable, checkOther = true): boolean {
		if (
			this.contains(other.start1) ||
			this.contains(other.start2) ||
			this.contains(other.end1 - 1) ||
			this.contains(other.end2 - 1)
		) {
			return true;
		}

		if (checkOther) {
			return other.overlaps(this, false);
		}
		return false;
	}
	toReplicationRange() {
		return new ReplicationRange({
			id: this.id,
			offset: this.start1,
			factor: this.width,
			timestamp: this.timestamp,
			mode: this.mode,
		});
	}

	distanceTo(point: number) {
		let wrappedPoint = MAX_U32 - point;
		return Math.min(
			Math.abs(this.start1 - point),
			Math.abs(this.end2 - point),
			Math.abs(this.start1 - wrappedPoint),
			Math.abs(this.end2 - wrappedPoint),
		);
	}
	get wrapped() {
		return this.end2 < this.end1;
	}

	get widthNormalized() {
		return this.width / MAX_U32;
	}

	equals(other: ReplicationRangeIndexable) {
		if (
			equals(this.id, other.id) &&
			this.hash === other.hash &&
			this.timestamp === other.timestamp &&
			this.mode === other.mode &&
			this.start1 === other.start1 &&
			this.end1 === other.end1 &&
			this.start2 === other.start2 &&
			this.end2 === other.end2 &&
			this.width === other.width
		) {
			return true;
		}

		return false;
	}

	equalRange(other: ReplicationRangeIndexable) {
		return (
			this.start1 === other.start1 &&
			this.end1 === other.end1 &&
			this.start2 === other.start2 &&
			this.end2 === other.end2
		);
	}

	toString() {
		let roundToTwoDecimals = (num: number) => Math.round(num * 100) / 100;

		if (Math.abs(this.start1 - this.start2) < 0.0001) {
			return `([${roundToTwoDecimals(this.start1 / MAX_U32)}, ${roundToTwoDecimals(this.end1 / MAX_U32)}])`;
		}
		return `([${roundToTwoDecimals(this.start1 / MAX_U32)}, ${roundToTwoDecimals(this.end1 / MAX_U32)}] [${roundToTwoDecimals(this.start2 / MAX_U32)}, ${roundToTwoDecimals(this.end2 / MAX_U32)}])`;
	}

	toStringDetailed() {
		return `(hash ${this.hash} range: ${this.toString()})`;
	}

	/* removeRange(other: ReplicationRangeIndexable): ReplicationRangeIndexable | ReplicationRangeIndexable[] {
		if (!this.overlaps(other)) {
			return this
		}

		if (this.equalRange(other)) {
			return []
		}

		let diff: ReplicationRangeIndexable[] = [];
		let start1 = this.start1;
		if (other.start1 > start1) {
			diff.push(new ReplicationRangeIndexable({
				id: this.id,
				offset: this.start1,
				length: other.start1 - this.start1,
				mode: this.mode,
				publicKeyHash: this.hash,
				timestamp: this.timestamp,
				normalized: false
			}));

			start1 = other.end2
		}

		if (other.end1 < this.end1) {
			diff.push(new ReplicationRangeIndexable({
				id: this.id,
				offset: other.end1,
				length: this.end1 - other.end1,
				mode: this.mode,
				publicKeyHash: this.hash,
				timestamp: this.timestamp,
				normalized: false
			}));
		}

		if (other.start2 > this.start2) {
			diff.push(new ReplicationRangeIndexable({
				id: this.id,
				offset: this.start2,
				length: other.start2 - this.start2,
				mode: this.mode,
				publicKeyHash: this.hash,
				timestamp: this.timestamp,
				normalized: false
			}));
		}

		if (other.end2 < this.end2) {
			diff.push(new ReplicationRangeIndexable({
				id: this.id,
				offset: other.end2,
				length: this.end2 - other.end2,
				mode: this.mode,
				publicKeyHash: this.hash,
				timestamp: this.timestamp,
				normalized: false
			}));
		}

		return diff;
	} */
}

const containingPoint = <S extends Shape | undefined = undefined>(
	rects: Index<ReplicationRangeIndexable>,
	point: number,
	roleAgeLimit: number,
	matured: boolean,
	now: number,
	options?: {
		shape?: S;
		sort?: Sort[];
	},
): IndexIterator<ReplicationRangeIndexable, S> => {
	// point is between 0 and 1, and the range can start at any offset between 0 and 1 and have length between 0 and 1

	let queries = [
		new Or([
			new And([
				new IntegerCompare({
					key: "start1",
					compare: Compare.LessOrEqual,
					value: point,
				}),
				new IntegerCompare({
					key: "end1",
					compare: Compare.Greater,
					value: point,
				}),
			]),
			new And([
				new IntegerCompare({
					key: "start2",
					compare: Compare.LessOrEqual,
					value: point,
				}),
				new IntegerCompare({
					key: "end2",
					compare: Compare.Greater,
					value: point,
				}),
			]),
		]),
		new IntegerCompare({
			key: "timestamp",
			compare: matured ? Compare.LessOrEqual : Compare.Greater,
			value: BigInt(now - roleAgeLimit),
		}),
	];
	return rects.iterate(
		{
			query: queries,
			sort: options?.sort,
		},
		options,
	);
};

const getClosest = <S extends Shape | undefined = undefined>(
	direction: "above" | "below",
	rects: Index<ReplicationRangeIndexable>,
	point: number,
	roleAgeLimit: number,
	matured: boolean,
	now: number,
	includeStrict: boolean,
	options?: { shape?: S },
): IndexIterator<ReplicationRangeIndexable, S> => {
	const createQueries = (p: number, equality: boolean) => {
		let queries: Query[];
		if (direction === "below") {
			queries = [
				new IntegerCompare({
					key: "end2",
					compare: equality ? Compare.LessOrEqual : Compare.Less,
					value: p,
				}),
				new IntegerCompare({
					key: "timestamp",
					compare: matured ? Compare.LessOrEqual : Compare.GreaterOrEqual,
					value: BigInt(now - roleAgeLimit),
				}),
			];
		} else {
			queries = [
				new IntegerCompare({
					key: "start1",
					compare: equality ? Compare.GreaterOrEqual : Compare.Greater,
					value: p,
				}),
				new IntegerCompare({
					key: "timestamp",
					compare: matured ? Compare.LessOrEqual : Compare.GreaterOrEqual,
					value: BigInt(now - roleAgeLimit),
				}),
			];
		}
		queries.push(
			new IntegerCompare({ key: "width", compare: Compare.Greater, value: 0 }),
		);

		if (!includeStrict) {
			queries.push(
				new IntegerCompare({
					key: "mode",
					compare: Compare.Equal,
					value: ReplicationIntent.NonStrict,
				}),
			);
		}
		return queries;
	};

	const sortByOldest = new Sort({ key: "timestamp", direction: "asc" });
	const sortByHash = new Sort({ key: "hash", direction: "asc" }); // when breaking even

	const iterator = rects.iterate(
		{
			query: createQueries(point, false),
			sort: [
				direction === "below"
					? new Sort({ key: ["end2"], direction: "desc" })
					: new Sort({ key: ["start1"], direction: "asc" }),
				sortByOldest,
				sortByHash,
			],
		},
		options,
	);

	const iteratorWrapped = rects.iterate(
		{
			query: createQueries(direction === "below" ? MAX_U32 : 0, true),
			sort: [
				direction === "below"
					? new Sort({ key: ["end2"], direction: "desc" })
					: new Sort({ key: ["start1"], direction: "asc" }),
				sortByOldest,
				sortByHash,
			],
		},
		options,
	);

	return joinIterator<S>([iterator, iteratorWrapped], point, direction);
};

export const hasCoveringRange = async (
	rects: Index<ReplicationRangeIndexable>,
	range: ReplicationRangeIndexable,
) => {
	return (
		(await rects.count({
			query: [
				new Or([
					new And([
						new IntegerCompare({
							key: "start1",
							compare: Compare.LessOrEqual,
							value: range.start1,
						}),
						new IntegerCompare({
							key: "end1",
							compare: Compare.GreaterOrEqual,
							value: range.end1,
						}),
					]),
					new And([
						new IntegerCompare({
							key: "start2",
							compare: Compare.LessOrEqual,
							value: range.start1,
						}),
						new IntegerCompare({
							key: "end2",
							compare: Compare.GreaterOrEqual,
							value: range.end1,
						}),
					]),
				]),
				new Or([
					new And([
						new IntegerCompare({
							key: "start1",
							compare: Compare.LessOrEqual,
							value: range.start2,
						}),
						new IntegerCompare({
							key: "end1",
							compare: Compare.GreaterOrEqual,
							value: range.end2,
						}),
					]),
					new And([
						new IntegerCompare({
							key: "start2",
							compare: Compare.LessOrEqual,
							value: range.start2,
						}),
						new IntegerCompare({
							key: "end2",
							compare: Compare.GreaterOrEqual,
							value: range.end2,
						}),
					]),
				]),
				new StringMatch({
					key: "hash",
					value: range.hash,
				}),
				// assume that we are looking for other ranges, not want to update an existing one
				new Not(
					new ByteMatchQuery({
						key: "id",
						value: range.id,
					}),
				),
			],
		})) > 0
	);
};

export const getDistance = (
	from: number,
	to: number,
	direction: "above" | "below" | "closest",
	end = MAX_U32,
) => {
	// if direction is 'above' only measure distance from 'from to 'to' from above.
	// i.e if from < to, then from needs to wrap around 0 to 1 and then to to
	// if direction is 'below' and from > to, then from needs to wrap around 1 to 0 and then to to
	// if direction is 'closest' then the shortest distance is the distance

	// also from is 0.1 and to is 0.9, then distance should be 0.2 not 0.8
	// same as for if from is 0.9 and to is 0.1, then distance should be 0.2 not 0.8

	if (direction === "closest") {
		if (from === to) {
			return 0;
		}

		return Math.min(Math.abs(from - to), Math.abs(end - Math.abs(from - to)));
	}

	if (direction === "above") {
		if (from <= to) {
			return Math.abs(end - to) + from;
		}
		return from - to;
	}

	if (direction === "below") {
		if (from >= to) {
			return Math.abs(end - from) + to;
		}
		return to - from;
	}

	throw new Error("Invalid direction");
};

const joinIterator = <S extends Shape | undefined = undefined>(
	iterators: IndexIterator<ReplicationRangeIndexable, S>[],
	point: number,
	direction: "above" | "below" | "closest",
): IndexIterator<ReplicationRangeIndexable, S> => {
	let queues: {
		elements: {
			result: IndexedResult<ReturnTypeFromShape<ReplicationRangeIndexable, S>>;
			dist: number;
		}[];
	}[] = [];

	return {
		next: async (
			count: number,
		): Promise<
			IndexedResults<ReturnTypeFromShape<ReplicationRangeIndexable, S>>
		> => {
			let results: IndexedResults<
				ReturnTypeFromShape<ReplicationRangeIndexable, S>
			> = [];
			for (let i = 0; i < iterators.length; i++) {
				let queue = queues[i];
				if (!queue) {
					queue = { elements: [] };
					queues[i] = queue;
				}
				let iterator = iterators[i];
				if (queue.elements.length < count && iterator.done() !== true) {
					let res = await iterator.next(count);

					for (const el of res) {
						const closest = el.value;

						let dist: number;
						if (direction === "closest") {
							dist = Math.min(
								getDistance(closest.start1, point, direction),
								getDistance(closest.end2, point, direction),
							);
						} else if (direction === "above") {
							dist = getDistance(closest.start1, point, direction);
						} else if (direction === "below") {
							dist = getDistance(closest.end2, point, direction);
						} else {
							throw new Error("Invalid direction");
						}

						queue.elements.push({ result: el, dist });
					}
				}
			}

			// pull the 'count' the closest element from one of the queue

			for (let i = 0; i < count; i++) {
				let closestQueue = -1;
				let closestDist = Number.MAX_SAFE_INTEGER;
				for (let j = 0; j < queues.length; j++) {
					let queue = queues[j];
					if (queue && queue.elements.length > 0) {
						let closest = queue.elements[0];
						if (closest.dist < closestDist) {
							closestDist = closest.dist;
							closestQueue = j;
						}
					}
				}

				if (closestQueue === -1) {
					break;
				}

				let closest = queues[closestQueue]?.elements.shift();
				if (closest) {
					results.push(closest.result);
				}
			}
			return results;
		},
		pending: async () => {
			let allPending = await Promise.all(iterators.map((x) => x.pending()));
			return allPending.reduce((acc, x) => acc + x, 0);
		},
		done: () => iterators.every((x) => x.done() === true),
		close: async () => {
			for (const iterator of iterators) {
				await iterator.close();
			}
		},
		all: async () => {
			let results: IndexedResult<
				ReturnTypeFromShape<ReplicationRangeIndexable, S>
			>[] = [];
			for (const iterator of iterators) {
				let res = await iterator.all();
				results.push(...res);
			}
			return results;
		},
	};
};

const getClosestAround = <
	S extends (Shape & { timestamp: true }) | undefined = undefined,
>(
	peers: Index<ReplicationRangeIndexable>,
	point: number,
	roleAge: number,
	now: number,
	includeStrictBelow: boolean,
	includeStrictAbove: boolean,
	options?: { shape?: S },
) => {
	const closestBelow = getClosest<S>(
		"below",
		peers,
		point,
		roleAge,
		true,
		now,
		includeStrictBelow,
		options,
	);
	const closestAbove = getClosest<S>(
		"above",
		peers,
		point,
		roleAge,
		true,
		now,
		includeStrictAbove,
		options,
	);
	const containing = containingPoint<S>(
		peers,
		point,
		roleAge,
		true,
		now,
		options,
	);

	return iteratorInSeries(
		containing,
		joinIterator<S>([closestBelow, closestAbove], point, "closest"),
	);
};

const collectNodesAroundPoint = async (
	roleAge: number,
	peers: Index<ReplicationRangeIndexable>,
	collector: (
		rect: { hash: string },
		matured: boolean,
		interescting: boolean,
	) => void,
	point: u32,
	now: number,
	done: () => boolean = () => true,
) => {
	/* let shape = { timestamp: true, hash: true } as const */
	const containing = containingPoint(
		peers,
		point,
		0,
		true,
		now /* , { shape } */,
	);
	const allContaining = await containing.all();
	for (const rect of allContaining) {
		collector(rect.value, isMatured(rect.value, now, roleAge), true);
	}

	if (done()) {
		return;
	}

	const closestBelow = getClosest(
		"below",
		peers,
		point,
		0,
		true,
		now,
		false /* , { shape } */,
	);
	const closestAbove = getClosest(
		"above",
		peers,
		point,
		0,
		true,
		now,
		false /* , { shape } */,
	);
	const aroundIterator = joinIterator(
		[closestBelow, closestAbove],
		point,
		"closest",
	);
	while (aroundIterator.done() !== true && done() !== true) {
		const res = await aroundIterator.next(1);
		for (const rect of res) {
			collector(rect.value, isMatured(rect.value, now, roleAge), false);
			if (done()) {
				return;
			}
		}
	}
};

export const getEvenlySpacedU32 = (from: number, count: number) => {
	let ret: number[] = new Array(count);
	for (let i = 0; i < count; i++) {
		ret[i] = Math.round(from + (i * MAX_U32) / count) % MAX_U32;
	}
	return ret;
};

export const isMatured = (
	segment: { timestamp: bigint },
	now: number,
	minAge: number,
) => {
	return now - Number(segment.timestamp) >= minAge;
};
// get peer sample that are responsible for the cursor point
// will return a list of peers that want to replicate the data,
// but also if necessary a list of peers that are responsible for the data
// but have not explicitly replicating a range that cover the cursor point
export const getSamples = async (
	cursor: u32[],
	peers: Index<ReplicationRangeIndexable>,
	roleAge: number,
): Promise<Map<string, { intersecting: boolean }>> => {
	const leaders: Map<string, { intersecting: boolean }> = new Map();
	if (!peers) {
		return new Map();
	}

	const now = +new Date();

	const maturedLeaders = new Set();
	for (let i = 0; i < cursor.length; i++) {
		// evenly distributed

		// aquire at least one unique node for each point
		await collectNodesAroundPoint(
			roleAge,
			peers,
			(rect, m, intersecting) => {
				if (m) {
					maturedLeaders.add(rect.hash);
				}

				const prev = leaders.get(rect.hash);

				if (!prev || (intersecting && !prev.intersecting)) {
					leaders.set(rect.hash, { intersecting });
				}
			},
			cursor[i],
			now,
			() => {
				if (maturedLeaders.size > i) {
					return true;
				}
				return false;
			},
		);
	}

	return leaders;
};

const fetchOne = async <S extends Shape | undefined>(
	iterator: IndexIterator<ReplicationRangeIndexable, S>,
) => {
	const value = await iterator.next(1);
	await iterator.close();
	return value[0]?.value;
};

export const minimumWidthToCover = async (
	minReplicas: number /* , replicatorCount: number */,
) => {
	/* minReplicas = Math.min(minReplicas, replicatorCount); */ // TODO do we need this?

	// If min replicas = 2
	// then we need to make sure we cover 0.5 of the total 'width' of the replication space
	// to make sure we reach sufficient amount of nodes such that at least one one has
	// the entry we are looking for

	let widthToCoverScaled = Math.round(MAX_U32 / minReplicas);
	return widthToCoverScaled;
};

export const getCoverSet = async (properties: {
	peers: Index<ReplicationRangeIndexable>;
	start: number | PublicSignKey | undefined;
	widthToCoverScaled: number;
	roleAge: number;
	intervalWidth?: number;
	eager?:
		| {
				unmaturedFetchCoverSize?: number;
		  }
		| boolean;
}): Promise<Set<string>> => {
	let intervalWidth: number = properties.intervalWidth ?? MAX_U32;
	const { peers, start, widthToCoverScaled, roleAge } = properties;

	const now = Date.now();
	const { startNode, startLocation, endLocation } = await getStartAndEnd(
		peers,
		start,
		widthToCoverScaled,
		roleAge,
		now,
		intervalWidth,
	);

	let ret = new Set<string>();

	// if start node (assume is self) and not mature, ask all known remotes if limited
	// TODO consider a more robust stragety here in a scenario where there are many nodes, lets say
	// a social media app with 1m user, then it does not makes sense to query "all" just because we started
	if (properties.eager) {
		const eagerFetch =
			properties.eager === true
				? 1000
				: (properties.eager.unmaturedFetchCoverSize ?? 1000);

		// pull all umatured
		const iterator = peers.iterate({
			query: [
				new IntegerCompare({
					key: "timestamp",
					compare: Compare.GreaterOrEqual,
					value: BigInt(now - roleAge),
				}),
			],
		});
		const rects = await iterator.next(eagerFetch);
		await iterator.close();
		for (const rect of rects) {
			ret.add(rect.value.hash);
		}
	}

	const endIsWrapped = endLocation <= startLocation;

	if (!startNode) {
		return ret;
	}

	let current = startNode;

	// push edges
	ret.add(current.hash);

	const resolveNextContaining = async (
		nextLocation: number,
		roleAge: number,
	) => {
		let next = await fetchOne(
			containingPoint(peers, nextLocation, roleAge, true, now, {
				sort: [new Sort({ key: "end2", direction: SortDirection.DESC })],
			}),
		); // get entersecting sort by largest end2
		return next;
	};

	const resolveNextAbove = async (nextLocation: number, roleAge: number) => {
		// if not get closest from above
		let next = await fetchOne(
			getClosest("above", peers, nextLocation, roleAge, true, now, true),
		);
		return next;
	};

	const resolveNext = async (
		nextLocation: number,
		roleAge: number,
	): Promise<[ReplicationRangeIndexable, boolean]> => {
		const containing = await resolveNextContaining(nextLocation, roleAge);
		if (containing) {
			return [containing, true];
		}
		return [await resolveNextAbove(nextLocation, roleAge), false];
	};

	// fill the middle
	let wrappedOnce = current.end2 < current.end1;

	let coveredLength = 0;
	const addLength = (from: number) => {
		if (current.end2 < from || current.wrapped) {
			wrappedOnce = true;
			coveredLength += MAX_U32 - from;
			coveredLength += current.end2;
		} else {
			coveredLength += current.end1 - from;
		}
	};
	addLength(startLocation);

	let maturedCoveredLength =
		coveredLength; /* TODO only increase matured length when startNode is matured? i.e. do isMatured(startNode, now, roleAge) ? coveredLength : 0; */
	let nextLocation = current.end2;

	while (
		maturedCoveredLength < widthToCoverScaled && // eslint-disable-line no-unmodified-loop-condition
		coveredLength <= MAX_U32 // eslint-disable-line no-unmodified-loop-condition
	) {
		let nextCandidate = await resolveNext(nextLocation, roleAge);
		/* let fromAbove = false; */
		let matured = true;

		if (!nextCandidate[0]) {
			matured = false;
			nextCandidate = await resolveNext(nextLocation, 0);
			/* fromAbove = true; */
		}

		if (!nextCandidate[0]) {
			break;
		}

		let nextIsCurrent = equals(nextCandidate[0].id, current.id);
		if (nextIsCurrent) {
			break;
		}
		let last = current;
		current = nextCandidate[0];

		let distanceBefore = coveredLength;

		addLength(nextLocation);

		let isLast =
			distanceBefore < widthToCoverScaled &&
			coveredLength >= widthToCoverScaled;

		if (
			!isLast ||
			nextCandidate[1] ||
			Math.min(
				getDistance(last.start1, endLocation, "closest"),
				getDistance(last.end2, endLocation, "closest"),
			) >
				Math.min(
					getDistance(current.start1, endLocation, "closest"),
					getDistance(current.end2, endLocation, "closest"),
				)
		) {
			ret.add(current.hash);
		}

		if (isLast && !nextCandidate[1] /*  || equals(endRect.id, current.id) */) {
			break;
		}

		if (matured) {
			maturedCoveredLength = coveredLength;
		}

		nextLocation = endIsWrapped
			? wrappedOnce
				? Math.min(current.end2, endLocation)
				: current.end2
			: Math.min(current.end2, endLocation);
	}

	start instanceof PublicSignKey && ret.add(start.hashcode());
	return ret;
};
/* export const getReplicationDiff = (changes: ReplicationChange) => {
	// reduce the change set to only regions that are changed for each peer
	// i.e. subtract removed regions from added regions, and vice versa
	const result = new Map<string, { range: ReplicationRangeIndexable, added: boolean }[]>();

	for (const addedChange of changes.added ?? []) {
		let prev = result.get(addedChange.hash) ?? [];
		for (const [_hash, ranges] of result.entries()) {
			for (const r of ranges) {

			}
		}
	}
}
 */

const matchRangeQuery = (range: ReplicationRangeIndexable) => {
	let ors = [];
	ors.push(
		new And([
			new IntegerCompare({
				key: "coordinate",
				compare: "gte",
				value: range.start1,
			}),
			new IntegerCompare({
				key: "coordinate",
				compare: "lt",
				value: range.end1,
			}),
		]),
	);

	ors.push(
		new And([
			new IntegerCompare({
				key: "coordinate",
				compare: "gte",
				value: range.start2,
			}),
			new IntegerCompare({
				key: "coordinate",
				compare: "lt",
				value: range.end2,
			}),
		]),
	);

	return new Or(ors);
};
export const toRebalance = (
	changes: ReplicationChanges,
	index: Index<EntryReplicated>,
): AsyncIterable<{ gid: string; entries: EntryReplicated[] }> => {
	const assignedRangesQuery = (changes: ReplicationChanges) => {
		let ors: Query[] = [];
		for (const change of changes) {
			const matchRange = matchRangeQuery(change.range);
			if (change.type === "updated") {
				// assuming a range is to be removed, is this entry still enoughly replicated
				const prevMatchRange = matchRangeQuery(change.prev);
				ors.push(prevMatchRange);
				ors.push(matchRange);
			} else {
				ors.push(matchRange);
			}
		}

		// entry is assigned to a range boundary, meaning it is due to be inspected
		ors.push(
			new BoolQuery({
				key: "assignedToRangeBoundary",
				value: true,
			}),
		);

		// entry is not sufficiently replicated, and we are to still keep it
		return new Or(ors);
	};
	return {
		[Symbol.asyncIterator]: async function* () {
			const iterator = index.iterate({
				query: assignedRangesQuery(changes),
			});

			while (iterator.done() !== true) {
				const entries = await iterator.next(1000); // TODO choose right batch sizes here for optimal memory usage / speed

				// TODO do we need this
				const grouped = await groupByGidSync(entries.map((x) => x.value));

				for (const [gid, entries] of grouped.entries()) {
					yield { gid, entries };
				}
			}
		},
	};
};

export const fetchOneFromPublicKey = async <
	S extends (Shape & { timestamp: true }) | undefined = undefined,
>(
	publicKey: PublicSignKey,
	index: Index<ReplicationRangeIndexable>,
	roleAge: number,
	now: number,
	options?: {
		shape: S;
	},
) => {
	let iterator = index.iterate<S>(
		{
			query: [new StringMatch({ key: "hash", value: publicKey.hashcode() })],
		},
		options,
	);
	let result = await iterator.next(1);
	await iterator.close();
	let node = result[0]?.value;
	if (node) {
		if (!isMatured(node, now, roleAge)) {
			const matured = await fetchOne(
				getClosestAround<S>(
					index,
					node.start1,
					roleAge,
					now,
					false,
					false,
					options,
				),
			);
			if (matured) {
				node = matured;
			}
		}
	}
	return node;
};

export const getStartAndEnd = async <
	S extends (Shape & { timestamp: true }) | undefined,
>(
	peers: Index<ReplicationRangeIndexable>,
	start: number | PublicSignKey | undefined | undefined,
	widthToCoverScaled: number,
	roleAge: number,
	now: number,
	intervalWidth: number,
	options?: { shape: S },
): Promise<{
	startNode: ReturnTypeFromShape<ReplicationRangeIndexable, S> | undefined;
	startLocation: number;
	endLocation: number;
}> => {
	// find a good starting point
	let startNode: ReturnTypeFromShape<ReplicationRangeIndexable, S> | undefined =
		undefined;
	let startLocation: number | undefined = undefined;

	const nodeFromPoint = async (point = scaleToU32(Math.random())) => {
		startLocation = point;
		startNode = await fetchOneClosest(
			peers,
			startLocation,
			roleAge,
			now,
			false,
			true,
			options,
		);
	};

	if (start instanceof PublicSignKey) {
		// start at our node (local first)
		startNode = await fetchOneFromPublicKey(
			start,
			peers,
			roleAge,
			now,
			options,
		);
		if (!startNode) {
			// fetch randomly
			await nodeFromPoint();
		} else {
			startLocation = startNode.start1;
		}
	} else if (typeof start === "number") {
		await nodeFromPoint(start);
	} else {
		await nodeFromPoint();
	}

	if (!startNode || startLocation == null) {
		return { startNode: undefined, startLocation: 0, endLocation: 0 };
	}

	let endLocation = startLocation + widthToCoverScaled;
	if (intervalWidth != null) {
		endLocation = endLocation % intervalWidth;
	}

	// if start location is after endLocation and startNode is strict then return undefined because this is not a node we want to choose
	let coveredDistanceToStart = 0;
	if (startNode.start1 < startLocation) {
		coveredDistanceToStart += intervalWidth - startLocation + startNode.start1;
	} else {
		coveredDistanceToStart += startNode.start1 - startLocation;
	}

	if (
		startNode.mode === ReplicationIntent.Strict &&
		coveredDistanceToStart > widthToCoverScaled
	) {
		return { startNode: undefined, startLocation: 0, endLocation: 0 };
	}

	return {
		startNode,
		startLocation: Math.round(startLocation),
		endLocation: Math.round(endLocation),
	};
};

export const fetchOneClosest = <
	S extends (Shape & { timestamp: true }) | undefined = undefined,
>(
	peers: Index<ReplicationRangeIndexable>,
	point: number,
	roleAge: number,
	now: number,
	includeStrictBelow: boolean,
	includeStrictAbove: boolean,
	options?: { shape?: S },
) => {
	return fetchOne(
		getClosestAround<S>(
			peers,
			point,
			roleAge,
			now,
			includeStrictBelow,
			includeStrictAbove,
			options,
		),
	);
};
