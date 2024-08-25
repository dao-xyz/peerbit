import { PublicSignKey, equals } from "@peerbit/crypto";
import {
	And,
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
	SearchRequest,
	Sort,
	SortDirection,
	StringMatch,
	iterate,
	iteratorInSeries,
} from "@peerbit/indexer-interface";
import type { u32 } from "./replication-domain.js";
import {
	ReplicationIntent,
	type ReplicationRangeIndexable,
} from "./replication.js";
import { MAX_U32, scaleToU32 } from "./role.js";

const containingPoint = (
	rects: Index<ReplicationRangeIndexable>,
	point: number,
	roleAgeLimit: number,
	matured: boolean,
	now: number,
	options?: {
		sort?: Sort[];
	},
): IndexIterator<ReplicationRangeIndexable> => {
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
	return iterate(
		rects,
		new SearchRequest({
			query: queries,
			sort: options?.sort,
			fetch: 0xffffffff,
		}),
	);
	/* const results = await rects.query(new SearchRequest({
		query: queries,
		sort: options?.sort,
		fetch: 0xffffffff
	}))
	return results.results.map(x => x.value) */
};

const getClosest = (
	direction: "above" | "below",
	rects: Index<ReplicationRangeIndexable>,
	point: number,
	roleAgeLimit: number,
	matured: boolean,
	now: number,
	includeStrict: boolean,
): IndexIterator<ReplicationRangeIndexable> => {
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

	const iterator = iterate(
		rects,
		new SearchRequest({
			query: createQueries(point, false),
			sort:
				direction === "below"
					? new Sort({ key: ["end2"], direction: "desc" })
					: new Sort({ key: ["start1"], direction: "asc" }),
		}),
	);
	const iteratorWrapped = iterate(
		rects,
		new SearchRequest({
			query: createQueries(direction === "below" ? MAX_U32 : 0, true),
			sort:
				direction === "below"
					? new Sort({ key: ["end2"], direction: "desc" })
					: new Sort({ key: ["start1"], direction: "asc" }),
		}),
	);

	return joinIterator([iterator, iteratorWrapped], point, direction);
};

export const hasCoveringRange = async (
	rects: Index<ReplicationRangeIndexable>,
	range: ReplicationRangeIndexable,
) => {
	return (
		(await rects.count(
			new SearchRequest({
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
			}),
		)) > 0
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

const joinIterator = (
	iterators: IndexIterator<ReplicationRangeIndexable>[],
	point: number,
	direction: "above" | "below" | "closest",
) => {
	let queues: {
		kept: number;
		elements: {
			result: IndexedResult<ReplicationRangeIndexable>;
			dist: number;
		}[];
	}[] = [];

	return {
		next: async (
			count: number,
		): Promise<IndexedResults<ReplicationRangeIndexable>> => {
			let results: IndexedResults<ReplicationRangeIndexable> = {
				kept: 0, // TODO
				results: [],
			};
			for (let i = 0; i < iterators.length; i++) {
				let queue = queues[i];
				if (!queue) {
					queue = { elements: [], kept: 0 };
					queues[i] = queue;
				}
				let iterator = iterators[i];
				if (queue.elements.length < count && iterator.done() === false) {
					let res = await iterator.next(count);
					queue.kept = res.kept;

					for (const el of res.results) {
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
					results.results.push(closest.result);
				}
			}

			for (let i = 0; i < queues.length; i++) {
				results.kept += queues[i].elements.length + queues[i].kept;
			}

			return results;
		},
		done: () => iterators.every((x) => x.done()),
		close: async () => {
			for (const iterator of iterators) {
				await iterator.close();
			}
		},
		all: async () => {
			let results: IndexedResult<ReplicationRangeIndexable>[] = [];
			for (const iterator of iterators) {
				let res = await iterator.all();
				results.push(...res);
			}
			return results;
		},
	};
};

const getClosestAround = (
	peers: Index<ReplicationRangeIndexable>,
	point: number,
	roleAge: number,
	now: number,
	includeStrictBelow: boolean,
	includeStrictAbove: boolean,
) => {
	const closestBelow = getClosest(
		"below",
		peers,
		point,
		roleAge,
		true,
		now,
		includeStrictBelow,
	);
	const closestAbove = getClosest(
		"above",
		peers,
		point,
		roleAge,
		true,
		now,
		includeStrictAbove,
	);
	const containing = containingPoint(peers, point, roleAge, true, now);

	return iteratorInSeries(
		containing,
		joinIterator([closestBelow, closestAbove], point, "closest"),
	);
};

const collectNodesAroundPoint = async (
	roleAge: number,
	peers: Index<ReplicationRangeIndexable>,
	collector: (rect: ReplicationRangeIndexable, matured: boolean) => void,
	point: u32,
	now: number,
	done: () => boolean = () => true,
) => {
	const containing = containingPoint(peers, point, 0, true, now);

	const allContaining = await containing.next(0xffffffff);
	for (const rect of allContaining.results) {
		collector(rect.value, isMatured(rect.value, now, roleAge));
	}

	if (done()) {
		return;
	}

	const closestBelow = getClosest("below", peers, point, 0, true, now, false);
	const closestAbove = getClosest("above", peers, point, 0, true, now, false);
	const aroundIterator = joinIterator(
		[closestBelow, closestAbove],
		point,
		"closest",
	);
	while (aroundIterator.done() === false && done() === false) {
		const res = await aroundIterator.next(1);
		for (const rect of res.results) {
			collector(rect.value, isMatured(rect.value, now, roleAge));
			if (done()) {
				return;
			}
		}
	}
};

export const isMatured = (
	segment: { timestamp: bigint },
	now: number,
	minAge: number,
) => {
	return now - Number(segment.timestamp) >= minAge;
};

export const getSamples = async (
	cursor: u32,
	peers: Index<ReplicationRangeIndexable>,
	amount: number,
	roleAge: number,
) => {
	const leaders: Set<string> = new Set();
	if (!peers) {
		return [];
	}

	const size = await peers.getSize();

	amount = Math.min(amount, size);

	if (amount === 0) {
		return [];
	}

	const now = +new Date();

	const maturedLeaders = new Set();
	for (let i = 0; i < amount; i++) {
		// evenly distributed
		const point = Math.round(cursor + (i * MAX_U32) / amount) % MAX_U32;

		// aquire at least one unique node for each point
		await collectNodesAroundPoint(
			roleAge,
			peers,
			(rect, m) => {
				if (m) {
					maturedLeaders.add(rect.hash);
				}
				leaders.add(rect.hash);
			},
			point,
			now,
			() => {
				if (maturedLeaders.size > i) {
					return true;
				}
				return false;
			},
		);
	}

	return [...leaders];
};

const fetchOne = async (iterator: IndexIterator<ReplicationRangeIndexable>) => {
	const value = await iterator.next(1);
	await iterator.close();
	return value.results[0]?.value;
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

export const getCoverSet = async (
	peers: Index<ReplicationRangeIndexable>,
	roleAge: number,
	start: number | PublicSignKey | undefined,
	widthToCoverScaled: number,
	intervalWidth: number = MAX_U32,
): Promise<Set<string>> => {
	const { startNode, startLocation, endLocation } = await getStartAndEnd(
		peers,
		start,
		widthToCoverScaled,
		roleAge,
		Date.now(),
		intervalWidth,
	);

	let results: ReplicationRangeIndexable[] = [];

	let now = +new Date();

	const endIsWrapped = endLocation <= startLocation;

	if (!startNode) {
		return new Set();
	}

	let current = startNode;

	// push edges
	results.push(current);

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

	let maturedCoveredLength = coveredLength;
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
			results.push(current);
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

	const res = new Set(results.map((x) => x.hash));

	start instanceof PublicSignKey && res.add(start.hashcode());
	return res;
};

export const fetchOneFromPublicKey = async (
	publicKey: PublicSignKey,
	index: Index<ReplicationRangeIndexable>,
	roleAge: number,
	now: number,
) => {
	let result = await index.query(
		new SearchRequest({
			query: [new StringMatch({ key: "hash", value: publicKey.hashcode() })],
			fetch: 1,
		}),
	);
	let node = result.results[0]?.value;
	if (node) {
		if (!isMatured(node, now, roleAge)) {
			const matured = await fetchOne(
				getClosestAround(index, node.start1, roleAge, now, false, false),
			);
			if (matured) {
				node = matured;
			}
		}
	}
	return node;
};

export const getStartAndEnd = async (
	peers: Index<ReplicationRangeIndexable>,
	start: number | PublicSignKey | undefined | undefined,
	widthToCoverScaled: number,
	roleAge: number,
	now: number,
	intervalWidth: number,
) => {
	// find a good starting point
	let startNode: ReplicationRangeIndexable | undefined = undefined;
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
		);
	};

	if (start instanceof PublicSignKey) {
		// start at our node (local first)
		startNode = await fetchOneFromPublicKey(start, peers, roleAge, now);
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

export const fetchOneClosest = (
	peers: Index<ReplicationRangeIndexable>,
	point: number,
	roleAge: number,
	now: number,
	includeStrictBelow: boolean,
	includeStrictAbove: boolean,
) => {
	return fetchOne(
		getClosestAround(
			peers,
			point,
			roleAge,
			now,
			includeStrictBelow,
			includeStrictAbove,
		),
	);
};
