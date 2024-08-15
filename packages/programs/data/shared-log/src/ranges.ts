import { PublicSignKey, equals } from "@peerbit/crypto";
import {
	And,
	Compare,
	type Index,
	type IndexIterator,
	type IndexedResult,
	type IndexedResults,
	IntegerCompare,
	Or,
	type Query,
	SearchRequest,
	Sort,
	SortDirection,
	StringMatch,
	iterate,
	iteratorInSeries,
} from "@peerbit/indexer-interface";
import { type ReplicationRangeIndexable } from "./replication.js";
import { SEGMENT_COORDINATE_SCALE } from "./role.js";

const containingPoint = (
	rects: Index<ReplicationRangeIndexable>,
	point: number,
	roleAgeLimit: number,
	matured: boolean,
	now: number,
	options?: {
		sort?: Sort[];
		scaled?: boolean;
	},
): IndexIterator<ReplicationRangeIndexable> => {
	// point is between 0 and 1, and the range can start at any offset between 0 and 1 and have length between 0 and 1
	// so we need to query for all ranges that contain the point
	let pointScaled = Math.round(
		point * (options?.scaled ? 1 : SEGMENT_COORDINATE_SCALE),
	);
	let queries = [
		new Or([
			new And([
				new IntegerCompare({
					key: "start1",
					compare: Compare.LessOrEqual,
					value: pointScaled,
				}),
				new IntegerCompare({
					key: "end1",
					compare: Compare.Greater,
					value: pointScaled,
				}),
			]),
			new And([
				new IntegerCompare({
					key: "start2",
					compare: Compare.LessOrEqual,
					value: pointScaled,
				}),
				new IntegerCompare({
					key: "end2",
					compare: Compare.Greater,
					value: pointScaled,
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
	scaled: boolean = false,
): IndexIterator<ReplicationRangeIndexable> => {
	const scaledPoint = Math.round(
		point * (scaled ? 1 : SEGMENT_COORDINATE_SCALE),
	);
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
		return queries;
	};

	const iterator = iterate(
		rects,
		new SearchRequest({
			query: createQueries(scaledPoint, false),
			sort:
				direction === "below"
					? new Sort({ key: ["end2"], direction: "desc" })
					: new Sort({ key: ["start1"], direction: "asc" }),
		}),
	);
	const iteratorWrapped = iterate(
		rects,
		new SearchRequest({
			query: createQueries(
				direction === "below" ? SEGMENT_COORDINATE_SCALE : 0,
				true,
			),
			sort:
				direction === "below"
					? new Sort({ key: ["end2"], direction: "desc" })
					: new Sort({ key: ["start1"], direction: "asc" }),
		}),
	);

	return joinIterator(
		[iterator, iteratorWrapped],
		scaledPoint,
		true,
		direction,
	);
};

export const getDistance = (
	from: number,
	to: number,
	direction: "above" | "below" | "closest",
	end = SEGMENT_COORDINATE_SCALE,
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
	scaled: boolean,
	direction: "above" | "below" | "closest",
) => {
	const scaledPoint = Math.round(
		point * (scaled ? 1 : SEGMENT_COORDINATE_SCALE),
	);
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
								getDistance(closest.start1, scaledPoint, direction),
								getDistance(closest.end2, scaledPoint, direction),
							);
						} else if (direction === "above") {
							dist = getDistance(closest.start1, scaledPoint, direction);
						} else if (direction === "below") {
							dist = getDistance(closest.end2, scaledPoint, direction);
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
	scaled: boolean,
) => {
	const closestBelow = getClosest(
		"below",
		peers,
		point,
		roleAge,
		true,
		now,
		scaled,
	);
	const closestAbove = getClosest(
		"above",
		peers,
		point,
		roleAge,
		true,
		now,
		scaled,
	);
	const containing = containingPoint(peers, point, roleAge, true, now, {
		scaled: scaled,
	});

	return iteratorInSeries(
		containing,
		joinIterator([closestBelow, closestAbove], point, scaled, "closest"),
	);
};

const collectNodesAroundPoint = async (
	roleAge: number,
	peers: Index<ReplicationRangeIndexable>,
	collector: (rect: ReplicationRangeIndexable, matured: boolean) => void,
	point: number,
	now: number,
	done: () => boolean = () => true,
) => {
	const containing = containingPoint(peers, point, 0, true, now, {
		scaled: false,
	});

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
		false,
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
	cursor: number,
	peers: Index<ReplicationRangeIndexable>,
	amount: number,
	roleAge: number,
) => {
	const leaders: Set<string> = new Set();
	const width = 1;
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
		const point = ((cursor + i / amount) % 1) * width;

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

export const getCoverSet = async (
	peers: Index<ReplicationRangeIndexable>,
	roleAge: number,
	start: number | PublicSignKey | undefined,
	widthToCoverScaled: number,
	intervalWidth: number | undefined,
): Promise<Set<string>> => {
	const { startNode, startLocation, endLocation } = await getStartAndEndNode(
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

	const endRect =
		(await fetchOne(
			getClosestAround(peers, endLocation, roleAge, now, true),
		)) || (await fetchOne(getClosestAround(peers, endLocation, 0, now, true))); // (await getClosest('above', peers, nextLocation, roleAge, true, 1, true))[0]

	if (!endRect) {
		return new Set();
	}

	let current =
		/* (await getClosestAround(peers, startLocation, roleAge, 1, true))[0] */ startNode ||
		(await fetchOne(getClosestAround(peers, startLocation, 0, now, true))); //(await getClosest('above', peers, startLocation, roleAge, true, 1, true))[0]
	let coveredLength = current.width;
	let nextLocation = current.end2;

	// push edges
	results.push(endRect);
	results.push(current);
	/* const endIsSameAsStart = equals(endRect.id, current.id); */

	const resolveNextContaining = async (
		nextLocation: number,
		roleAge: number,
	) => {
		let next = await fetchOne(
			containingPoint(peers, nextLocation, roleAge, true, now, {
				scaled: true,
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

	let maturedCoveredLength = coveredLength;
	/* 	let lastMatured = isMatured(startNode, now, roleAge) ? startNode : undefined;
	 */

	while (
		maturedCoveredLength < widthToCoverScaled &&
		coveredLength <= SEGMENT_COORDINATE_SCALE
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

		current = nextCandidate[0];

		let distanceBefore = coveredLength;

		if (current.end2 < nextLocation) {
			wrappedOnce = true;
			coveredLength += SEGMENT_COORDINATE_SCALE - nextLocation;
			coveredLength += current.end2;
		} else {
			coveredLength += current.end1 - nextLocation;
		}

		let isLast =
			distanceBefore < widthToCoverScaled &&
			coveredLength >= widthToCoverScaled;
		if ((isLast && !nextCandidate[1]) || equals(endRect.id, current.id)) {
			break;
		}

		if (matured) {
			maturedCoveredLength = coveredLength;
		}

		results.push(current);

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
				getClosestAround(index, node.start1, roleAge, now, true),
			);
			if (matured) {
				node = matured;
			}
		}
	}
	return node;
};

export const getStartAndEndNode = async (
	peers: Index<ReplicationRangeIndexable>,
	start: number | PublicSignKey | undefined | undefined,
	widthToCoverScaled: number,
	roleAge: number,
	now: number,
	intervalWidth: number | undefined,
) => {
	// find a good starting point
	let startNode: ReplicationRangeIndexable | undefined = undefined;
	let startLocation: number;

	if (start instanceof PublicSignKey) {
		// start at our node (local first)
		startNode = await fetchOneFromPublicKey(start, peers, roleAge, now);
	} else if (typeof start === "number") {
		startLocation = start;
		startNode = await fetchOneClosest(peers, startLocation, roleAge, now);
	}

	if (!startNode) {
		startLocation = Math.random() * SEGMENT_COORDINATE_SCALE;
		startNode = await fetchOneClosest(peers, startLocation, roleAge, now);
	} else {
		// TODO choose start location as the point with the longest range?
		startLocation =
			startNode.start1 ?? Math.random() * SEGMENT_COORDINATE_SCALE;
	}

	if (!startNode) {
		return { startNode: undefined, startLocation: 0, endLocation: 0 };
	}

	let endLocation = startLocation + widthToCoverScaled;
	if (intervalWidth != null) {
		endLocation = endLocation % intervalWidth;
	}

	return { startNode, startLocation, endLocation };
};

export const fetchOneClosest = async (
	peers: Index<ReplicationRangeIndexable>,
	point: number,
	roleAge: number,
	now: number,
) => {
	return await fetchOne(getClosestAround(peers, point, roleAge, now, true));
};
