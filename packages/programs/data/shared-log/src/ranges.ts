import { type PublicSignKey, equals } from "@peerbit/crypto";
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
import {
	/* getSegmentsFromOffsetAndRange, */
	type ReplicationRangeIndexable,
} from "./replication.js";
import { SEGMENT_COORDINATE_SCALE } from "./role.js";

/* 
export const containsPoint = (
	rect: { offset: number; length: number },
	point: number,
	eps = 0.00001 // we do this to handle numerical errors
) => {
	if (rect.factor === 0) {
		return false;
	}
	const start = rect.offset;
	const width = rect.factor + eps; // we do this to handle numerical errors. It is better to be more inclusive
	const endUnwrapped = rect.offset + width;
	let end = endUnwrapped;
	let wrapped = false;
	if (endUnwrapped > 1) {
		end = endUnwrapped % 1;
		wrapped = true;
	}

	const inFirstInterval = point >= start && point < Math.min(endUnwrapped, 1);
	const inSecondInterval =
		!inFirstInterval && wrapped && point >= 0 && point < end;

	return inFirstInterval || inSecondInterval;
}; */

/* const resolveRectsThatContainPoint = async (
	rects: Index<ReplicationRangeIndexable>,
	point: number,
	roleAgeLimit: number,
	matured: boolean
): Promise<ReplicationRangeIndexable[]> => {
	// point is between 0 and 1, and the range can start at any offset between 0 and 1 and have length between 0 and 1
	// so we need to query for all ranges that contain the point
	const scaledPoint = Math.round(point * SEGMENT_COORDINATE_SCALE)
	let queries = [
		new IntegerCompare({ key: 'start', compare: Compare.LessOrEqual, value: scaledPoint }),
		new IntegerCompare({ key: 'end', compare: Compare.Greater, value: scaledPoint }),
		new IntegerCompare({ key: 'timestamp', compare: matured ? Compare.LessOrEqual : Compare.Greater, value: Date.now() - roleAgeLimit })
	]

	const results = await rects.query(new SearchRequest({
		query: [
			new Nested({
				path: 'segments',
				query: queries
			})
		]
	}))
	return results.results.map(x => x.value)
} */

/* const resolveRectsInRange = async (rects: Index<ReplicationRangeIndexable>,
	start: number,
	end: number,
	roleAgeLimit: number,
	matured: boolean
): Promise<ReplicationRangeIndexable[]> => {
	// point is between 0 and 1, and the range can start at any offset between 0 and 1 and have length between 0 and 1
	// so we need to query for all ranges that contain the point
	let endScaled = Math.round(end * SEGMENT_COORDINATE_SCALE);
	let startScaled = Math.round(start * SEGMENT_COORDINATE_SCALE);
	let queries = [
		new Or([
			new And([
				new IntegerCompare({ key: 'start1', compare: Compare.Less, value: endScaled }),
				new IntegerCompare({ key: 'end1', compare: Compare.GreaterOrEqual, value: startScaled }),
			]),
			new And([
				new IntegerCompare({ key: 'start2', compare: Compare.Less, value: endScaled }),
				new IntegerCompare({ key: 'end2', compare: Compare.GreaterOrEqual, value: startScaled }),
			])
		]),
		new IntegerCompare({ key: 'timestamp', compare: matured ? Compare.LessOrEqual : Compare.Greater, value: BigInt(+new Date - roleAgeLimit) })
	]

	const results = await rects.query(new SearchRequest({
		query: queries,
		sort: [new Sort({ key: "start1" }), new Sort({ key: "start2" })],
		fetch: 0xffffffff
	}))
	return results.results.map(x => x.value)
} */

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
				// console.log(m, rect.start1 / SEGMENT_COORDINATE_SCALE, rect.width / SEGMENT_COORDINATE_SCALE)
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
	coveringWidth: number,
	peers: Index<ReplicationRangeIndexable>,
	roleAge: number,
	startNodeIdentity?: PublicSignKey,
): Promise<Set<string>> => {
	let now = +new Date();

	// find a good starting point
	let startNode: ReplicationRangeIndexable | undefined = undefined;
	if (startNodeIdentity) {
		// start at our node (local first)
		let result = await peers.query(
			new SearchRequest({
				query: [
					new StringMatch({ key: "hash", value: startNodeIdentity.hashcode() }),
				],
				fetch: 1,
			}),
		);
		startNode = result.results[0]?.value;

		if (startNode) {
			if (!isMatured(startNode, now, roleAge)) {
				const matured = await fetchOne(
					getClosestAround(peers, startNode.start1, roleAge, now, true),
				);
				if (matured) {
					startNode = matured;
				}
			}
		}
	}
	let startLocation: number;

	if (!startNode) {
		startLocation = Math.random() * SEGMENT_COORDINATE_SCALE;
		startNode = await fetchOne(
			getClosestAround(peers, startLocation, roleAge, now, true),
		);
	} else {
		// TODO choose start location as the point with the longest range?
		startLocation =
			startNode.start1 ?? Math.random() * SEGMENT_COORDINATE_SCALE;
	}

	/* const startNode = walker; */
	if (!startNode) {
		return new Set();
	}

	let results: ReplicationRangeIndexable[] = [];

	let widthToCoverScaled = coveringWidth * SEGMENT_COORDINATE_SCALE;
	const endLocation =
		(startLocation + widthToCoverScaled) % SEGMENT_COORDINATE_SCALE;
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
		if (
			(isLast &&
				!nextCandidate[1]) /* || Math.min(current.start1, current.start2) > Math.min(endRect.start1, endRect.start2) */ /* (Math.min(current.start1, current.start2) > endLocation) */ ||
			equals(endRect.id, current.id)
		) {
			/* if ((isLast && ((endIsWrapped && wrappedOnce) || (!endIsWrapped))) && (current.start1 > endLocation || equals(current.id, endRect.id))) { */
			// this is the end!
			/* if (lastMatured && lastMatured.distanceTo(endLocation) < current.distanceTo(endLocation)) {
				breaks;
			} */
			break;
		}

		/* if (fromAbove && next && next.start1 > endRect.start1 && (endIsWrapped === false || wrappedOnce)) {
			break;
		} */

		// this is a skip condition to not include too many rects

		if (matured) {
			maturedCoveredLength = coveredLength;
			/* lastMatured = current; */
		}

		results.push(current);
		/* 
		
				if (current.start1 > endLocation && (wrappedOnce || !endIsWrapped)) {
					break;
				}
		
		 */
		nextLocation = endIsWrapped
			? wrappedOnce
				? Math.min(current.end2, endLocation)
				: current.end2
			: Math.min(current.end2, endLocation);
	}

	const res = new Set(results.map((x) => x.hash));
	startNodeIdentity && res.add(startNodeIdentity.hashcode());
	return res;

	//
	/* const set: Set<string> = new Set();
	let currentNode = startNode;
	const t = +new Date();
	
	let wrappedOnce = false;
	const startPoint = startNode.segment.offset;
	
	const getNextPoint = (): [number, number, number, boolean] => {
		let nextPoint =
			currentNode.segment.offset + currentNode.segment.factor;
	
		if (nextPoint > 1 || nextPoint < startPoint) {
			wrappedOnce = true;
		}
	
		nextPoint = nextPoint % 1;
		let distanceStart: number;
	
		if (wrappedOnce) {
			distanceStart = (1 - startPoint + currentNode.segment.offset) % 1;
		} else {
			distanceStart = (currentNode.segment.offset - startPoint) % 1;
		}
	
		const distanceEnd = distanceStart + currentNode.segment.factor;
	
		return [nextPoint, distanceStart, distanceEnd, wrappedOnce];
	};
	
	const getNextMatured = async (from: ReplicatorRect) => {
		let next = (await peers.query(new SearchRequest({ query: [new IntegerCompare({ key: ['segment', 'offset'], compare: Compare.Greater, value: from.segment.offset })], fetch: 1 })))?.results[0]?.value  // (from.next || peers.head)!;
		while (
			next.hash !== from.hash &&
			next.hash !== startNode.hash
		) {
			if (isMatured(next.segment, t, roleAge)) {
				return next;
			}
			next = (next.next || peers.head)!;
		}
		return undefined;
	}; */

	/**
	 * The purpose of this loop is to cover at least coveringWidth
	 * so that if we query all nodes in this range, we know we will
	 * "query" all data in that range
	 */

	/* let isPastThePoint = false;
	outer: while (currentNode) {
		if (set.has(currentNode.hash)) break;
	
		const [nextPoint, distanceStart, distanceEnd, wrapped] = getNextPoint();
	
		if (distanceStart <= coveringWidth) {
			set.add(currentNode.hash);
		}
	
		if (distanceEnd >= coveringWidth) {
			break;
		}
	
		let next = currentNode.next || peers.head;
		while (next) {
			if (next.value.publicKey.equals(startNode.value.publicKey)) {
				break outer;
			}
	
			const prevOffset = (next.prev || peers.tail)!.value.role.offset;
			const nextOffset = next.value.role.offset;
			const nextHasWrapped = nextOffset < prevOffset;
	
			if (
				(!wrapped && nextOffset > nextPoint) ||
				(nextHasWrapped &&
					(wrapped ? nextOffset > nextPoint : prevOffset < nextPoint)) ||
				(!nextHasWrapped && prevOffset < nextPoint && nextPoint <= nextOffset)
			) {
				isPastThePoint = true;
			}
	
			if (isPastThePoint) {
				break; // include this next in the set;
			}
	
			const overlapsRange = containsPoint(next.value.role, nextPoint);
	
			if (overlapsRange) {
				// Find out if there is a better choice ahead of us
				const nextNext = await getNextMatured(next);
				if (
					nextNext &&
					nextNext.hash === currentNode.hash &&
					nextNext.segment.offset < nextPoint &&
					nextNext.segment.offset + nextNext.segment.factor > nextPoint
				) {
					// nextNext is better (continue to iterate)
				} else {
					// done
					break;
				}
			} else {
				// (continue to iterate)
			}
	
			next = next.next || peers.head;
		}
		currentNode = next!;
	} */

	// collect 1 point around the boundary of the start and one at the end,
	// preferrd matured and that we already have it
	/* for (const point of [
		startNode.segment.offset,
		(startNode.segment.offset + coveringWidth) % 1
	]) {
		let done = false;
		const unmatured: string[] = [];
		collectNodesAroundPoint(
			t,
			roleAge,
			peers,
			isMatured(startNode.segment, t, roleAge) ? startNode : peers.head, // start at startNode is matured, else start at head (we only seek to find one matured node at the point)
			(rect, matured) => {
				if (matured) {
					if (set.has(rect.hash)) {
						// great!
					} else {
						set.add(rect.hash);
					}
					done = true;
				} else {
					unmatured.push(rect.hash);
				}
			},
			point,
			() => done
		);
		if (!done && unmatured.length > 0) {
			set.add(unmatured[0]);
			// TODO add more elements?
		}
	}
	return set; */
};
