import yallist from "yallist";
import { type ReplicatorRect } from "./replication.js";
import { ReplicationSegment, Replicator } from "./role.js";
import { PublicSignKey } from "@peerbit/crypto";
import { Compare, IntegerCompare, SearchRequest, StringMatch, type Index } from "@peerbit/indexer-interface";

export const containsPoint = (
	rect: { offset: number; factor: number },
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
};

const collectNodesAroundPoint = (
	time: number,
	roleAge: number,
	peers: Index<ReplicatorRect>,
	currentNode: yallist.Node<ReplicatorRect> | null,
	collector: (rect: ReplicatorRect, matured: boolean) => void,
	point: number,
	done: (postProcess: boolean) => boolean = () => true
) => {
	/* 	let uniqueMatured = 0;
	 */ const maybeIncrementMatured = (rect: ReplicatorRect) => {
		const isMature = isMatured(rect.segment, time, roleAge);
		collector(rect, isMature);
	};

	// Assume peers does not mutate during this loop
	const startNode = currentNode;
	const diffs: { diff: number; rect: ReplicatorRect }[] = [];
	while (currentNode) {
		if (containsPoint(currentNode.value.role, point)) {
			maybeIncrementMatured(currentNode.value);
			if (done(false)) {
				return;
			}
		} /* if (matured === 0) */ else {
			const start = currentNode.value.role.offset;
			const end =
				(currentNode.value.role.offset + currentNode.value.role.factor) % 1;
			const absDelta = Math.min(Math.abs(start - point), Math.abs(end - point));
			const diff = Math.min(absDelta, 1 - absDelta);
			diffs.push({
				diff:
					currentNode.value.role.factor > 0
						? diff / currentNode.value.role.factor
						: Number.MAX_SAFE_INTEGER,
				rect: currentNode.value
			});
		}

		currentNode = currentNode.next || peers.head;

		if (
			currentNode?.value.publicKey &&
			startNode?.value.publicKey.equals(currentNode?.value.publicKey)
		) {
			break; // TODO throw error for failing to fetch ffull width
		}
	}

	if (done(true) == false) {
		diffs.sort((x, y) => x.diff - y.diff);
		for (const node of diffs) {
			maybeIncrementMatured(node.rect);
			if (done(true)) {
				break;
			}
		}
	}
};

export const isMatured = (segment: ReplicationSegment, now: number, minAge: number) => {
	return now - Number(segment.timestamp) >= minAge;
};

export const getSamples = async (
	cursor: number,
	peers: Index<ReplicatorRect>,
	amount: number,
	roleAge: number
) => {
	const leaders: Set<string> = new Set();
	const width = 1;
	if (!peers) {
		return [];
	}

	const size = await peers.getSize()

	amount = Math.min(amount, size);

	if (amount === 0) {
		return []
	}

	const t = +new Date();


	const maturedLeaders = new Set();
	for (let i = 0; i < amount; i++) {
		// evenly distributed
		const point = ((cursor + i / amount) % 1) * width;
		const currentNode = peers.head;

		// aquire at least one unique node for each point
		// but if previous point yielded more than one node
		collectNodesAroundPoint(
			t,
			roleAge,
			peers,
			currentNode,
			(rect, m) => {
				if (m) {
					maturedLeaders.add(rect.hash);
				}
				leaders.add(rect.hash);
			},
			point,
			(postProcess) => {
				if (postProcess) {
					if (maturedLeaders.size > i) {
						return true;
					}
				}
				return false; // collect all intersecting points
			}
		);
	}

	return [...leaders];
};

/* export const getCover = (
	coveringWidth: number,
	peers: Index<ReplicatorRect>,
	roleAge: number,
	startNodeIdentity?: PublicSignKey
): string[] => {
	return [...getCoverSet(coveringWidth, peers, roleAge, startNodeIdentity)];
}; */

export const getCoverSet = async (
	coveringWidth: number,
	peers: Index<ReplicatorRect>,
	roleAge: number,
	startNodeIdentity?: PublicSignKey
): Promise<Set<string>> => {
	// find a good starting point
	let startNode: ReplicatorRect | undefined = undefined;
	if (startNodeIdentity) {
		// start at our node (local first)
		let result = await peers.query(new SearchRequest({ query: [new StringMatch({ key: 'hash', value: startNodeIdentity.hashcode() })], fetch: 1 }), startNodeIdentity)
		/* while (walker) {
			if (walker.value.publicKey.equals(startNodeIdentity)) {
				break;
			}
			walker = walker.next;
		}
		if (!walker) {
			walker = peers.head;
		} */
		startNode = result.results[0]?.value
	}

	if (!startNode) {
		const seed = Math.round((await peers.getSize()) * Math.random()); // start at a random point
		/* for (let i = 0; i < seed - 1; i++) {
			if (walker?.next == null) {
				break;
			}
			walker = walker.next;
		} */
		let result = await peers.query(new SearchRequest({ query: [new StringMatch({ key: 'hash', value: startNodeIdentity.hashcode() })], fetch: 1 }), startNodeIdentity)
		startNode = result.results[0]?.value
	}

	/* const startNode = walker; */
	if (!startNode) {
		return new Set();
	}

	//
	const set: Set<string> = new Set();
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
			!next.value.publicKey.equals(from.value.hash) &&
			!next.value.publicKey.equals(startNode.hash)
		) {
			if (isMatured(next.value.role, t, roleAge)) {
				return next;
			}
			next = (next.next || peers.head)!;
		}
		return undefined;
	};

	/**
	 * The purpose of this loop is to cover at least coveringWidth
	 * so that if we query all nodes in this range, we know we will
	 * "query" all data in that range
	 */

	let isPastThePoint = false;
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
					!nextNext.value.publicKey.equals(currentNode.value.publicKey) &&
					nextNext.value.role.offset < nextPoint &&
					nextNext.value.role.offset + nextNext.value.role.factor > nextPoint
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
	}

	// collect 1 point around the boundary of the start and one at the end,
	// preferrd matured and that we already have it
	for (const point of [
		startNode.segment.offset,
		(startNode.segment.offset + coveringWidth) % 1
	]) {
		let done = false;
		const unmatured: string[] = [];
		collectNodesAroundPoint(
			t,
			roleAge,
			peers,
			isMatured(startNode.segment.role, t, roleAge) ? startNode : peers.head, // start at startNode is matured, else start at head (we only seek to find one matured node at the point)
			(rect, matured) => {
				if (matured) {
					if (set.has(rect.publicKey.hashcode())) {
						// great!
					} else {
						set.add(rect.publicKey.hashcode());
					}
					done = true;
				} else {
					unmatured.push(rect.publicKey.hashcode());
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
	return set;
};
