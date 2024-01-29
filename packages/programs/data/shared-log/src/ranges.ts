import yallist from "yallist";
import { ReplicatorRect } from "./replication.js";
import { Replicator } from "./role.js";
import { PublicSignKey } from "@peerbit/crypto";

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
	peers: yallist<ReplicatorRect>,
	currentNode: yallist.Node<ReplicatorRect> | null,
	collector: (rect: ReplicatorRect, matured: boolean) => void,
	point: number,
	done: (postProcess: boolean) => boolean = () => true
) => {
	/* 	let uniqueMatured = 0;
	 */ const maybeIncrementMatured = (rect: ReplicatorRect) => {
		const isMature = isMatured(rect.role, time, roleAge);
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

const isMatured = (role: Replicator, now: number, minAge: number) => {
	return now - Number(role.timestamp) >= minAge;
};

export const getSamples = (
	cursor: number,
	peers: yallist<ReplicatorRect>,
	amount: number,
	roleAge: number,
	dbg?: string
) => {
	const leaders: Set<string> = new Set();
	const width = 1;
	if (!peers || peers?.length === 0) {
		return [];
	}
	amount = Math.min(amount, peers.length);

	const t = +new Date();

	const matured = 0;

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
					maturedLeaders.add(rect.publicKey.hashcode());
				}
				leaders.add(rect.publicKey.hashcode());
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

export const getCover = (
	coveringWidth: number,
	peers: yallist<ReplicatorRect>,
	roleAge: number,
	startNodeIdentity?: PublicSignKey
) => {
	// find a good starting point
	let walker = peers.head;
	if (startNodeIdentity) {
		// start at our node (local first)
		while (walker) {
			if (walker.value.publicKey.equals(startNodeIdentity)) {
				break;
			}
			walker = walker.next;
		}
		if (!walker) {
			walker = peers.head;
		}
	} else {
		const seed = Math.round(peers.length * Math.random()); // start at a random point
		for (let i = 0; i < seed - 1; i++) {
			if (walker?.next == null) {
				break;
			}
			walker = walker.next;
		}
	}

	const startNode = walker;
	if (!startNode) {
		return [];
	}

	//
	const set: Set<string> = new Set();
	let currentNode = startNode;
	const t = +new Date();

	let wrappedOnce = false;
	const startPoint = startNode.value.role.offset;

	const getNextPoint = (): [number, number, number, boolean] => {
		let nextPoint =
			currentNode.value.role.offset + currentNode.value.role.factor;

		if (nextPoint > 1 || nextPoint < startPoint) {
			wrappedOnce = true;
		}

		nextPoint = nextPoint % 1;
		let distanceStart: number;

		if (wrappedOnce) {
			distanceStart = (1 - startPoint + currentNode.value.role.offset) % 1;
		} else {
			distanceStart = (currentNode.value.role.offset - startPoint) % 1;
		}

		const distanceEnd = distanceStart + currentNode.value.role.factor;

		return [nextPoint, distanceStart, distanceEnd, wrappedOnce];
	};

	const getNextMatured = (from: yallist.Node<ReplicatorRect>) => {
		let next = (from.next || peers.head)!;
		while (
			!next.value.publicKey.equals(from.value.publicKey) &&
			!next.value.publicKey.equals(startNode.value.publicKey)
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
		if (set.has(currentNode.value.publicKey.hashcode())) break;

		const [nextPoint, distanceStart, distanceEnd, wrapped] = getNextPoint();

		if (distanceStart <= coveringWidth) {
			set.add(currentNode.value.publicKey.hashcode());
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
				const nextNext = getNextMatured(next);
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
		startNode.value.role.offset,
		(startNode.value.role.offset + coveringWidth) % 1
	]) {
		let done = false;
		const unmatured: string[] = [];
		collectNodesAroundPoint(
			t,
			roleAge,
			peers,
			isMatured(startNode.value.role, t, roleAge) ? startNode : peers.head, // start at startNode is matured, else start at head (we only seek to find one matured node at the point)
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
	return [...set];
};
