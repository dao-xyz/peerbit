import yallist from "yallist";
import { ReplicatorRect } from "./replication.js";
import { Replicator, containsPoint } from "./role.js";
import { PublicSignKey } from "@peerbit/crypto";

export const collectNodesAroundPoint = (
	time: number,
	roleAge: number,
	peers: yallist<ReplicatorRect>,
	currentNode: yallist.Node<ReplicatorRect> | null,
	collector: Set<string>,
	point: number,
	once: boolean = false
) => {
	let matured = 0;

	const maybeIncrementMatured = (role: Replicator) => {
		if (isMatured(role, time, roleAge)) {
			matured++;
			return true;
		}
		return false;
	};

	// Assume peers does not mutate during this loop
	const startNode = currentNode;
	const diffs: { diff: number; rect: ReplicatorRect }[] = [];
	while (currentNode) {
		if (containsPoint(currentNode.value.role, point)) {
			collector.add(currentNode.value.publicKey.hashcode());
			if (maybeIncrementMatured(currentNode.value.role)) {
				if (once) {
					return;
				}
			}
		} else {
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

	if (matured === 0) {
		diffs.sort((x, y) => x.diff - y.diff);
		for (const node of diffs) {
			collector.add(node.rect.publicKey.hashcode());
			maybeIncrementMatured(node.rect.role);
			if (matured > 0) {
				break;
			}
		}
	}
};

const isMatured = (role: Replicator, now: number, minAge: number) => {
	return now - Number(role.timestamp) >= minAge;
};

export const getCover = (
	coveringWidth: number,
	peers: yallist<ReplicatorRect>,
	roleAge: number,
	startNodeIdentity?: PublicSignKey
) => {
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
	const set: Set<string> = new Set();
	let currentNode = startNode;

	/**
	 * The purpose of this loop is to cover at least coveringWidth
	 * so that if we query all nodes in this range, we know we will
	 * "query" all data in that range
	 */

	let wrapped = false;
	const getNextPoint = (): [number, number, boolean] => {
		let nextPoint =
			currentNode.value.role.offset + currentNode.value.role.factor;
		if (nextPoint > 1) {
			wrapped = true;
			nextPoint = nextPoint % 1;
		}
		let distance: number;
		if (wrapped) {
			distance = 1 - startNode.value.role.offset + nextPoint;
		} else {
			distance =
				currentNode.value.role.offset -
				startNode.value.role.offset +
				currentNode.value.role.factor;
		}
		return [nextPoint, distance, wrapped];
	};

	const t = +new Date();

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

	outer: while (currentNode) {
		if (set.has(currentNode.value.publicKey.hashcode())) break;

		set.add(currentNode.value.publicKey.hashcode());

		const [nextPoint, distance, wrapped] = getNextPoint();

		if (distance >= coveringWidth) {
			break;
		}

		let next = currentNode.next || peers.head;
		while (next) {
			if (next.value.publicKey.equals(startNode.value.publicKey)) {
				break outer;
			}

			if (
				next.value.role.offset < nextPoint &&
				(next.value.role.offset > currentNode.value.role.offset || wrapped)
			) {
				if (next.value.role.offset + next.value.role.factor > nextPoint) {
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
			} else {
				// we got the best choice
				break;
			}
			next = next.next;
		}
		currentNode = next!;
	}

	// collect around the boundary of the start
	collectNodesAroundPoint(
		t,
		roleAge,
		peers,
		isMatured(startNode.value.role, t, roleAge) ? startNode : peers.head, // start at startNode is matured, else start at head (we only seek to find one matured node at the point)
		set,
		startNode.value.role.offset,
		true
	);

	// collect around the boundary of the end
	collectNodesAroundPoint(
		t,
		roleAge,
		peers,
		startNode, // start somewhere close to the border
		set,
		(startNode.value.role.offset + coveringWidth) % 1,
		true
	);
	return [...set];
};
