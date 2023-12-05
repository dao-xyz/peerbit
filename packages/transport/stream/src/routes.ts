import { PublicSignKey } from "@peerbit/crypto";

export class Routes {
	// END receiver -> Neighbour

	routes: Map<
		string,
		Map<string, { session: number; list: { hash: string; distance: number }[] }>
	> = new Map();

	pendingRoutes: Map<
		number,
		Map<
			string,
			{
				from: string;
				neighbour: string;
				distance: number;
			}[]
		>
	> = new Map();
	latestSession: number;

	constructor(readonly me: string) {
		this.latestSession = 0;
	}

	clear() {
		this.routes.clear();
		this.pendingRoutes.clear();
	}

	add(
		from: string,
		neighbour: string,
		target: string,
		distance: number,
		session: number
	) {
		let fromMap = this.routes.get(from);
		if (!fromMap) {
			fromMap = new Map();
			this.routes.set(from, fromMap);
		}

		let prev = fromMap.get(target) || {
			session: session ?? +new Date(),
			list: [] as { hash: string; distance: number }[]
		};

		this.latestSession = Math.max(this.latestSession, session);

		if (session != null) {
			// this condition means that when we add new routes in a session that is newer
			if (prev.session < session) {
				prev = { session, list: [] }; // reset route info how to reach this target
			} else if (prev.session > session) {
				return; // new routing information superseedes this
			}
		}

		if (from === this.me && neighbour === target) {
			// force distance to neighbour as targets to always favor directly sending to them
			// i.e. if target is our neighbour, always assume the shortest path to them is the direct path
			distance = -1;
		}

		for (const route of prev.list) {
			if (route.hash === neighbour) {
				route.distance = Math.min(route.distance, distance);
				prev.list.sort((a, b) => a.distance - b.distance);
				return;
			}
		}
		prev.list.push({ distance, hash: neighbour });
		prev.list.sort((a, b) => a.distance - b.distance);
		fromMap.set(target, prev);
	}

	removeTarget(target: string) {
		this.routes.delete(target);
		for (const [fromMapKey, fromMap] of this.routes) {
			// delete target
			fromMap.delete(target);
			if (fromMap.size === 0) {
				this.routes.delete(fromMapKey);
			}
		}
		return [target];
	}

	removeNeighbour(target: string) {
		this.routes.delete(target);
		const maybeUnreachable: Set<string> = new Set([target]);
		for (const [fromMapKey, fromMap] of this.routes) {
			// delete target
			fromMap.delete(target);

			// delete this as neighbour
			for (const [remote, neighbours] of fromMap) {
				neighbours.list = neighbours.list.filter((x) => x.hash !== target);
				if (neighbours.list.length === 0) {
					fromMap.delete(remote);
					maybeUnreachable.add(remote);
				}
			}

			if (fromMap.size === 0) {
				this.routes.delete(fromMapKey);
			}
		}
		return [...maybeUnreachable].filter((x) => !this.isReachable(this.me, x));
	}

	findNeighbor(from: string, target: string) {
		return this.routes.get(from)?.get(target);
	}

	isReachable(from: string, target: string) {
		return this.routes.get(from)?.has(target) === true;
	}

	hasShortestPath(target: string) {
		const path = this.routes.get(this.me)?.get(target);
		if (!path) {
			return false;
		}
		return path.list[0].distance <= 0;
	}

	hasTarget(target: string) {
		for (const [k, v] of this.routes) {
			if (v.has(target)) {
				return true;
			}
		}
		return false;
	}

	getDependent(target: string) {
		const dependent: string[] = [];
		for (const [fromMapKey, fromMap] of this.routes) {
			if (fromMapKey !== this.me && fromMap.has(target)) {
				dependent.push(fromMapKey);
			}
		}
		return dependent;
	}

	count(from = this.me) {
		const set: Set<string> = new Set();
		const map = this.routes.get(from);
		if (map) {
			for (const [k, v] of map) {
				set.add(k);
				for (const peer of v.list) {
					set.add(peer.hash);
				}
			}
		}
		return set.size;
	}

	countAll() {
		let size = 0;
		for (const [from, map] of this.routes) {
			for (const [k, v] of map) {
				size += v.list.length;
			}
		}
		return size;
	}

	// for all tos if
	getFanout(
		from: PublicSignKey,
		tos: string[],
		redundancy: number
	): Map<string, { to: string; timestamp: number }[]> | undefined {
		if (tos.length === 0) {
			return undefined;
		}

		let fanoutMap:
			| Map<string, { to: string; timestamp: number }[]>
			| undefined = undefined;

		const fromKey = from.hashcode();

		// Message to > 0
		if (tos.length > 0) {
			for (const to of tos) {
				if (to === this.me || fromKey === to) {
					continue; // don't send to me or backwards
				}

				const neighbour = this.findNeighbor(fromKey, to);
				if (neighbour) {
					let foundClosest = false;
					for (
						let i = 0;
						i < Math.min(neighbour.list.length, redundancy);
						i++
					) {
						const distance = neighbour.list[i].distance;
						if (distance >= redundancy) {
							break; // because neighbour listis sorted
						}
						if (distance <= 0) {
							foundClosest = true;
						}
						const fanout = (fanoutMap || (fanoutMap = new Map())).get(
							neighbour.list[i].hash
						);
						if (!fanout) {
							fanoutMap.set(neighbour.list[i].hash, [
								{ to, timestamp: neighbour.session }
							]);
						} else {
							fanout.push(to);
						}
					}
					if (!foundClosest && from.hashcode() === this.me) {
						return undefined; // we dont have the shortest path to our target (yet). Send to all
					}

					continue;
				}

				// we can't find path, send message to all peers
				return undefined;
			}
		}
		return fanoutMap || (fanoutMap = new Map());
	}

	/**
	 * Returns a list of a prunable nodes that are not needed to reach all remote nodes
	 */
	getPrunable(neighbours: string[]): string[] {
		const map = this.routes.get(this.me);
		if (map) {
			// check if all targets can be reached without it
			return neighbours.filter((candidate) => {
				for (const [target, neighbours] of map) {
					if (
						target !== candidate &&
						neighbours.list.length === 1 &&
						neighbours.list[0].hash === candidate
					) {
						return false;
					}
				}
				return true;
			});
		}
		return [];
	}

	public addPendingRouteConnection(
		session: number,
		route: {
			from: string;
			neighbour: string;
			target: PublicSignKey;
			distance: number;
		}
	) {
		let map = this.pendingRoutes.get(session);
		if (!map) {
			map = new Map();
			this.pendingRoutes.set(session, map);
		}
		let arr = map.get(route.target.hashcode());
		if (!arr) {
			arr = [];
			map.set(route.target.hashcode(), arr);
		}
		arr.push(route);

		const neighbour = this.findNeighbor(route.from, route.target.hashcode());
		if (!neighbour || neighbour.session === session) {
			// Commit directly since we dont have any data at all (better have something than nothing)
			this.commitPendingRouteConnection(session, route.target.hashcode());
		}
	}

	// always commit if we dont know the peer yet
	// do pending commits per remote (?)

	public commitPendingRouteConnection(session: number, target?: string) {
		const map = this.pendingRoutes.get(session);
		if (!map) {
			return;
		}
		if (target) {
			const routes = map.get(target);
			if (routes) {
				for (const route of routes) {
					this.add(
						route.from,
						route.neighbour,
						target,
						route.distance,
						session
					);
				}
			}
			/* 	if (map.size === 1) {
					this.pendingRoutes.delete(session);
					return;
				} */
		} else {
			for (const [target, routes] of map) {
				for (const route of routes) {
					this.add(
						route.from,
						route.neighbour,
						target,
						route.distance,
						session
					);
				}
			}
		}

		this.pendingRoutes.delete(session);
	}
}
