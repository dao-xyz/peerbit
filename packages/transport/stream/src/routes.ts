import { AbortError, delay } from "@peerbit/time";

export const MAX_ROUTE_DISTANCE = Number.MAX_SAFE_INTEGER - 1;
type RouteInfo = {
	session: number;
	hash: string;
	expireAt?: number;
	distance: number;
};
export class Routes {
	// END receiver -> Neighbour

	routes: Map<
		string,
		Map<
			string,
			{
				latestSession: number;
				list: RouteInfo[];
			}
		>
	> = new Map();

	routeMaxRetentionPeriod: number;
	signal: AbortSignal;

	constructor(
		readonly me: string,
		options: { routeMaxRetentionPeriod: number; signal: AbortSignal }
	) {
		this.routeMaxRetentionPeriod = options.routeMaxRetentionPeriod;
		this.signal = options.signal;
	}

	clear() {
		this.routes.clear();
	}

	private cleanup(from: string, to: string) {
		const fromMap = this.routes.get(from);
		if (fromMap) {
			const map = fromMap.get(to);
			if (map) {
				const now = +new Date();
				const keepRoutes: RouteInfo[] = [];
				for (const route of map.list) {
					// delete all routes after a while
					if (route.expireAt != null && route.expireAt < now) {
						// expired
					} else {
						keepRoutes.push(route);
					}
				}

				if (keepRoutes.length > 0) {
					map.list = keepRoutes;
				} else {
					fromMap.delete(to);
					if (fromMap.size === 1) {
						this.routes.delete(from);
					}
				}
			}
		}
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

		let prev = fromMap.get(target);

		if (!prev) {
			prev = { latestSession: 0, list: [] as RouteInfo[] };
			fromMap.set(target, prev);
		}

		if (from === this.me && neighbour === target) {
			// force distance to neighbour as targets to always favor directly sending to them
			// i.e. if target is our neighbour, always assume the shortest path to them is the direct path
			distance = -1;
		}

		// Update routes and cleanup all old routes that are older than latest session - some threshold
		const isNewSession = session > prev.latestSession;
		prev.latestSession = Math.max(session, prev.latestSession);

		if (isNewSession) {
			// Mark previous routes as old

			const expireAt = +new Date() + this.routeMaxRetentionPeriod;
			for (const route of prev.list) {
				// delete all routes after a while
				if (!route.expireAt) {
					route.expireAt = expireAt;
				}
			}

			// Initiate cleanup
			if (distance !== -1) {
				delay(this.routeMaxRetentionPeriod + 100, { signal: this.signal })
					.then(() => {
						this.cleanup(from, target);
					})
					.catch((e) => {
						if (e instanceof AbortError) {
							// skip
							return;
						}
						throw e;
					});
			}
		}

		// Modify list for new/update route
		for (const route of prev.list) {
			if (route.hash === neighbour) {
				// if route is faster or just as fast, update existing route
				if (route.distance > distance) {
					route.distance = distance;
					route.session = session;
					route.expireAt = undefined; // remove expiry since we updated
					prev.list.sort((a, b) => a.distance - b.distance);
					return;
				} else if (route.distance === distance) {
					route.session = session;
					route.expireAt = undefined; // remove expiry since we updated
					return;
				}

				// else break and push the route as a new route (that ought to be longer)
				break;
			}
		}

		prev.list.push({ distance, session, hash: neighbour });
		prev.list.sort((a, b) => a.distance - b.distance);
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

	isReachable(from: string, target: string, maxDistance = MAX_ROUTE_DISTANCE) {
		return (
			(this.routes.get(from)?.get(target)?.list[0]?.distance ??
				Number.MAX_SAFE_INTEGER) <= maxDistance
		);
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
		from: string,
		tos: string[],
		redundancy: number
	): Map<string, Map<string, { to: string; timestamp: number }>> | undefined {
		if (tos.length === 0) {
			return undefined;
		}

		let fanoutMap:
			| Map<string, Map<string, { to: string; timestamp: number }>>
			| undefined = undefined;

		// Message to > 0
		if (tos.length > 0) {
			for (const to of tos) {
				if (to === this.me || from === to) {
					continue; // don't send to me or backwards
				}

				const neighbour = this.findNeighbor(from, to);
				if (neighbour) {
					let foundClosest = false;
					let redundancyModified = redundancy;
					for (let i = 0; i < neighbour.list.length; i++) {
						const { distance, session } = neighbour.list[i];
						if (distance >= redundancyModified) {
							break; // because neighbour listis sorted
						}

						let fanout: Map<string, { to: string; timestamp: number }> = (
							fanoutMap || (fanoutMap = new Map())
						).get(neighbour.list[i].hash);
						if (!fanout) {
							fanout = new Map();
							fanoutMap.set(neighbour.list[i].hash, fanout);
						}

						fanout.set(to, { to, timestamp: session });

						if (
							(distance == 0 && session === neighbour.latestSession) ||
							distance == -1
						) {
							foundClosest = true;

							if (distance == -1) {
								// remove 1 from the expected redunancy since we got a route with negative 1 distance
								// if we do not do this, we would get 2 routes if redundancy = 1, {-1, 0}, while it should just be
								// {-1} in this case
								redundancyModified -= 1;
								break;
							}
						}
					}

					if (!foundClosest && from === this.me) {
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
}
