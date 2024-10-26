import { AbortError, delay } from "@peerbit/time";

export const MAX_ROUTE_DISTANCE = Number.MAX_SAFE_INTEGER - 1;

type RelayInfo = {
	session: number;
	hash: string;
	expireAt?: number;
	distance: number;
};
type RouteInfo = {
	remoteSession: number;
	session: number;
	list: RelayInfo[];
};

export class Routes {
	// FROM -> TO -> { ROUTE INFO, A list of neighbours that we can send data through to reach to}
	routes: Map<string, Map<string, RouteInfo>> = new Map();

	remoteInfo: Map<string, { session?: number }> = new Map();

	// Maximum amount of time to retain routes that are not valid anymore
	// once we receive new route info to reach a specific target
	routeMaxRetentionPeriod: number;

	signal?: AbortSignal;

	constructor(
		readonly me: string,
		options?: { routeMaxRetentionPeriod?: number; signal?: AbortSignal },
	) {
		this.routeMaxRetentionPeriod =
			options?.routeMaxRetentionPeriod ?? 10 * 1000;
		this.signal = options?.signal;
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
				const keepRoutes: RelayInfo[] = [];
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
		session: number,
		remoteSession: number,
	): "new" | "updated" | "restart" {
		let fromMap = this.routes.get(from);
		if (!fromMap) {
			fromMap = new Map();
			this.routes.set(from, fromMap);
		}

		let prev = fromMap.get(target);
		const routeDidExist = prev;
		const isNewSession = !prev || session > prev.session;
		const isOldSession = prev && session < prev.session;

		if (!prev) {
			prev = { session, remoteSession, list: [] as RelayInfo[] };
			fromMap.set(target, prev);
		}

		const isRelayed = from !== this.me;
		const targetIsNeighbour = neighbour === target;
		if (targetIsNeighbour) {
			if (!isRelayed) {
				// force distance to neighbour as targets to always favor directly sending to them
				// i.e. if target is our neighbour, always assume the shortest path to them is the direct path
				distance = -1;
			}
		}

		let isNewRemoteSession = false;
		if (routeDidExist) {
			// if the remote session is later, we consider that the remote has 'restarted'
			isNewRemoteSession = remoteSession > (prev.remoteSession || -1);
			prev.remoteSession = Math.max(remoteSession, prev.remoteSession || -1);
		}

		prev.session = Math.max(session, prev.session);

		const scheduleCleanup = () => {
			return delay(this.routeMaxRetentionPeriod + 100, { signal: this.signal })
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
		};

		// Update routes and cleanup all old routes that are older than latest session - some threshold
		if (isNewSession) {
			// Mark previous routes as old
			const expireAt = +new Date() + this.routeMaxRetentionPeriod;
			let foundNodeToExpire = false;
			for (const route of prev.list) {
				// delete all routes after a while
				if (!route.expireAt) {
					foundNodeToExpire = true;
					route.expireAt = expireAt;
				}
			}

			// Initiate cleanup
			if (distance !== -1 && foundNodeToExpire) {
				scheduleCleanup();
			}
		} else if (isOldSession) {
			scheduleCleanup();
		}

		// Modify list for new/update route
		let exist = false;
		for (const route of prev.list) {
			if (route.hash === neighbour) {
				// if route is faster or just as fast, update existing route
				if (isNewSession) {
					if (route.distance > distance) {
						route.distance = distance;
						route.session = session;
						route.expireAt = undefined; // remove expiry since we updated
						prev.list.sort((a, b) => a.distance - b.distance);
						return isNewRemoteSession ? "restart" : "updated";
					} else if (route.distance === distance) {
						route.session = session;
						route.expireAt = undefined; // remove expiry since we updated
						return isNewRemoteSession ? "restart" : "updated";
					}
				}

				exist = true;
				// else break and push the route as a new route (that ought to be longer)
				break;
			}
		}

		// if not exist add new route
		// else if it exist then we only end up here if the distance is longer than prev, this means that we want to keep prev while adding the new route
		if (!exist || isNewSession) {
			prev.list.push({
				distance,
				session,
				hash: neighbour,
				expireAt: isOldSession
					? +new Date() + this.routeMaxRetentionPeriod
					: undefined,
			});
			prev.list.sort((a, b) => a.distance - b.distance);
		}

		return exist ? (isNewRemoteSession ? "restart" : "updated") : "new";
	}

	/**
	 *
	 * @param target
	 * @returns unreachable nodes (from me) after removal
	 */
	remove(target: string) {
		this.routes.delete(target);
		const maybeUnreachable: Set<string> = new Set();
		let targetRemoved = false;
		for (const [fromMapKey, fromMap] of this.routes) {
			// delete target
			const deletedAsTarget = fromMap.delete(target);
			targetRemoved =
				targetRemoved || (deletedAsTarget && fromMapKey === this.me);

			// delete this as neighbour
			for (const [remote, neighbours] of fromMap) {
				const filtered = neighbours.list.filter((x) => x.hash !== target);
				neighbours.list = filtered;
				if (neighbours.list.length === 0) {
					fromMap.delete(remote);

					if (fromMapKey === this.me) {
						// TODO we only return maybeUnreachable if the route starts from me.
						// expected?
						maybeUnreachable.add(remote);
					}
				}
			}

			if (fromMap.size === 0) {
				this.routes.delete(fromMapKey);
			}
		}
		this.remoteInfo.delete(target);

		if (targetRemoved) {
			maybeUnreachable.add(target);
		}
		return [...maybeUnreachable].filter((x) => !this.isReachable(this.me, x));
	}

	removeNeighbour(neighbour: string) {
		this.routes.delete(neighbour);
		for (const [_fromMapKey, fromMap] of this.routes) {
			for (const [key, routes] of fromMap) {
				routes.list = routes.list.filter((x) => x.hash !== neighbour);
				if (routes.list.length === 0) {
					fromMap.delete(key);
				}
			}
		}
	}

	findNeighbor(from: string, target: string) {
		return this.routes.get(from)?.get(target);
	}

	isReachable(from: string, target: string, maxDistance = MAX_ROUTE_DISTANCE) {
		const remoteInfo = this.remoteInfo.get(target);
		const routeInfo = this.routes.get(from)?.get(target);
		if (!routeInfo) {
			return false;
		}
		if (!remoteInfo) {
			return false;
		}
		if (
			// TODO why do we need this check?
			// eslint-disable-next-line eqeqeq
			routeInfo.remoteSession == undefined ||
			remoteInfo.session === undefined
		) {
			return false;
		}

		if (routeInfo.remoteSession < remoteInfo.session) {
			// route info is older than remote info
			return false;
		}

		return (
			(routeInfo?.list[0]?.distance ?? Number.MAX_SAFE_INTEGER) <= maxDistance
		);
	}

	hasTarget(target: string) {
		for (const [_k, v] of this.routes) {
			if (v.has(target)) {
				return true;
			}
		}
		return false;
	}

	updateSession(remote: string, session?: number) {
		if (session == null) {
			this.remoteInfo.delete(remote);
			return false;
		}

		const remoteInfo = this.remoteInfo.get(remote);
		if (remoteInfo) {
			// remote has restartet, mark all routes originating from me to the remote as 'old'
			if (remoteInfo.session === -1) {
				return false;
			}
			if (session === -1) {
				remoteInfo.session = -1;
				return false;
			} else {
				if (session > (remoteInfo.session || -1)) {
					remoteInfo.session = session;
					return true;
				}
				return false;
			}
		} else if (session !== undefined) {
			this.remoteInfo.set(remote, { session });
			return true;
		}
		return false;
	}

	getSession(remote: string): number | undefined {
		return this.remoteInfo.get(remote)?.session;
	}

	isUpToDate(target: string, route: RouteInfo) {
		const peerInfo = this.remoteInfo.get(target);
		return peerInfo?.session != null && route.remoteSession >= peerInfo.session;
	}

	getDependent(peer: string) {
		const dependent: string[] = [];

		outer: for (const [fromMapKey, fromMap] of this.routes) {
			if (fromMapKey === this.me) {
				continue; // skip this because these routes are starting from me. We are looking for routes that affect others
			}

			// If the route is to the target
			// tell 'from' that it is no longer reachable
			if (fromMap.has(peer)) {
				dependent.push(fromMapKey);
				continue outer;
			}

			// If the relay is dependent of peer
			// tell 'from' that it is no longer reachable
			for (const [_to, through] of fromMap) {
				for (const neighbour of through.list) {
					if (neighbour.hash === peer) {
						dependent.push(fromMapKey);
						continue outer;
					}
				}
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
		for (const [_from, map] of this.routes) {
			for (const [_k, v] of map) {
				size += v.list.length;
			}
		}
		return size;
	}

	// for all tos if
	getFanout(
		from: string,
		tos: string[],
		redundancy: number,
	): Map<string, Map<string, { to: string; timestamp: number }>> | undefined {
		if (tos.length === 0) {
			return undefined;
		}

		let fanoutMap:
			| Map<string, Map<string, { to: string; timestamp: number }>>
			| undefined = undefined;

		const relaying = from !== this.me;

		// Message to > 0
		if (tos.length > 0) {
			for (const to of tos) {
				if (to === this.me || from === to) {
					continue; // don't send to me or backwards
				}

				// neighbours that are links from 'from' to 'to'
				const neighbour = this.findNeighbor(from, to);
				if (neighbour) {
					let foundClosest = false;
					let added = 0;
					for (let i = 0; i < neighbour.list.length; i++) {
						const { distance, session, expireAt } = neighbour.list[i];

						if (expireAt && !relaying) {
							// don't send on old paths if not relaying
							// TODO there could be a benifit of doing this (?)
							continue;
						}

						if (distance >= redundancy) {
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
							distance <= 0 &&
							session <= neighbour.session // (<) will never be the case, but we do add routes in the tests with later session timestamps
						) {
							foundClosest = true;
							if (distance === -1) {
								break; // dont send to more peers if we have the direct route
							}
						}

						if (!expireAt) {
							// only count non-expired routes or if we are relaying then also count expired routes
							added++;
							if (added >= redundancy) {
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
