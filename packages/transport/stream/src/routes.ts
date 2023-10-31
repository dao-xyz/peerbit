import { PublicSignKey } from "@peerbit/crypto";

export class Routes {
	// END receiver -> Neighbour

	routes: Map<
		string,
		Map<string, { session: number; list: { hash: string; distance: number }[] }>
	> = new Map();

	constructor(readonly me: string) {}

	clear() {
		this.routes.clear();
	}

	add(
		from: string,
		neighbour: string,
		target: string,
		distance: number,
		session?: number
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
		if (session != null && prev.session < session) {
			// second condition means that when we add new routes in a session that is newer
			prev = { session, list: [] }; // reset route info how to reach this target
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

	/* 
		add(neighbour: string, target: string, quality: number) {
			this.routes.set(target, { neighbour, distance: quality })
		}
	 */

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

	/* removeTarget(to: string, neighbour?: string) {
		for (const [fromMapKey, fromMap] of this.routes) {
			let unreachable = true;
			if(neighbour)
			{
				const neighbours = fromMap.get(to);
				unreachable = neighbours?.list.find(x=>x.hash === neighbour)
			}
			if (unreachable) {
				fromMap.delete(to)
				if (fromMap.size === 0) {
					this.routes.delete(fromMapKey);
				}
			}

		}

		this.removeNeighbour(to)
	}

	removeNeighbour(neighbour: string) {
		const removed: string[] = [];
		for (const [fromMapKey, fromMap] of this.routes) {
			for (const [target, v] of fromMap) {
				const keepRoutes: { hash: string; distance: number }[] = [];
				for (const route of v.list) {
					if (route.hash !== neighbour) {
						keepRoutes.push(route);
					}
				}

				if (keepRoutes.length === 0) {
					fromMap.delete(target);
					removed.push(target);
				} else {
					fromMap.set(target, { session: v.session, list: keepRoutes });
				}
			}

			removed.push(neighbour);
			fromMap.delete(neighbour);
			if (fromMap.size === 0) {
				this.routes.delete(fromMapKey);
			}
		}



		// Return all unreachable
		return removed.filter((x) => !this.routes.has(x));
	} */

	findNeighbor(from: string, target: string) {
		return this.routes.get(from)?.get(target);
	}

	isReachable(from: string, target: string) {
		return this.routes.get(from)?.has(target) === true;
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

	count() {
		const set: Set<string> = new Set();
		const map = this.routes.get(this.me);
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

	// for all tos if
	getFanout(
		from: PublicSignKey,
		tos: string[],
		redundancy: number
	): Map<string, string[]> | undefined {
		if (tos.length === 0) {
			return undefined;
		}

		let fanoutMap: Map<string, string[]> | undefined = undefined;

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
							fanoutMap.set(neighbour.list[i].hash, [to]);
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
}
