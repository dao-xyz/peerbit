import type { PeerId } from "@libp2p/interface-peer-id";
import { default as path, PathFinder } from "ngraph.path";
import createGraph, { Graph } from "ngraph.graph";

export class Routes {
	graph: Graph<PeerId, any>;
	private pathFinder: PathFinder<any>;
	private peerId: string;
	constructor(peerId: string) {
		this.peerId = peerId;
		this.graph = createGraph();
		this.pathFinder = path.aGreedy(this.graph);
	}

	get linksCount() {
		return this.graph.getLinksCount();
	}

	get nodeCount() {
		return this.graph.getNodesCount();
	}

	get links(): [string, string][] {
		const links: [string, string][] = [];
		this.graph.forEachLink((link) => {
			links.push([link.fromId.toString(), link.toId.toString()]);
		});
		return links;
	}
	/**
	 *
	 * @param from
	 * @param to
	 * @returns new nodes
	 */
	addLink(from: string, to: string, origin: string = this.peerId): string[] {
		if (from > to) {
			const temp = from;
			from = to;
			to = temp;
		}

		const linkExisted = !!this.getLink(from, to);
		const newReachableNodesFromOrigin: string[] = [];
		if (!linkExisted) {
			const currentTime = +new Date();
			const fromWasReachable =
				origin == from || this.getPath(origin, from).length;
			const toWasReachable = origin === to || this.getPath(origin, to).length;
			const fromIsNowReachable = toWasReachable;
			const toIsNowReachable = fromWasReachable;

			const visited = new Set<string | number>();
			const newReachableNodes: string[] = [];
			if (fromIsNowReachable) {
				newReachableNodes.push(from);
			}
			if (toIsNowReachable) {
				newReachableNodes.push(to);
			}
			if (fromWasReachable) {
				visited.add(from);
			}
			if (toWasReachable) {
				visited.add(to);
			}

			this.graph.addLink(from, to, currentTime);

			for (const newReachableNode of newReachableNodes) {
				// get all nodes from this and add them to the new reachable set of nodes one can access from origin

				const node = this.graph.getNode(newReachableNode); // iterate from the not reachable node
				const stack = [node];
				while (stack.length > 0) {
					const node = stack.shift();
					if (!node) {
						continue;
					}
					if (visited.has(node.id)) {
						continue;
					}

					visited.add(node.id);

					const links = node.links;

					if (links) {
						for (const link of links) {
							if (link.data > currentTime) {
								continue; // a new link has been added while we are iterating, dont follow this path
							}

							const toId = node.id === link.toId ? link.fromId : link.toId;
							if (visited.has(toId)) {
								continue;
							}

							const next = this.graph.getNode(toId);
							if (next) {
								stack.push(next);
							}
						}
					}
					newReachableNodesFromOrigin.push(node.id.toString());
				}
			}
		} else {
			this.graph.addLink(from, to, +new Date());
		}

		if (
			newReachableNodesFromOrigin.length === 1 &&
			this.linksCount >= 2 &&
			linkExisted === false
		) {
			const t = 123;
		}
		return newReachableNodesFromOrigin;
	}

	/**
	 *
	 * @param from
	 * @param to
	 * @param origin
	 * @returns nodes that are no longer reachable from origin
	 */
	deleteLink(from: string, to: string, origin: string = this.peerId): string[] {
		const link = this.getLink(from, to);
		if (link) {
			const date = +new Date();
			const fromWasReachable =
				origin == from || this.getPath(origin, from).length;
			const toWasReachable = origin === to || this.getPath(origin, to).length;
			this.graph.removeLink(link);

			const unreachableNodesFromOrigin: string[] = [];
			if (
				fromWasReachable &&
				origin !== from &&
				this.getPath(origin, from).length === 0
			) {
				unreachableNodesFromOrigin.push(from);
			}
			if (
				toWasReachable &&
				origin !== to &&
				this.getPath(origin, to).length === 0
			) {
				unreachableNodesFromOrigin.push(to);
			}

			// remove subgraphs that are now disconnected from me
			for (const disconnected of [...unreachableNodesFromOrigin]) {
				const node = this.graph.getNode(disconnected);

				if (!node) {
					continue;
				}

				const stack = [node];
				const visited = new Set<string | number>();
				while (stack.length > 0) {
					const node = stack.shift();
					if (!node) {
						continue;
					}
					if (visited.has(node.id)) {
						continue;
					}

					visited.add(node.id);

					const links = node.links;

					if (links) {
						for (const link of links) {
							if (link.data > date) {
								continue; // don't follow path because this is a new link that might provide some new connectivity
							}

							const toId = node.id === link.toId ? link.fromId : link.toId;
							if (visited.has(toId)) {
								continue;
							}

							const next = this.graph.getNode(toId);
							if (next) {
								stack.push(next);
							}
						}
					}
					if (
						this.graph.removeNode(node.id) &&
						disconnected !== node.id.toString()
					) {
						unreachableNodesFromOrigin.push(node.id.toString());
					}
				}
			}
			return unreachableNodesFromOrigin;
		}
		return [];
	}

	getLink(from: string, to: string) {
		return from < to
			? this.graph.getLink(from, to)
			: this.graph.getLink(to, from);
	}

	getPath(from: string, to: string) {
		try {
			const path = this.pathFinder.find(from, to);
			if (path?.length > 0 && path[0].id !== from) {
				path.reverse();
			}
			return path;
		} catch (error) {
			return [];
		}
	}
	clear() {
		this.graph.clear();
	}
}
