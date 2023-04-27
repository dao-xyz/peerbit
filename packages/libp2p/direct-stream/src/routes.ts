import Graphs from "graphology";
import type { MultiUndirectedGraph } from "graphology";
import { dijkstra, unweighted } from "graphology-shortest-path";
import { logger } from "./logger.js";
import { MinimalEdgeMapper } from "graphology-utils/getters";

interface EdgeData {
	weight: number;
	time: number;
}
export class Routes {
	graph: MultiUndirectedGraph<any, EdgeData>;
	private peerId: string;
	constructor(peerId: string) {
		this.peerId = peerId;
		this.graph = new (Graphs as any).UndirectedGraph();
	}

	get linksCount() {
		return this.graph.edges().length;
	}

	get nodeCount() {
		return this.graph.nodes().length;
	}

	/**
	 *
	 * @param from
	 * @param to
	 * @returns new nodes
	 */
	addLink(
		from: string,
		to: string,
		weight: number,
		origin: string = this.peerId
	): string[] {
		const linkExisted = this.hasLink(from, to);
		const newReachableNodesFromOrigin: string[] = [];
		if (!linkExisted) {
			const currentTime = +new Date();
			const fromWasReachable =
				origin == from ||
				this.getPath(origin, from, { unweighted: true }).length;
			const toWasReachable =
				origin === to || this.getPath(origin, to, { unweighted: true }).length;
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

			if (!this.graph.hasNode(from)) {
				this.graph.addNode(from);
			}
			if (!this.graph.hasNode(to)) {
				this.graph.addNode(to);
			}

			this.graph.addUndirectedEdge(from, to, { weight, time: currentTime });

			for (const newReachableNode of newReachableNodes) {
				// get all nodes from this and add them to the new reachable set of nodes one can access from origin

				const stack = [newReachableNode]; // iterate from the not reachable node
				while (stack.length > 0) {
					const node = stack.shift();
					if (!node) {
						continue;
					}
					if (visited.has(node)) {
						continue;
					}

					visited.add(node);
					const neighbors = this.graph.neighbors(node);
					for (const neighbor of neighbors) {
						const edge = this.graph.undirectedEdge(node, neighbor);
						if (!edge) {
							logger.warn(`Missing edge between: ${node} - ${neighbor}`);
							continue;
						}

						const attributes = this.graph.getEdgeAttributes(edge);
						if (attributes.time > currentTime) {
							continue; // a new link has been added while we are iterating, dont follow this path
						}

						if (visited.has(neighbor)) {
							continue;
						}

						stack.push(neighbor);
					}
					newReachableNodesFromOrigin.push(node);
				}
			}
		} else {
			// update weight
			const edge = this.graph.undirectedEdge(from, to);
			this.graph.setEdgeAttribute(edge, "weight", weight);
			this.graph.setEdgeAttribute(edge, "time", +new Date());
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
				origin == from ||
				this.getPath(origin, from, { unweighted: true }).length;
			const toWasReachable =
				origin === to || this.getPath(origin, to, { unweighted: true }).length;
			this.graph.dropEdge(link);

			const unreachableNodesFromOrigin: string[] = [];
			if (
				fromWasReachable &&
				origin !== from &&
				this.getPath(origin, from, { unweighted: true }).length === 0
			) {
				unreachableNodesFromOrigin.push(from);
			}
			if (
				toWasReachable &&
				origin !== to &&
				this.getPath(origin, to, { unweighted: true }).length === 0
			) {
				unreachableNodesFromOrigin.push(to);
			}

			// remove subgraphs that are now disconnected from me
			for (const disconnected of [...unreachableNodesFromOrigin]) {
				const node = disconnected;
				if (!this.graph.hasNode(node)) {
					continue;
				}

				const stack = [disconnected];
				const visited = new Set<string | number>();
				while (stack.length > 0) {
					const node = stack.shift();
					const nodeId = node;
					if (!nodeId || !this.graph.hasNode(nodeId)) {
						continue;
					}
					if (visited.has(nodeId)) {
						continue;
					}

					visited.add(nodeId);

					const neighbors = this.graph.neighbors(node);

					for (const neighbor of neighbors) {
						const edge = this.graph.undirectedEdge(node, neighbor);
						if (!edge) {
							logger.warn(`Missing edge between: ${node} - ${neighbor}`);
							continue;
						}
						const attributes = this.graph.getEdgeAttributes(edge);
						if (attributes.time > date) {
							continue; // don't follow path because this is a new link that might provide some new connectivity
						}

						if (visited.has(neighbor)) {
							continue;
						}

						stack.push(neighbor);
					}
					this.graph.dropNode(nodeId);
					if (disconnected !== nodeId) {
						unreachableNodesFromOrigin.push(nodeId.toString());
					}
				}
			}
			return unreachableNodesFromOrigin;
		}
		return [];
	}

	getLink(from: string, to: string): string | undefined {
		if (!this.graph.hasNode(from) || !this.graph.hasNode(to)) {
			return undefined;
		}

		const edges = this.graph.edges(from, to);
		if (edges.length > 1) {
			throw new Error("Unexpected edge count: " + edges.length);
		}
		if (edges.length > 0) {
			return edges[0];
		}
		return undefined;
	}

	getLinkData(from: string, to: string): EdgeData | undefined {
		const edgeId = this.getLink(from, to);
		if (edgeId) return this.graph.getEdgeAttributes(edgeId);
		return undefined;
	}

	hasLink(from: string, to: string): boolean {
		return this.graph.hasEdge(from, to);
	}
	hasNode(node: string): boolean {
		return this.graph.hasNode(node);
	}

	getPath(
		from: string,
		to: string,
		options?: { unweighted?: boolean } | { block?: string }
	): unweighted.ShortestPath | dijkstra.BidirectionalDijstraResult {
		try {
			let getEdgeWeight:
				| keyof EdgeData
				| MinimalEdgeMapper<number, EdgeData> = (edge) =>
				this.graph.getEdgeAttribute(edge, "weight");
			const blockId = (options as { block?: string })?.block;
			if (blockId) {
				const neighBourEdges = new Set(
					this.graph
						.inboundNeighbors(blockId)
						.map((x) => this.graph.edges(x, blockId))
						.flat()
				);
				getEdgeWeight = (edge) => {
					if (neighBourEdges.has(edge)) {
						return Number.MAX_SAFE_INTEGER;
					}
					return this.graph.getEdgeAttribute(edge, "weight");
				};
			}

			// TODO catching for network changes and resuse last result
			const path =
				((options as { unweighted?: boolean })?.unweighted
					? unweighted.bidirectional(this.graph, from, to)
					: dijkstra.bidirectional(this.graph, from, to, getEdgeWeight)) || [];
			if (path?.length > 0 && path[0] !== from) {
				path.reverse();
			}

			if (blockId) {
				if (path.includes(blockId)) {
					return []; // Path does not exist, as we go through a blocked node with inifite weight
				}
			}

			return path as any; // TODO fix types
		} catch (error) {
			return [];
		}
	}
	clear() {
		this.graph.clear();
	}
}
