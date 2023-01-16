import type { PeerId } from "@libp2p/interface-peer-id";
/* import TTLCache from '@isaacs/ttlcache'
 */
import { default as path, PathFinder } from "ngraph.path";
import createGraph, { Graph } from "ngraph.graph";
/* const lruKey = (a: string, b: string) => {
	if (a < b) {
		return a + b;
	}
	return b + a;
} */

export class Routes {
    /* map: Map<string, {
		a: string,
		b: string
	}> */
    graph: Graph<PeerId, any>;
    private pathFinder: PathFinder<any>;
    private peerId: string;
    constructor(
        peerId: string,
        options?: {
            /* ttl: number */
        }
    ) {
        this.peerId = peerId;
        this.graph = createGraph();
        this.pathFinder = path.aGreedy(this.graph);
        /* this.map = new TTLCache({
			ttl: options?.ttl || 60 * 1000, dispose: (v, k, event) => {
				if (event !== 'delete' && (v.a === this.peerId || v.b === this.peerId)) {
					this.map.set(k, v) // re-add
					return;
				}
				const link = this.graph.getLink(v.a, v.b);
				if (link) {
					this.graph.removeLink(link)
				}
			}
		}); */
        /* this.map = new Map() */
    }

    add(from: string, to: string) {
        if (from > to) {
            const temp = from;
            from = to;
            to = temp;
        }
        /* const key = lruKey(from, to);
		if (this.map.has(key)) {
			// this.map.setTTL(key)
		}
		else {
			this.map.set(key, { a: from, b: to })
		} */

        this.graph.addLink(from, to);
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
    deleteLink(from: string, to: string, keepRoutesTo: string = this.peerId) {
        const link = this.getLink(from, to);
        if (link) {
            this.graph.removeLink(link);

            const disconnectedNodes: string[] = [];
            if (this.getPath(keepRoutesTo, to).length === 0) {
                disconnectedNodes.push(to);
            }
            if (this.getPath(keepRoutesTo, from).length === 0) {
                disconnectedNodes.push(from);
            }

            // remove subgraphs that are now disconnected from me
            for (const disconnected of disconnectedNodes) {
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
                            const toId =
                                node.id === link.toId ? link.fromId : link.toId;
                            if (visited.has(toId)) {
                                continue;
                            }

                            const next = this.graph.getNode(toId);
                            if (next) {
                                stack.push(next);
                            }
                        }
                    }
                    this.graph.removeNode(node.id);
                }
            }
        }
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
        /* this.map.clear(); */
        this.graph.clear();
    }
}
