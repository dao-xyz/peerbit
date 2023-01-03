import type { PeerId } from '@libp2p/interface-peer-id'
import TTLCache from '@isaacs/ttlcache'
import { default as path, PathFinder } from 'ngraph.path';
import createGraph, { Graph } from 'ngraph.graph';
const lruKey = (a: string, b: string) => {
	if (a < b) {
		return a + b;
	}
	return b + a;
}

export class
	Routes {

	map: TTLCache<string, {
		a: string,
		b: string
	}>
	graph: Graph<PeerId, any>;
	private pathFinder: PathFinder<any>;
	private peerId: string;
	constructor(peerId: string, options?: { ttl: number }) {
		this.peerId = peerId;
		this.graph = createGraph()
		this.pathFinder = path.aGreedy(this.graph);
		this.map = new TTLCache({
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
		});
	}


	add(from: string, to: string) {
		const key = lruKey(from, to);
		if (this.map.has(key)) {
			this.map.setTTL(key)
		}
		else {
			this.map.set(key, { a: from, b: to })
			this.graph.addLink(from, to)
		}
	}

	delete(from: string, to: string) {
		this.map.delete(lruKey(from, to))
	}

	getPath(from: string, to: string) {
		try {
			const path = this.pathFinder.find(from, to)
			if (path?.length > 0 && path[0].id !== from) {
				path.reverse()
			}
			return path;
		} catch (error) {
			return [];
		}
	}
}