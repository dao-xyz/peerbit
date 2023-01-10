import { PublicSignKey } from '@dao-xyz/peerbit-crypto'
import type { PeerId } from '@libp2p/interface-peer-id'
import { peerIdFromString } from '@libp2p/peer-id'

/**
 * We can't use PeerIds as map keys because map keys are
 * compared using same-value-zero equality, so this is just
 * a map that stringifies the PeerIds before storing them.
 *
 * PeerIds cache stringified versions of themselves so this
 * should be a cheap operation.
 */
export class PeerMap<T> {
	private readonly map: Map<string, T>

	constructor() {
		this.map = new Map()

	}

	clear(): void {
		this.map.clear()
	}

	delete(peer): void {
		this.map.delete(peer)
	}



	forEach(fn: (value: T, key: string, map: PeerMap<T>) => void): void {
		this.map.forEach((value, key) => {
			fn(value, key, this)
		})
	}

	get(peer: string): T | undefined {
		return this.map.get(peer)
	}

	has(peer: | string): boolean {
		return this.map.has(peer)
	}

	set(peer: string, value: T): void {
		this.map.set(peer, value)
	}

	values(): IterableIterator<T> {
		return this.map.values()
	}

	get size(): number {
		return this.map.size
	}
	get length(): number {
		return this.size;
	}
}
