
import type { PeerId } from "@libp2p/interface-peer-id";
import type { KeyChain } from "@libp2p/interface-keychain";
import { Keychain as CKeychain, Libp2pKeychain } from '@peerbit/crypto'
import { Cache } from '@peerbit/cache'
export interface KeychainComponents {
	peerId: PeerId;
	keychain: KeyChain
}


export class Keychain {

	keychain: CKeychain
	constructor(
		readonly components: KeychainComponents
	) {
		this.keychain = new Libp2pKeychain(this.components.keychain, {
			cache: new Cache({ max: 1000 }),
		});
	}

	async start() {

	}
	async stop() {

	}
}