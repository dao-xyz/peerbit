import {
	LSession as SSession,
	LibP2POptions as SLibP2POptions,
} from "@dao-xyz/libp2p-test-utils";
import {
	CreateOptionsWithServices,
	Libp2pExtended,
	Libp2pExtendServices,
} from "@dao-xyz/peerbit-libp2p";
import { waitForPeers as waitForPeersStreams } from "@dao-xyz/libp2p-direct-stream";
import { CreateOptions } from "@dao-xyz/peerbit-libp2p";
import { DirectBlock } from "@dao-xyz/libp2p-direct-block";
import { DirectSub } from "@dao-xyz/libp2p-direct-sub";

export type LibP2POptions = SLibP2POptions<Libp2pExtendServices>;

export class LSession {
	private session: SSession<Libp2pExtendServices>;
	constructor(session: SSession<Libp2pExtendServices>) {
		this.session = session;
	}

	public get peers(): Libp2pExtended[] {
		return this.session.peers;
	}

	async connect(groups?: Libp2pExtended[][]) {
		await this.session.connect(groups);
		return;
	}
	async stop() {
		return this.session.stop();
	}

	static async connected(n: number, options?: CreateOptions) {
		const session = await LSession.disconnected(n, options);
		await session.connect();
		await waitForPeersStreams(
			...session.peers.map((x) => x.services.directblock)
		);
		return session;
	}

	static async disconnected(n: number, options?: CreateOptions) {
		let optionsWithServices: CreateOptionsWithServices = {
			...options,
			services: {
				directblock: (c) => new DirectBlock(c),
				directsub: (c) => new DirectSub(c, { canRelayMessage: true }),
				...options?.services,
			},
		};
		const session = await SSession.disconnected(n, optionsWithServices);
		return new LSession(session);
	}
}
