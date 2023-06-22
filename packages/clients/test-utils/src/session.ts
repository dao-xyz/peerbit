import {
	LSession as SSession,
	LibP2POptions as SLibP2POptions,
} from "@peerbit/libp2p-test-utils";
import {
	DirectStream,
	waitForPeers as waitForPeersStreams,
} from "@peerbit/stream";
import {
	Libp2pCreateOptions,
	Libp2pExtendServices,
	Libp2pCreateOptionsWithServices,
} from "peerbit";
import { DirectBlock } from "@peerbit/blocks";
import { DirectSub } from "@peerbit/pubsub";
import { Peerbit } from "peerbit";
import { Peerbit as IPeerbit } from "@peerbit/interface";

export type LibP2POptions = SLibP2POptions<Libp2pExtendServices>;

export class LSession {
	private session: SSession<Libp2pExtendServices>;
	private _peers: Peerbit[];
	constructor(session: SSession<Libp2pExtendServices>, peers: Peerbit[]) {
		this.session = session;
		this._peers = peers;
	}

	public get peers(): IPeerbit[] {
		return this._peers;
	}

	async connect(groups?: IPeerbit[][]) {
		await this.session.connect(groups?.map((x) => x.map((y) => y)));
		return;
	}
	async stop() {
		await Promise.all(this._peers.map((x) => x.stop()));
	}

	static async connected(
		n: number,
		options?: { libp2p?: Libp2pCreateOptions; directory?: string }
	) {
		const session = await LSession.disconnected(n, options);
		await session.connect();
		// TODO types
		await waitForPeersStreams(
			...session.peers.map((x) => x.services.blocks as any as DirectStream<any>)
		);
		return session;
	}

	static async disconnected(
		n: number,
		options?: { libp2p?: Libp2pCreateOptions; directory?: string }
	) {
		let optionsWithServices: Libp2pCreateOptionsWithServices = {
			...options?.libp2p,
			services: {
				blocks: (c) => new DirectBlock(c),
				pubsub: (c) => new DirectSub(c, { canRelayMessage: true }),
				...options?.libp2p?.services,
			},
		};
		const session = await SSession.disconnected(n, optionsWithServices);
		return new LSession(
			session,
			await Promise.all(
				session.peers.map((x) =>
					Peerbit.create({ libp2p: x, directory: options?.directory })
				)
			)
		);
	}
}
