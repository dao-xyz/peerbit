import { yamux } from "@chainsafe/libp2p-yamux";
import { DirectBlock } from "@peerbit/blocks";
import { DefaultKeychain } from "@peerbit/keychain";
import { TestSession as SSession } from "@peerbit/libp2p-test-utils";
import { type ProgramClient } from "@peerbit/program";
import { DirectSub } from "@peerbit/pubsub";
import {
	type DirectStream,
	waitForNeighbour as waitForPeersStreams,
} from "@peerbit/stream";
import { type Libp2pOptions } from "libp2p";
import {
	type Libp2pCreateOptions,
	type Libp2pCreateOptionsWithServices,
	type Libp2pExtendServices,
} from "peerbit";
import { Peerbit } from "peerbit";

export type LibP2POptions = Libp2pOptions<Libp2pExtendServices>;

type CreateOptions = { libp2p?: Libp2pCreateOptions; directory?: string };
export class TestSession {
	private session: SSession<Libp2pExtendServices>;
	private _peers: Peerbit[];
	constructor(session: SSession<Libp2pExtendServices>, peers: Peerbit[]) {
		this.session = session;
		this._peers = peers;
	}

	public get peers(): ProgramClient[] {
		return this._peers;
	}

	async connect(groups?: ProgramClient[][]) {
		await this.session.connect(groups?.map((x) => x.map((y) => y)));
		return;
	}
	async stop() {
		await Promise.all(this._peers.map((x) => x.stop()));
		await Promise.all(this._peers.map((x) => x.libp2p.stop())); // beacuse we initialize libp2p externally, we have to close externally
	}

	static async connected(n: number, options?: CreateOptions | CreateOptions[]) {
		const session = await TestSession.disconnected(n, options);
		await session.connect();
		// TODO types
		await waitForPeersStreams(
			...session.peers.map(
				(x) => x.services.blocks as any as DirectStream<any>,
			),
		);
		return session;
	}

	static async disconnected(
		n: number,
		options?: CreateOptions | CreateOptions[],
	) {
		const m = (o?: CreateOptions): Libp2pCreateOptionsWithServices => {
			return {
				...o?.libp2p,
				services: {
					blocks: (c: any) => new DirectBlock(c),
					pubsub: (c: any) => new DirectSub(c, { canRelayMessage: true }),
					keychain: () => new DefaultKeychain(),
					...o?.libp2p?.services,
				} as any, /// TODO types
				streamMuxers: [yamux()],
				connectionMonitor: {
					enabled: false,
				},
			};
		};
		let optionsWithServices:
			| Libp2pCreateOptionsWithServices
			| Libp2pCreateOptionsWithServices[] = Array.isArray(options)
			? options.map(m)
			: m(options);
		const session = await SSession.disconnected(n, optionsWithServices);
		return new TestSession(
			session,
			(await Promise.all(
				session.peers.map((x, ix) =>
					Array.isArray(options)
						? Peerbit.create({ libp2p: x, directory: options[ix]?.directory })
						: Peerbit.create({ libp2p: x, directory: options?.directory }),
				),
			)) as Peerbit[],
		);
	}
}
