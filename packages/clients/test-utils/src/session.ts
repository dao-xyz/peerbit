import { yamux } from "@chainsafe/libp2p-yamux";
import { DirectBlock } from "@peerbit/blocks";
import { keychain } from "@peerbit/keychain";
import {
	listenFast,
	TestSession as SSession,
	transportsFast,
} from "@peerbit/libp2p-test-utils";
import { type ProgramClient } from "@peerbit/program";
import { DirectSub } from "@peerbit/pubsub";
import {
	type DirectStream,
	waitForNeighbour as waitForPeersStreams,
} from "@peerbit/stream";
import { type Libp2pOptions } from "libp2p";
import path from "path";
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
		await Promise.all(this._peers.map((x) => x.libp2p.stop())); // beacuse we initialize libp2p externally (potentially), we have to close externally
	}

	/**
	 * Create a "mock-ish" session intended for fast and stable Node.js tests.
	 *
	 * Uses TCP-only transport (no WebRTC/WebSockets/circuit-relay) and disables
	 * the libp2p relay service by default.
	 */
	static async connectedMock(n: number, options?: CreateOptions | CreateOptions[]) {
		const session = await TestSession.disconnectedMock(n, options);
		await session.connect();
		// TODO types
		await waitForPeersStreams(
			...session.peers.map(
				(x) => x.services.blocks as any as DirectStream<any>,
			),
		);
		return session;
	}

	static async disconnectedMock(
		n: number,
		options?: CreateOptions | CreateOptions[],
	) {
		const applyMockDefaults = (o?: CreateOptions): CreateOptions | undefined => {
			if (!o) {
				return {
					libp2p: {
						transports: transportsFast(),
						addresses: { listen: listenFast() },
						services: { relay: null },
					} as any,
				};
			}

			return {
				...o,
				libp2p: {
					...(o.libp2p ?? {}),
					transports: o.libp2p?.transports ?? transportsFast(),
					addresses: {
						...(o.libp2p?.addresses ?? {}),
						listen: o.libp2p?.addresses?.listen ?? listenFast(),
					},
					services: {
						...(o.libp2p?.services ?? {}),
						relay: o.libp2p?.services?.relay ?? null,
					},
				} as any,
			};
		};

		const optionsWithMockDefaults = Array.isArray(options)
			? options.map(applyMockDefaults)
			: applyMockDefaults(options);

		return TestSession.disconnected(n, optionsWithMockDefaults as any);
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
		const useMockSession =
			process.env.PEERBIT_TEST_SESSION === "mock" ||
			process.env.PEERBIT_TEST_SESSION === "fast" ||
			process.env.PEERBIT_TEST_SESSION === "tcp";

		const m = (o?: CreateOptions): Libp2pCreateOptionsWithServices => {
			const blocksDirectory = o?.directory
				? path.join(o.directory, "/blocks").toString()
				: undefined;

			const libp2pOptions: Libp2pCreateOptions = {
				...(o?.libp2p ?? {}),
			};

			if (useMockSession) {
				libp2pOptions.transports = libp2pOptions.transports ?? transportsFast();
				libp2pOptions.addresses = {
					...(libp2pOptions.addresses ?? {}),
					listen: libp2pOptions.addresses?.listen ?? listenFast(),
				};
				libp2pOptions.services = {
					...(libp2pOptions.services ?? {}),
					relay: libp2pOptions.services?.relay ?? null,
				};
			}

			return {
				...libp2pOptions,
				services: {
					blocks: (c: any) =>
						new DirectBlock(c, {
							directory: blocksDirectory,
						}),
					pubsub: (c: any) => new DirectSub(c, { canRelayMessage: true }),
					keychain: keychain(),
					...libp2pOptions.services,
				} as any, /// TODO types
				streamMuxers: [yamux()],
				connectionMonitor: {
					enabled: false,
				},
				start: false, /// make Peerbit.create to start the client instead, this allows also so that Peerbit will terminate the client
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
