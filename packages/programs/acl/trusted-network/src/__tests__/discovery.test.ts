import { LSession, waitForPeers } from "@dao-xyz/peerbit-test-utils";
it("_", () => {});
/* import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";
import { delay, waitFor } from "@dao-xyz/peerbit-time";
import { PermissionedEventStore } from "./test-store.js";
import { jest } from "@jest/globals";

describe(`discovery`, function () {
	jest.setTimeout(60 * 1000);
	jest.retryTimes(1);
	let session1: LSession, session2: LSession;
	let client1: Peerbit, client2: Peerbit, client3: Peerbit;

	beforeAll(async () => {
		session1 = await LSession.connected(2);
		session2 = await LSession.connected(1);
	});

	afterAll(async () => {
		await session1.stop();
		await session2.stop();
	});

	beforeEach(async () => {
		client1 = await Peerbit.create(session1.peers[0], {
			localNetwork: true,
		});
		client2 = await Peerbit.create(session1.peers[1], {
			localNetwork: true,
		});
		client3 = await Peerbit.create(session2.peers[0], {
			localNetwork: true,
		});
	});

	afterEach(async () => {
		if (client1) await client1.stop();

		if (client2) await client2.stop();

		if (client3) await client3.stop();
	});

	it("will connect to network with swarm exchange", async () => {
		const program = await client1.open(
			new PermissionedEventStore({
				network: new TrustedNetwork({
					id: "network-tests",
					rootTrust: client1.identity.publicKey,
				}),
			})
		);
		await client1.join(program);

		// trust client 2
		await program.network.add(client2.id); // we have to trust peer because else other party will not exchange heads
		await program.network.add(client2.identity.publicKey); // will have to trust identity because else this can t add more idenetities

		// trust client 3
		await program.network.add(client3.id); // we have to trust peer because else other party will not exchange heads
		await program.network.add(client3.identity.publicKey); // will have to trust identity because else this can t add more idenetities
		await waitFor(() => program.network.trustGraph.index.size === 5);

		await client2.open(program.address!);

		// Connect client 1 with 3, but try to connect 2 to 3 by swarm messages
		await session1.peers[0].peerStore.addressBook.set(
			session2.peers[0].peerId,
			session2.peers[0].getMultiaddrs()
		);
		await session1.peers[0].dial(session2.peers[0].peerId);
		await waitForPeers(
			session2.peers[0],
			session1.peers[0]
		);
		await client3.open(program.address!);

		expect(client3.libp2p.peerStore.has(client1.id)).toBeTrue();
		expect(client3.libp2p.peerStore.has(client1.id)).toBeTrue();
	});
});
 */
