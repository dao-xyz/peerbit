import { Peerbit } from "../peer";
import { EventStore } from "./utils/stores/event-store";
import { KeyBlocks } from "./utils/stores/key-value-store";
import assert from "assert";
import mapSeries from "p-each-series";
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { waitFor } from "@dao-xyz/peerbit-time";

describe(`Automatic Replication`, function () {
	/*  let ipfsd1: Controller, ipfsd2: Controller, ipfsd3: Controller, ipfsd4: Controller, ipfs1: IPFS, ipfs2: IPFS, ipfs3: IPFS, ipfs4: IPFS */
	let client1: Peerbit, client2: Peerbit, client3: Peerbit, client4: Peerbit;
	let session: LSession;
	beforeEach(async () => {
		session = await LSession.connected(2);
		client1 = await Peerbit.create({ libp2p: session.peers[0] });
		client2 = await Peerbit.create({ libp2p: session.peers[1] });
	});

	afterEach(async () => {
		if (client1) {
			await client1.stop();
		}
		if (client2) {
			await client2.stop();
		}
		if (client3) {
			await client3.stop();
		}
		if (client4) {
			await client4.stop();
		}

		await session.stop();
	});

	it("starts replicating the database when peers connect", async () => {
		const entryCount = 33;
		const entryArr: number[] = [];

		const db1 = await client1.open(
			new EventStore<string>({ id: "replicate-automatically-tests" })
		);

		const db3 = await client1.open(
			new KeyBlocks<string>({
				id: "replicate-automatically-tests-kv",
			}),
			{
				onReplicationComplete: (_) => {
					fail();
				},
			}
		);

		// Create the entries in the first database
		for (let i = 0; i < entryCount; i++) {
			entryArr.push(i);
		}

		await mapSeries(entryArr, (i) => db1.add("hello" + i));

		// Open the second database
		let done = false;
		const db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.directblock,
				db1.address!
			))!,
			{
				onReplicationComplete: (_) => {
					// Listen for the 'replicated' events and check that all the entries
					// were replicated to the second database
					expect(db2.iterator({ limit: -1 }).collect().length).toEqual(
						entryCount
					);
					const result1 = db1.iterator({ limit: -1 }).collect();
					const result2 = db2.iterator({ limit: -1 }).collect();
					expect(result1.length).toEqual(result2.length);
					for (let i = 0; i < result1.length; i++) {
						assert(result1[i].equals(result2[i]));
					}
					done = true;
				},
			}
		);

		const _db4 = await client2.open<KeyBlocks<string>>(
			(await KeyBlocks.load<KeyBlocks<string>>(
				client2.libp2p.directblock,
				db3.address!
			))!,
			{
				onReplicationComplete: (_) => {
					fail();
				},
			}
		);

		await waitFor(() => done);
	});

	it("starts replicating the database when peers connect in write mode", async () => {
		const entryCount = 1;
		const entryArr: number[] = [];
		const db1 = await client1.open(
			new EventStore<string>({ id: "replicate-automatically-tests" }),
			{ replicate: false }
		);

		// Create the entries in the first database
		for (let i = 0; i < entryCount; i++) {
			entryArr.push(i);
		}

		await mapSeries(entryArr, (i) => db1.add("hello" + i));

		// Open the second database
		let done = false;
		const db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.directblock,
				db1.address!
			))!,
			{
				onReplicationComplete: (_) => {
					// Listen for the 'replicated' events and check that all the entries
					// were replicated to the second database
					expect(db2.iterator({ limit: -1 }).collect().length).toEqual(
						entryCount
					);
					const result1 = db1.iterator({ limit: -1 }).collect();
					const result2 = db2.iterator({ limit: -1 }).collect();
					expect(result1.length).toEqual(result2.length);
					for (let i = 0; i < result1.length; i++) {
						expect(result1[i].equals(result2[i])).toBeTrue();
					}
					done = true;
				},
			}
		);

		await waitFor(() => done);
	});
});
