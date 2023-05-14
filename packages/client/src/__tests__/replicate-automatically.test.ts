import { LSession } from "@dao-xyz/peerbit-test-utils";
import { waitForAsync } from "@dao-xyz/peerbit-time";
import { ObserverType } from "@dao-xyz/peerbit-program";
import { Peerbit } from "../peer.js";
import { EventStore } from "./utils/stores/event-store";
import { KeyBlocks } from "./utils/stores/key-value-store";
import assert from "assert";
import mapSeries from "p-each-series";
import { randomBytes } from "@dao-xyz/peerbit-crypto";

describe(`Automatic Replication`, function () {
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
			new EventStore<string>({ id: randomBytes(32) })
		);

		const db3 = await client1.open(
			new KeyBlocks<string>({
				id: randomBytes(32),
			})
		);

		// Create the entries in the first database
		for (let i = 0; i < entryCount; i++) {
			entryArr.push(i);
		}

		await mapSeries(entryArr, (i) => db1.add("hello" + i));

		// Open the second database
		const db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.services.directblock,
				db1.address!
			))!
		);

		const db4 = await client2.open<KeyBlocks<string>>(
			(await KeyBlocks.load<KeyBlocks<string>>(
				client2.libp2p.services.directblock,
				db3.address!
			))!
		);

		await waitForAsync(
			async () =>
				(await db2.iterator({ limit: -1 })).collect().length === entryCount
		);
		const result1 = (await db1.iterator({ limit: -1 })).collect();
		const result2 = (await db2.iterator({ limit: -1 })).collect();
		expect(result1.length).toEqual(result2.length);
		for (let i = 0; i < result1.length; i++) {
			assert(result1[i].equals(result2[i]));
		}

		expect(db3.log.length).toEqual(0);
		expect(db4.log.length).toEqual(0);
	});

	it("starts replicating the database when peers connect in write mode", async () => {
		const entryCount = 1;
		const entryArr: number[] = [];
		const db1 = await client1.open(
			new EventStore<string>({ id: randomBytes(32) }),
			{ role: new ObserverType() }
		);

		// Create the entries in the first database
		for (let i = 0; i < entryCount; i++) {
			entryArr.push(i);
		}

		await mapSeries(entryArr, (i) => db1.add("hello" + i));

		// Open the second database
		const db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.services.directblock,
				db1.address!
			))!
		);

		await waitForAsync(
			async () =>
				(await db2.iterator({ limit: -1 })).collect().length === entryCount
		);
		const result1 = (await db1.iterator({ limit: -1 })).collect();
		const result2 = (await db2.iterator({ limit: -1 })).collect();
		expect(result1.length).toEqual(result2.length);
		for (let i = 0; i < result1.length; i++) {
			expect(result1[i].equals(result2[i])).toBeTrue();
		}
	});
});
