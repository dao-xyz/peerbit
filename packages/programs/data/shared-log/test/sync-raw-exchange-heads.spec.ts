import { keys } from "@libp2p/crypto";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { v4 as uuid } from "uuid";
import {
	ExchangeHeadsMessage,
	RawExchangeHeadsMessage,
} from "../src/exchange-heads.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/event-store.js";

describe("raw exchange-head sync", () => {
	it("uses raw exchange heads for capable simple-sync peers", async () => {
		const session = await TestSession.disconnected(2, [
			{
				libp2p: {
					privateKey: keys.privateKeyFromRaw(
						new Uint8Array([
							204, 234, 187, 172, 226, 232, 70, 175, 62, 211, 147, 91,
							229, 157, 168, 15, 45, 242, 144, 98, 75, 58, 208, 9,
							223, 143, 251, 52, 252, 159, 64, 83, 52, 197, 24, 246,
							24, 234, 141, 183, 151, 82, 53, 142, 57, 25, 148, 150,
							26, 209, 223, 22, 212, 40, 201, 6, 191, 72, 148, 82, 66,
							138, 199, 185,
						]),
					),
				},
			},
			{
				libp2p: {
					privateKey: keys.privateKeyFromRaw(
						new Uint8Array([
							237, 55, 205, 86, 40, 44, 73, 169, 196, 118, 36, 69, 214,
							122, 28, 157, 208, 163, 15, 215, 104, 193, 151, 177, 62,
							231, 253, 120, 122, 222, 174, 242, 120, 50, 165, 97, 8,
							235, 97, 186, 148, 251, 100, 168, 49, 10, 119, 71, 246,
							246, 174, 163, 198, 54, 224, 6, 174, 212, 159, 187, 2,
							137, 47, 192,
						]),
					),
				},
			},
		]);

		try {
			const setup = {
				domain: createReplicationDomainHash("u32"),
				type: "u32" as const,
				syncronizer: SimpleSyncronizer,
				name: "u32-simple-raw",
			};
			const store = new EventStore<string, any>();
			const openArgs = {
				replicate: { factor: 1 },
				setup,
				nativeGraph: true,
				sync: { rawExchangeHeads: true },
			};
			const db1 = await session.peers[0].open(store.clone(), {
				args: openArgs,
			});
			const db2 = await session.peers[1].open(store.clone(), {
				args: openArgs,
			});

			let exchangeHeads = 0;
			let rawExchangeHeads = 0;
			for (const db of [db1, db2]) {
				const send = db.log.rpc.send.bind(db.log.rpc);
				db.log.rpc.send = async (message, options) => {
					if (message instanceof RawExchangeHeadsMessage) {
						rawExchangeHeads += 1;
					} else if (message instanceof ExchangeHeadsMessage) {
						exchangeHeads += 1;
					}
					return send(message, options);
				};
			}

			const entryCount = 25;
			for (let i = 0; i < entryCount; i++) {
				await db1.add(uuid(), { meta: { next: [] } });
			}
			expect(db1.log.log.length).to.equal(entryCount);

			await waitForResolved(() =>
				session.peers[0].dial(session.peers[1].getMultiaddrs()),
			);
			await waitForResolved(
				() => {
					expect(db2.log.log.length).to.equal(entryCount);
				},
				{ timeout: 30_000, delayInterval: 100 },
			);

			expect(rawExchangeHeads).to.be.greaterThan(0);
			expect(exchangeHeads).to.equal(0);
		} finally {
			await session.stop();
		}
	});
});
