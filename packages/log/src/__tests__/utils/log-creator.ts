import { KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { Log } from "../../log.js";
import { Timestamp } from "../../clock.js";
import { BlockStore } from "@dao-xyz/libp2p-direct-block";

export class LogCreator {
	static async createLogWithSixteenEntries(
		store: BlockStore,
		signKeys: KeyWithMeta<Ed25519Keypair>[]
	) {
		const expectedData = [
			"entryA1",
			"entryB1",
			"entryA2",
			"entryB2",
			"entryA3",
			"entryB3",
			"entryA4",
			"entryB4",
			"entryA5",
			"entryB5",
			"entryA6",
			"entryC0",
			"entryA7",
			"entryA8",
			"entryA9",
			"entryA10",
		];

		const create = async (): Promise<Log<string>> => {
			const logA = new Log<string>(
				store,
				{
					...signKeys[0].keypair,
					sign: (data) => signKeys[0].keypair.sign(data),
				},
				{ logId: "X" }
			);
			const logB = new Log<string>(
				store,
				{
					...signKeys[1].keypair,
					sign: (data) => signKeys[1].keypair.sign(data),
				},
				{ logId: "X" }
			);
			const log3 = new Log<string>(
				store,
				{
					...signKeys[2].keypair,
					sign: (data) => signKeys[2].keypair.sign(data),
				},
				{ logId: "X" }
			);
			const log = new Log<string>(
				store,
				{
					...signKeys[3].keypair,
					sign: (data) => signKeys[3].keypair.sign(data),
				},
				{ logId: "X" }
			);

			for (let i = 1; i <= 5; i++) {
				await logA.append("entryA" + i);
				await logB.append("entryB" + i);
			}

			await log3.join(logA);
			await log3.join(logB);
			for (let i = 6; i <= 10; i++) {
				await logA.append("entryA" + i);
			}
			await log.join(log3);
			await log.append("entryC0", {
				timestamp: new Timestamp({
					wallTime: logA.toArray()[5].metadata.clock.timestamp.wallTime,
					logical: logA.toArray()[5].metadata.clock.timestamp.logical + 1,
				}),
			});
			await log.join(logA);
			expect(log.toArray().map((h) => h.payload.getValue())).toStrictEqual(
				expectedData
			);
			return log;
		};

		const log = await create();
		return { log: log, expectedData: expectedData, json: log.toJSON() };
	}

	static async createLogWithTwoHundredEntries(
		store: BlockStore,
		signKeys: KeyWithMeta<Ed25519Keypair>[]
	) {
		const amount = 100;

		const expectedData: string[] = [];

		const create = async (): Promise<Log<string>> => {
			const logA = new Log<string>(
				store,
				{
					...signKeys[0].keypair,
					sign: (data) => signKeys[0].keypair.sign(data),
				},
				{ logId: "X" }
			);
			const logB = new Log<string>(
				store,
				{
					...signKeys[1].keypair,
					sign: (data) => signKeys[1].keypair.sign(data),
				},
				{ logId: "X" }
			);
			for (let i = 1; i <= amount; i++) {
				await logA.append("entryA" + i);
				await logB.join(logA);
				await logB.append("entryB" + i);
				await logA.join(logB);
				expectedData.push("entryA" + i);
				expectedData.push("entryB" + i);
			}
			return logA;
		};

		const log = await create();
		return { log: log, expectedData: expectedData };
	}
}
