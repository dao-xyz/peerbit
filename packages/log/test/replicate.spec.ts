import { TestSession } from "@peerbit/test-utils";
import { randomBytes } from "@peerbit/crypto";
import { Log } from "../src/log.js";
import { Entry } from "../src/entry.js";
import { deserialize, serialize } from "@dao-xyz/borsh";
import { signKey, signKey2 } from "./fixtures/privateKey.js";
import { PubSubData } from "@peerbit/pubsub-interface";
import { JSON_ENCODING } from "./utils/encoding.js";
import { waitForResolved } from "@peerbit/time";
import { expect } from 'chai';

import { field, vec } from "@dao-xyz/borsh";

export class StringArray {

	@field({ type: vec("string") })
	arr: string[];

	constructor(properties: { arr: string[] }) {
		this.arr = properties.arr;
	}
}


describe("replication", function () {
	let session: TestSession;

	before(async () => {
		session = await TestSession.connected(2);
	});

	after(async () => {
		await session.stop();
	});

	describe("replicates logs deterministically", function () {
		const amount = 10 + 1;
		const channel = "XXX";
		const logId = randomBytes(32);

		let log1: Log<string>,
			log2: Log<string>,
			input1: Log<string>,
			input2: Log<string>;
		const buffer1: Uint8Array[] = [];
		const buffer2: Uint8Array[] = [];
		let processing = 0;

		const handleMessage = async (message: PubSubData, topic: string) => {
			if (!message.topics.includes(topic)) {
				return;
			}
			buffer1.push(message.data);
			processing++;
			await log1.join(deserialize(message.data, StringArray).arr);
			processing--;
		};

		const handleMessage2 = async (message: PubSubData, topic: string) => {
			if (!message.topics.includes(topic)) {
				return;
			}
			buffer2.push(message.data);
			processing++;
			await log2.join(deserialize(message.data, StringArray).arr);
			processing--;
		};

		beforeEach(async () => {
			log1 = new Log({ id: logId });
			await log1.open(session.peers[0].services.blocks, signKey),
				{ encoding: JSON_ENCODING };
			log2 = new Log({ id: logId });
			await log2.open(session.peers[1].services.blocks, signKey2, {
				encoding: JSON_ENCODING
			});

			input1 = new Log({ id: logId });
			await input1.open(session.peers[0].services.blocks, signKey, {
				encoding: JSON_ENCODING
			});
			input2 = new Log({ id: logId });
			await input2.open(session.peers[1].services.blocks, signKey2, {
				encoding: JSON_ENCODING
			});
			await session.peers[0].services.pubsub.subscribe(channel);
			await session.peers[1].services.pubsub.subscribe(channel);

			await waitForResolved(async () =>
				expect(
					(await session.peers[1].services.pubsub.getSubscribers(channel))
						?.length
				).equal(2)
			);
			await waitForResolved(async () =>
				expect(
					(await session.peers[1].services.pubsub.getSubscribers(channel))
						?.length
				).equal(2)
			);

			await session.peers[0].services.pubsub.addEventListener("data", (evt) => {
				handleMessage(evt.detail.data, channel);
			});
			await session.peers[1].services.pubsub.addEventListener("data", (evt) => {
				handleMessage2(evt.detail.data, channel);
			});
		});

		afterEach(async () => {
			await session.peers[0].services.pubsub.unsubscribe(channel);
			await session.peers[1].services.pubsub.unsubscribe(channel);
		});
		// TODO why is this test doing a lot of unchaught rejections? (Reproduce in VSCODE tick `Uncaught exceptions`)
		it("replicates logs", async () => {
			await session.peers[0].services.pubsub.waitFor(session.peers[1].peerId);

			let prev1: Entry<any> = undefined as any;
			let prev2: Entry<any> = undefined as any;
			for (let i = 1; i <= amount; i++) {
				prev1 = (
					await input1.append("A" + i, {
						meta: {
							next: prev1 ? [prev1] : undefined
						}
					})
				).entry;
				prev2 = (
					await input2.append("B" + i, {
						meta: {
							next: prev2 ? [prev2] : undefined
						}
					})
				).entry;
				const hashes1 = await input1.getHeads().all();
				const hashes2 = await input2.getHeads().all();
				await session.peers[0].services.pubsub.publish(
					Buffer.from(
						serialize(new StringArray({ arr: hashes1.map((x) => x.hash) }))
					),
					{
						topics: [channel]
					}
				);
				await session.peers[1].services.pubsub.publish(
					Buffer.from(
						serialize(new StringArray({ arr: hashes2.map((x) => x.hash) }))
					),
					{
						topics: [channel]
					}
				);
			}

			const whileProcessingMessages = (timeoutMs: number) => {
				return new Promise<void>((resolve, reject) => {
					const timeout = setTimeout(() => {
						reject(new Error("Timeout"));
					}, timeoutMs);
					const timer = setInterval(() => {
						if (
							buffer1.length + buffer2.length === amount * 2 &&
							processing === 0
						) {
							clearInterval(timer);
							clearTimeout(timeout);
							resolve();
						}
					}, 200);
				});
			};

			await whileProcessingMessages(10 * 1000);

			const result = new Log<string>({ id: logId });
			await result.open(session.peers[0].services.blocks, signKey, {
				encoding: JSON_ENCODING
			});

			await result.join(log1);
			await result.join(log2);

			expect(buffer1.length).equal(amount);
			expect(buffer2.length).equal(amount);
			expect(result.length).equal(amount * 2);
			expect(log1.length).equal(amount);
			expect(log2.length).equal(amount);
			expect(
				await Promise.all(
					[0, 1, 2, 3, 9, 10].map(async (i) =>
						(await result.toArray())[i].payload.getValue()
					)
				)
			).to.deep.equal(["A1", "B1", "A2", "B2", "B5", "A6"]);
		});
	});
});
