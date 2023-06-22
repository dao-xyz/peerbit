import { delay } from "@peerbit/time";
import { LSession } from "@peerbit/test-utils";
import { Entry } from "@peerbit/log";
import { Program } from "@peerbit/program";
import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { ClockService } from "../controller";
import { TrustedNetwork } from "@peerbit/trusted-network";
import { waitForSubscribers } from "@peerbit/pubsub";
import { Observer, Replicator } from "@peerbit/shared-log";

const maxTimeError = 3000;
@variant("clock-test")
class P extends Program {
	@field({ type: ClockService })
	clock: ClockService;

	constructor(properties?: { clock: ClockService }) {
		super();
		if (properties) {
			this.clock = properties.clock;
		}
	}

	async setup(): Promise<void> {
		await this.clock.setup({ maxTimeError });
	}
}

describe("clock", () => {
	let session: LSession, responder: P, reader: P;
	beforeEach(async () => {
		session = await LSession.connected(3);
		responder = new P({
			clock: new ClockService({
				trustedNetwork: new TrustedNetwork({
					rootTrust: session.peers[0].peerId,
				}),
			}),
		});
		await reader.clock.setup({
			role: new Replicator(),
		});
		await responder.open(session.peers[0]);
		responder.clock._maxError = BigInt(maxTimeError * 1e6);

		reader = deserialize(serialize(responder), P);
		await reader.clock.setup({
			role: new Observer(),
		});
		await reader.open(session.peers[1], {});
		await reader.waitFor(session.peers[0].peerId);
	});
	afterEach(async () => {
		await reader.drop();
		await responder.drop();
		await session.stop();
	});

	it("signs and verifies", async () => {
		const entry = await Entry.create({
			data: "hello world",
			identity: reader.node.identity,
			store: session.peers[1].services.blocks,
			signers: [
				reader.node.identity.sign.bind(reader.node.identity),
				reader.clock.sign.bind(reader.clock),
			],
		});
		expect(
			await Promise.all(entry.signatures.map((x) => x.publicKey.hashcode()))
		).toContainAllValues(
			await Promise.all([
				reader.node.identity.publicKey.hashcode(),
				responder.node.identity.publicKey.hashcode(),
			])
		);
		expect(await reader.clock.verify(entry)).toBeTrue();
	});

	it("reject old entry", async () => {
		await expect(
			Entry.create({
				data: "hello world",
				identity: reader.node.identity,
				store: session.peers[1].services.blocks,
				signers: [
					async (data: Uint8Array) => reader.node.identity.sign(data),
					async (data: Uint8Array) => {
						await delay(maxTimeError + 1000);
						return reader.clock.sign(data);
					},
				],
			})
		).rejects.toThrowError(
			new Error("Recieved an entry with an invalid timestamp")
		);
	});
});
