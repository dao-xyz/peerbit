import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { Entry } from "@peerbit/log";
import { Program } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { delay } from "@peerbit/time";
import { TrustedNetwork } from "@peerbit/trusted-network";
import { expect } from "chai";
import { ClockService } from "../src/controller.js";

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

	async open(): Promise<void> {
		await this.clock.open({ maxTimeError });
	}
}

describe("clock", () => {
	let session: TestSession, responder: P, reader: P;
	beforeEach(async () => {
		session = await TestSession.connected(3);
		responder = new P({
			clock: new ClockService({
				trustedNetwork: new TrustedNetwork({
					rootTrust: session.peers[0].peerId,
				}),
			}),
		});

		await session.peers[0].open(responder);
		responder.clock.maxError = BigInt(maxTimeError * 1e6);

		reader = deserialize(serialize(responder), P);
		await session.peers[1].open(reader, { args: { replicate: false } });
		await reader.waitFor(session.peers[0].peerId);
	});
	afterEach(async () => {
		await reader.drop();
		await responder.drop();
		await session.stop();
	});

	it("signs and verifies", async () => {
		const entry = await Entry.create({
			data: new Uint8Array([1]),
			identity: reader.node.identity,
			store: session.peers[1].services.blocks,
			signers: [
				reader.node.identity.sign.bind(reader.node.identity),
				reader.clock.sign.bind(reader.clock),
			],
		});
		expect(
			await Promise.all(entry.signatures.map((x) => x.publicKey.hashcode())),
		).to.have.members(
			await Promise.all([
				reader.node.identity.publicKey.hashcode(),
				responder.node.identity.publicKey.hashcode(),
			]),
		);
		expect(await reader.clock.verify(entry)).to.be.true;
	});

	it("reject old entry", async () => {
		await expect(
			Entry.create({
				data: new Uint8Array([1]),
				identity: reader.node.identity,
				store: session.peers[1].services.blocks,
				signers: [
					async (data: Uint8Array) => reader.node.identity.sign(data),
					async (data: Uint8Array) => {
						await delay(maxTimeError + 1000);
						return reader.clock.sign(data);
					},
				],
			}),
		).rejectedWith("Recieved an entry with an invalid timestamp");
	});
});
