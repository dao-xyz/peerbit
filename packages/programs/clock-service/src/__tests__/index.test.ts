import { delay } from "@dao-xyz/peerbit-time";
import { LSession, waitForPeers } from "@dao-xyz/peerbit-test-utils";
import { Ed25519Keypair, SignatureWithKey } from "@dao-xyz/peerbit-crypto";
import { Ed25519Identity, Entry } from "@dao-xyz/peerbit-log";
import { Program, ReplicatorType } from "@dao-xyz/peerbit-program";
import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { ClockService } from "../controller";
import { MemoryLevel } from "memory-level";
import { default as Cache } from "@dao-xyz/lazy-level";
import { v4 as uuid } from "uuid";
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";

const createIdentity = async () => {
	const ed = await Ed25519Keypair.create();
	return {
		publicKey: ed.publicKey,
		privateKey: ed.privateKey,
		sign: (data) => ed.sign(data),
	} as Ed25519Identity;
};

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
		const responderIdentity = await createIdentity();
		responder = new P({
			clock: new ClockService({
				trustedNetwork: new TrustedNetwork({
					rootTrust: responderIdentity.publicKey,
				}),
			}),
		});
		await responder.init(session.peers[0], responderIdentity, {
			role: new ReplicatorType(),
			replicators: () => [],
			store: {
				cacheId: "id",
				resolveCache: () => Promise.resolve(new Cache(new MemoryLevel())),
			},
		});

		responder.clock._maxError = BigInt(maxTimeError * 1e6);

		reader = deserialize(serialize(responder), P);
		await reader.init(session.peers[1], await createIdentity(), {
			store: {
				cacheId: "id",
				resolveCache: () => Promise.resolve(new Cache(new MemoryLevel())),
			} as any,
		} as any);
		const topic = responder.clock._remoteSigner.rpcTopic;
		if (!topic) {
			throw new Error("Expecting topic");
		}
		await waitForPeers(session.peers[1], [session.peers[0]], topic);
	});
	afterEach(async () => {
		await reader.drop();
		await responder.drop();
		await session.stop();
	});

	it("signs and verifies", async () => {
		const entry = await Entry.create({
			data: "hello world",
			identity: reader.identity,
			store: session.peers[1].directblock,
			signers: [
				reader.identity.sign.bind(reader.identity),
				reader.clock.sign.bind(reader.clock),
			],
		});
		expect(
			await Promise.all(entry.signatures.map((x) => x.publicKey.hashcode()))
		).toContainAllValues(
			await Promise.all([
				reader.identity.publicKey.hashcode(),
				responder.identity.publicKey.hashcode(),
			])
		);
		expect(await reader.clock.verify(entry)).toBeTrue();
	});

	it("reject old entry", async () => {
		await expect(
			Entry.create({
				data: "hello world",
				identity: reader.identity,
				store: session.peers[1].directblock,
				signers: [
					async (data: Uint8Array) => reader.identity.sign(data),
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
