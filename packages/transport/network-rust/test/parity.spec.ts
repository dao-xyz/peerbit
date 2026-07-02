import {
	Ed25519Keypair,
	Ed25519PublicKey,
	PreHash,
	Secp256k1Keypair,
	Secp256k1PublicKey,
	randomBytes,
} from "@peerbit/crypto";
import {
	ACK,
	AcknowledgeAnyWhere,
	AcknowledgeDelivery,
	AnyWhere,
	DataMessage,
	Goodbye,
	Hello,
	Message,
	MessageHeader,
	MultiAddrinfo,
	SilentDelivery,
	TracedDelivery,
} from "@peerbit/stream-interface";
import { expect } from "chai";
import { Uint8ArrayList } from "uint8arraylist";
import {
	NATIVE_WIRE_RECORD_WORDS,
	type NativeWireModule,
	NativeWireVerifyStatus,
	createNativeWire,
	readNativeWireFrameRecord,
} from "../src/index.js";

const toHex = (bytes: Uint8Array): string =>
	Array.from(bytes)
		.map((byte) => byte.toString(16).padStart(2, "0"))
		.join("");

const toFrame = (message: Message): Uint8Array => {
	const bytes = message.bytes();
	return bytes instanceof Uint8Array ? bytes : bytes.subarray();
};

const decodeTs = (frame: Uint8Array): Message =>
	Message.from(new Uint8ArrayList(frame));

/**
 * Mirror of the debug-JSON shape emitted by the Rust decoder
 * (`frame_to_debug_json` in src/wire.rs), built from a TS-decoded message.
 */
const toComparable = (message: Message): any => {
	const header = message.header;
	const mode = header.mode as any;
	let modeJson: any = null;
	if (mode instanceof SilentDelivery) {
		modeJson = { type: "silent", to: mode.to, redundancy: mode.redundancy };
	} else if (mode instanceof AcknowledgeDelivery) {
		modeJson = {
			type: "acknowledge",
			to: mode.to,
			redundancy: mode.redundancy,
			hops: mode.hops,
		};
	} else if (mode instanceof TracedDelivery) {
		modeJson = { type: "traced", trace: mode.trace };
	} else if (mode instanceof AcknowledgeAnyWhere) {
		modeJson = {
			type: "acknowledgeAnyWhere",
			redundancy: mode.redundancy,
			hops: mode.hops,
		};
	} else if (mode instanceof AnyWhere) {
		modeJson = { type: "anyWhere" };
	}
	const signatures = header.signatures
		? header.signatures.signatures.map((signature) => {
				const publicKey = signature.publicKey;
				let publicKeyType: string;
				let publicKeyBytes: Uint8Array;
				if (publicKey instanceof Ed25519PublicKey) {
					publicKeyType = "ed25519";
					publicKeyBytes = publicKey.publicKey;
				} else if (publicKey instanceof Secp256k1PublicKey) {
					publicKeyType = "secp256k1";
					publicKeyBytes = publicKey.publicKey;
				} else {
					throw new Error("Unexpected public key type");
				}
				return {
					signature: toHex(signature.signature),
					publicKeyType,
					publicKey: toHex(publicKeyBytes),
					prehash: signature.prehash,
				};
			})
		: null;
	const headerJson = {
		id: toHex(header.id),
		timestamp: header.timestamp.toString(),
		session: header.session.toString(),
		expires: header.expires.toString(),
		priority: header.priority ?? null,
		responsePriority: header.responsePriority ?? null,
		origin: header.origin ? header.origin.multiaddrs : null,
		mode: modeJson,
		signatures,
	};
	if (message instanceof DataMessage) {
		return {
			type: "data",
			header: headerJson,
			data: message.data != null ? toHex(message.data) : null,
		};
	}
	if (message instanceof ACK) {
		return {
			type: "ack",
			header: headerJson,
			messageIdToAcknowledge: toHex(message.messageIdToAcknowledge),
			seenCounter: message.seenCounter,
		};
	}
	if (message instanceof Hello) {
		return { type: "hello", header: headerJson, joined: message.joined };
	}
	if (message instanceof Goodbye) {
		return { type: "goodbye", header: headerJson, leaving: message.leaving };
	}
	throw new Error("Unexpected message type");
};

describe("wire parity", () => {
	let wire: NativeWireModule;
	let keypairA: Ed25519Keypair;
	let keypairB: Ed25519Keypair;
	let secpKeypair: Secp256k1Keypair;

	const signSha256 = (keypair: Ed25519Keypair) => (bytes: Uint8Array) =>
		keypair.sign(bytes, PreHash.SHA_256);

	const hashA = "peer-a-hashcode";
	const hashB = "peer-b-hashcode";
	const session = 123;

	/**
	 * TS-authored golden corpus: every top-level variant, every delivery
	 * mode, optional fields present/absent, empty/large payloads, multiple
	 * signatures. `expectVerified` refers to hot-path semantics
	 * (verify with expectSignatures = true).
	 */
	type CorpusEntry = {
		name: string;
		frame: Uint8Array;
		message: Message;
		expectVerified: boolean;
		expectNativeStatus: NativeWireVerifyStatus;
	};
	let corpus: CorpusEntry[];

	before(async () => {
		wire = await createNativeWire();
		keypairA = await Ed25519Keypair.create();
		keypairB = await Ed25519Keypair.create();
		secpKeypair = await Secp256k1Keypair.create();

		const entries: Omit<CorpusEntry, "frame">[] = [];

		entries.push({
			name: "data silent, small payload, 1 sha256 signature",
			message: await new DataMessage({
				header: new MessageHeader({
					session,
					mode: new SilentDelivery({ to: [hashA], redundancy: 1 }),
				}),
				data: new Uint8Array([1, 2, 3]),
			}).sign(signSha256(keypairA)),
			expectVerified: true,
			expectNativeStatus: NativeWireVerifyStatus.VERIFIED,
		});

		entries.push({
			name: "data acknowledge with hops, empty-but-present payload",
			message: await new DataMessage({
				header: new MessageHeader({
					session,
					mode: new AcknowledgeDelivery({
						to: [hashA, hashB],
						redundancy: 2,
						hops: [hashB],
					}),
					priority: 1,
					responsePriority: 3,
				}),
				data: new Uint8Array(0),
			}).sign(signSha256(keypairA)),
			expectVerified: true,
			expectNativeStatus: NativeWireVerifyStatus.VERIFIED,
		});

		entries.push({
			name: "data anyWhere, no payload, unsigned",
			message: new DataMessage({
				header: new MessageHeader({ session, mode: new AnyWhere() }),
			}),
			expectVerified: false,
			expectNativeStatus: NativeWireVerifyStatus.FAILED,
		});

		entries.push({
			name: "data acknowledgeAnyWhere, 512KB payload, 2 signatures",
			message: await (
				await new DataMessage({
					header: new MessageHeader({
						session,
						mode: new AcknowledgeAnyWhere({ redundancy: 2 }),
					}),
					data: new Uint8Array(512 * 1024).map((_, i) => i % 251),
				}).sign(signSha256(keypairA))
			).sign(signSha256(keypairB)),
			expectVerified: true,
			expectNativeStatus: NativeWireVerifyStatus.VERIFIED,
		});

		entries.push({
			name: "data without mode, explicit priority, unsigned",
			message: new DataMessage({
				header: new MessageHeader({
					session,
					mode: undefined as any,
					priority: 2,
				}),
				data: new Uint8Array([42]),
			}),
			expectVerified: false,
			expectNativeStatus: NativeWireVerifyStatus.FAILED,
		});

		const noPriority = new DataMessage({
			header: new MessageHeader({
				session,
				mode: new SilentDelivery({ to: [hashA], redundancy: 1 }),
			}),
			data: new Uint8Array([7]),
		});
		noPriority.header.priority = undefined;
		noPriority.header.responsePriority = undefined;
		entries.push({
			name: "data with absent priority options",
			message: await noPriority.sign(signSha256(keypairA)),
			expectVerified: true,
			expectNativeStatus: NativeWireVerifyStatus.VERIFIED,
		});

		entries.push({
			name: "ack traced with origin",
			message: await new ACK({
				messageIdToAcknowledge: randomBytes(32),
				seenCounter: 1,
				header: new MessageHeader({
					session,
					mode: new TracedDelivery([hashA, hashB]),
					origin: new MultiAddrinfo([
						"/ip4/127.0.0.1/tcp/4002",
						"/ip4/127.0.0.1/tcp/4003/ws",
					]),
				}),
			}).sign(signSha256(keypairB)),
			expectVerified: true,
			expectNativeStatus: NativeWireVerifyStatus.VERIFIED,
		});

		const hello = new Hello({ joined: [hashA] });
		hello.header = new MessageHeader({
			session,
			mode: new SilentDelivery({ to: [hashB], redundancy: 1 }),
		});
		await hello.sign(signSha256(keypairA));
		// second signature without prehash (PreHash.NONE)
		await hello.sign((bytes) => keypairB.sign(bytes, PreHash.NONE));
		entries.push({
			name: "hello with mixed prehash multi-signature",
			message: hello,
			expectVerified: true,
			expectNativeStatus: NativeWireVerifyStatus.VERIFIED,
		});

		entries.push({
			name: "goodbye silent",
			message: await new Goodbye({
				leaving: [hashA],
				header: new MessageHeader({
					session,
					mode: new SilentDelivery({ to: [hashB], redundancy: 2 }),
				}),
			}).sign(signSha256(keypairA)),
			expectVerified: true,
			expectNativeStatus: NativeWireVerifyStatus.VERIFIED,
		});

		entries.push({
			name: "expired header fails verification",
			message: await new DataMessage({
				header: new MessageHeader({
					session,
					mode: new SilentDelivery({ to: [hashA], redundancy: 1 }),
					expires: Date.now() - 1000,
				}),
				data: new Uint8Array([1]),
			}).sign(signSha256(keypairA)),
			expectVerified: false,
			expectNativeStatus: NativeWireVerifyStatus.FAILED,
		});

		entries.push({
			name: "secp256k1 signature is unsupported natively",
			message: await new DataMessage({
				header: new MessageHeader({
					session,
					mode: new SilentDelivery({ to: [hashA], redundancy: 1 }),
				}),
				data: new Uint8Array([1, 2]),
			}).sign((bytes) => secpKeypair.sign(bytes, PreHash.SHA_256)),
			expectVerified: true,
			expectNativeStatus: NativeWireVerifyStatus.UNSUPPORTED,
		});

		entries.push({
			name: "keccak prehash is unsupported natively",
			message: await new DataMessage({
				header: new MessageHeader({
					session,
					mode: new SilentDelivery({ to: [hashA], redundancy: 1 }),
				}),
				data: new Uint8Array([3, 4]),
			}).sign((bytes) => keypairA.sign(bytes, PreHash.ETH_KECCAK_256)),
			expectVerified: true,
			expectNativeStatus: NativeWireVerifyStatus.UNSUPPORTED,
		});

		corpus = entries.map((entry) => ({
			...entry,
			frame: toFrame(entry.message),
		}));
	});

	describe("ts to rust", () => {
		it("decodes every corpus frame to identical semantics", () => {
			for (const entry of corpus) {
				const rustJson = JSON.parse(wire.decodeFrameToJson(entry.frame));
				expect(rustJson, entry.name).to.deep.equal(
					toComparable(decodeTs(entry.frame)),
				);
			}
		});

		it("re-encodes every corpus frame byte-identically", () => {
			for (const entry of corpus) {
				expect(
					toHex(wire.reencodeFrame(entry.frame)),
					entry.name,
				).to.equal(toHex(entry.frame));
			}
		});

		it("computes the same signable bytes as getSignableBytes", () => {
			for (const entry of corpus) {
				expect(
					toHex(wire.signableBytes(entry.frame)),
					entry.name,
				).to.equal(toHex(decodeTs(entry.frame).getSignableBytes()));
			}
		});

		it("matches the ts verification outcome for the whole batch", async () => {
			const records = wire.decodeAndVerifyBatch(
				corpus.map((entry) => entry.frame),
				Date.now(),
			);
			expect(records.length).to.equal(
				corpus.length * NATIVE_WIRE_RECORD_WORDS,
			);
			for (let i = 0; i < corpus.length; i++) {
				const entry = corpus[i];
				const record = readNativeWireFrameRecord(records, i);
				expect(record.decodeOk, entry.name).to.equal(true);
				expect(record.verifyStatus, entry.name).to.equal(
					entry.expectNativeStatus,
				);
				const tsVerified = await decodeTs(entry.frame).verify(true);
				expect(tsVerified, entry.name).to.equal(entry.expectVerified);
				if (record.verifyStatus !== NativeWireVerifyStatus.UNSUPPORTED) {
					expect(
						record.verifyStatus === NativeWireVerifyStatus.VERIFIED,
						entry.name,
					).to.equal(tsVerified);
				}
			}
		});

		it("reports header fields and payload ranges", () => {
			const records = wire.decodeAndVerifyBatch(
				corpus.map((entry) => entry.frame),
				Date.now(),
			);
			for (let i = 0; i < corpus.length; i++) {
				const entry = corpus[i];
				const record = readNativeWireFrameRecord(records, i);
				const message = decodeTs(entry.frame);
				expect(record.priority, entry.name).to.equal(
					message.header.priority,
				);
				expect(record.signatureCount, entry.name).to.equal(
					message.header.signatures?.signatures.length ?? 0,
				);
				if (message instanceof DataMessage) {
					expect(record.variant, entry.name).to.equal(0);
					expect(record.hasData, entry.name).to.equal(
						message.data != null,
					);
					if (message.data != null) {
						expect(
							toHex(
								entry.frame.subarray(
									record.dataOffset,
									record.dataOffset + record.dataLength,
								),
							),
							entry.name,
						).to.equal(toHex(message.data));
					}
				}
			}
		});

		it("fails verification for a tampered signature", async () => {
			const message = await new DataMessage({
				header: new MessageHeader({
					session,
					mode: new SilentDelivery({ to: [hashA], redundancy: 1 }),
				}),
				data: new Uint8Array([9, 9, 9]),
			}).sign(signSha256(keypairA));
			message.header.signatures!.signatures[0].signature[0] ^= 0xff;
			const frame = toFrame(message);
			const record = readNativeWireFrameRecord(
				wire.decodeAndVerifyBatch([frame], Date.now()),
				0,
			);
			expect(record.verifyStatus).to.equal(NativeWireVerifyStatus.FAILED);
			expect(await decodeTs(frame).verify(true)).to.equal(false);
		});

		it("fails verification for a tampered signed payload", async () => {
			const message = await new DataMessage({
				header: new MessageHeader({
					session,
					mode: new SilentDelivery({ to: [hashA], redundancy: 1 }),
				}),
				data: new Uint8Array([9, 9, 9]),
			}).sign(signSha256(keypairA));
			const frame = toFrame(message);
			frame[frame.length - 1] ^= 0xff; // payload is signed
			const record = readNativeWireFrameRecord(
				wire.decodeAndVerifyBatch([frame], Date.now()),
				0,
			);
			expect(record.verifyStatus).to.equal(NativeWireVerifyStatus.FAILED);
			expect(await decodeTs(frame).verify(true)).to.equal(false);
		});

		it("keeps verification when the unsigned mode bytes are rewritten", async () => {
			// The delivery mode is excluded from the signable range (it is
			// mutated in transit), so a relay rewriting it must not break
			// signatures — in both implementations.
			const message = await new DataMessage({
				header: new MessageHeader({
					session,
					mode: new AcknowledgeDelivery({
						to: [hashA],
						redundancy: 1,
						hops: [],
					}),
				}),
				data: new Uint8Array([5, 5]),
			}).sign(signSha256(keypairA));
			const original = toFrame(message);
			(message.header.mode as AcknowledgeDelivery).hops = [hashB];
			const rewritten = toFrame(message);
			expect(toHex(rewritten)).to.not.equal(toHex(original));
			expect(toHex(wire.signableBytes(rewritten))).to.equal(
				toHex(wire.signableBytes(original)),
			);
			for (const frame of [original, rewritten]) {
				const record = readNativeWireFrameRecord(
					wire.decodeAndVerifyBatch([frame], Date.now()),
					0,
				);
				expect(record.verifyStatus).to.equal(
					NativeWireVerifyStatus.VERIFIED,
				);
				expect(await decodeTs(frame).verify(true)).to.equal(true);
			}
		});

		it("rejects garbage frames without failing the batch", () => {
			const good = corpus[0].frame;
			const records = wire.decodeAndVerifyBatch(
				[randomBytes(64), good, new Uint8Array(0)],
				Date.now(),
			);
			expect(readNativeWireFrameRecord(records, 0).decodeOk).to.equal(false);
			const goodRecord = readNativeWireFrameRecord(records, 1);
			expect(goodRecord.decodeOk).to.equal(true);
			expect(goodRecord.verifyStatus).to.equal(
				NativeWireVerifyStatus.VERIFIED,
			);
			expect(readNativeWireFrameRecord(records, 2).decodeOk).to.equal(false);
		});

		it("handles an empty batch", () => {
			expect(wire.decodeAndVerifyBatch([], Date.now()).length).to.equal(0);
		});
	});

	describe("rust to ts", () => {
		it("ts decodes, re-encodes and verifies the rust-authored corpus", async () => {
			const frames = wire.testCorpusFrames();
			expect(frames.length).to.equal(7);
			for (let i = 0; i < frames.length; i++) {
				const frame = frames[i];
				const message = decodeTs(frame);
				// semantic parity via the shared debug shape
				expect(
					JSON.parse(wire.decodeFrameToJson(frame)),
					`corpus frame ${i}`,
				).to.deep.equal(toComparable(message));
				// ts re-serialization is byte-identical
				expect(toHex(toFrame(message)), `corpus frame ${i}`).to.equal(
					toHex(frame),
				);
				// rust-made signatures verify through the ts path (frame 2 is
				// intentionally unsigned)
				expect(await message.verify(true), `corpus frame ${i}`).to.equal(
					i !== 2,
				);
			}
		});

		it("matches hand-mirrored expectations of the rust corpus", () => {
			const frames = wire.testCorpusFrames();
			const first = decodeTs(frames[0]) as DataMessage;
			expect(toHex(first.header.id)).to.equal("00".repeat(32));
			expect(first.header.timestamp).to.equal(1700000000000n);
			expect(first.header.session).to.equal(1690000000000n);
			expect(first.header.expires).to.equal(4102444800000n);
			expect(first.header.mode).to.be.instanceOf(SilentDelivery);
			expect((first.header.mode as SilentDelivery).to).to.deep.equal([
				hashA,
			]);
			expect([...first.data!]).to.deep.equal([1, 2, 3]);
			expect(
				first.header.signatures!.signatures.map((s) => s.prehash),
			).to.deep.equal([PreHash.SHA_256]);

			const ack = decodeTs(frames[3]) as ACK;
			expect(ack).to.be.instanceOf(ACK);
			expect(ack.header.mode).to.be.instanceOf(TracedDelivery);
			expect((ack.header.mode as TracedDelivery).trace).to.deep.equal([
				hashA,
				hashB,
			]);
			expect(ack.header.origin?.multiaddrs).to.deep.equal([
				"/ip4/127.0.0.1/tcp/4002",
				"/ip4/127.0.0.1/tcp/4003/ws",
			]);
			expect(toHex(ack.messageIdToAcknowledge)).to.equal("09".repeat(32));
			expect(ack.seenCounter).to.equal(1);

			const hello = decodeTs(frames[4]) as Hello;
			expect(hello).to.be.instanceOf(Hello);
			expect(hello.joined).to.deep.equal([hashA]);
			expect(
				hello.header.signatures!.signatures.map((s) => s.prehash),
			).to.deep.equal([PreHash.SHA_256, PreHash.NONE]);

			const goodbye = decodeTs(frames[5]) as Goodbye;
			expect(goodbye).to.be.instanceOf(Goodbye);
			expect(goodbye.leaving).to.deep.equal([hashA]);

			const big = decodeTs(frames[6]) as DataMessage;
			expect(big.header.mode).to.be.instanceOf(AcknowledgeAnyWhere);
			expect(big.data!.length).to.equal(4096);
			expect(big.data![4095]).to.equal(4095 % 251);
		});
	});
});
