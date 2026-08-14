import { serialize } from "@dao-xyz/borsh";
import { expect } from "chai";
import { PreHash, SignatureWithKey, sha256 } from "@peerbit/crypto";
import {
	ACK_CONTROL_PRIORITY,
	AcknowledgeDelivery,
	AnyWhere,
	BACKGROUND_MESSAGE_PRIORITY,
	CONVERGENCE_MESSAGE_PRIORITY,
	DataMessage,
	FOREGROUND_READ_MESSAGE_PRIORITY,
	MessageHeader,
	SilentDelivery,
	Signatures,
	TracedDelivery,
} from "@peerbit/stream-interface";
import {
	cpSync,
	existsSync,
	mkdtempSync,
	rmSync,
	writeFileSync,
} from "fs";
import path from "path";
import { Uint8ArrayList } from "uint8arraylist";
import { pathToFileURL } from "url";

const serializeUnsignedMessage = (message: DataMessage) => {
	const mode = message.header.mode;
	message.header.mode = undefined as any;
	const signatures = message.header.signatures;
	message.header.signatures = undefined;
	const bytes = serialize(message);
	message.header.signatures = signatures;
	message.header.mode = mode;
	return bytes;
};

const toByteArray = (bytes: Uint8Array | Uint8ArrayList) =>
	bytes instanceof Uint8Array ? bytes : bytes.subarray();

// Loads a SECOND, independent copy of @peerbit/crypto so the test can prove a
// signature produced by a foreign module identity still normalizes.
//
// Two details are load-bearing, and getting either wrong kills the whole run
// rather than failing this test. Until 2026-08-14 this copied `src` into
// os.tmpdir() and imported `index.ts`:
//
//   1. os.tmpdir() is outside every node_modules, so the copy's ten bare
//      specifiers (@dao-xyz/borsh, @libp2p/crypto, @noble/curves, ...) cannot
//      resolve — Node walks up from /tmp and finds nothing.
//   2. importing raw `.ts` puts the file through aegir's TypeScript loader with
//      no tsconfig context.
//
// The failure was not a failing test: the process exited 0 mid-run, mocha
// printed no summary, and every spec after this one silently never executed —
// 46 of ~187 tests ran and CI stayed green.
//
// So: copy the COMPILED output, and stage it INSIDE the crypto package where
// its own dependencies resolve.
const importForeignCrypto = async () => {
	const cryptoRoot = path.resolve(process.cwd(), "../../utils/crypto");
	const cryptoDist = path.join(cryptoRoot, "dist/src");
	if (!existsSync(path.join(cryptoDist, "index.js"))) {
		throw new Error(
			`@peerbit/crypto must be built before this test: ${cryptoDist}/index.js is missing`,
		);
	}
	const tempRoot = mkdtempSync(path.join(cryptoRoot, ".foreign-crypto-"));
	try {
		cpSync(cryptoDist, path.join(tempRoot, "src"), { recursive: true });
		writeFileSync(
			path.join(tempRoot, "package.json"),
			JSON.stringify({ type: "module" }),
		);
		const moduleUrl =
			pathToFileURL(path.join(tempRoot, "src/index.js")).href +
			`?t=${Date.now()}`;
		const foreign = await import(moduleUrl);
		return {
			foreign,
			cleanup: () => rmSync(tempRoot, { recursive: true, force: true }),
		};
	} catch (error) {
		rmSync(tempRoot, { recursive: true, force: true });
		throw error;
	}
};

describe("message signing", () => {
	it("defines the transport priority lane mapping", () => {
		expect(BACKGROUND_MESSAGE_PRIORITY).to.equal(0);
		expect(CONVERGENCE_MESSAGE_PRIORITY).to.equal(1);
		expect(FOREGROUND_READ_MESSAGE_PRIORITY).to.equal(2);
		expect(ACK_CONTROL_PRIORITY).to.equal(3);
	});

	it("defaults background delivery modes onto the background lane", () => {
		const silent = new MessageHeader({
			session: 1,
			mode: new SilentDelivery({
				to: ["peer-a"],
				redundancy: 1,
			}),
		});
		const anywhere = new MessageHeader({
			session: 1,
			mode: new AnyWhere(),
		});

		expect(silent.priority).to.equal(BACKGROUND_MESSAGE_PRIORITY);
		expect(anywhere.priority).to.equal(BACKGROUND_MESSAGE_PRIORITY);
	});

	it("normalizes foreign crypto signatures before serializing data-messages", async () => {
		const { foreign, cleanup } = await importForeignCrypto();
		try {
			const foreignKeypair = await (foreign as any).Ed25519Keypair.create();
			const message = new DataMessage({
				header: new MessageHeader({
					session: 1,
					mode: new SilentDelivery({
						to: ["peer-a"],
						redundancy: 1,
					}),
				}),
				data: new Uint8Array([9, 8, 7]),
			});

			await message.sign((bytes) => foreignKeypair.sign(bytes));

			expect(message.header.signatures).to.be.instanceOf(Signatures);
			expect(message.header.signatures?.signatures[0]).to.be.instanceOf(
				SignatureWithKey,
			);
			expect(Array.from(toByteArray(message.bytes()))).to.deep.equal(
				Array.from(serialize(message)),
			);
		} finally {
			cleanup();
		}
	});

	it("caches signable data-message bytes independently of routing changes", () => {
		const message = new DataMessage({
			header: new MessageHeader({
				session: 1,
				mode: new SilentDelivery({
					to: ["peer-a"],
					redundancy: 1,
				}),
			}),
			data: new Uint8Array([1, 2, 3]),
		});

		expect(Array.from(message.getSignableBytes())).to.deep.equal(
			Array.from(serializeUnsignedMessage(message)),
		);

		const cached = message.getSignableBytes();
		message.header.mode.to = ["peer-b", "peer-c"];

		expect(message.getSignableBytes()).to.equal(cached);
		expect(Array.from(message.getSignableBytes())).to.deep.equal(
			Array.from(serializeUnsignedMessage(message)),
		);
	});

	it("serializes data-message bytes canonically after routing changes", () => {
		const message = new DataMessage({
			header: new MessageHeader({
				session: 1,
				mode: new SilentDelivery({
					to: ["peer-a"],
					redundancy: 2,
				}),
			}),
			data: new Uint8Array([4, 5, 6]),
		});

		expect(Array.from(toByteArray(message.bytes()))).to.deep.equal(
			Array.from(serialize(message)),
		);

		message.header.mode.to = ["peer-z"];

		expect(Array.from(toByteArray(message.bytes()))).to.deep.equal(
			Array.from(serialize(message)),
		);
	});

	it("keeps the data buffer segmented when serializing a data-message", () => {
		const payload = new Uint8Array([7, 8, 9]);
		const message = new DataMessage({
			header: new MessageHeader({
				session: 1,
				mode: new SilentDelivery({
					to: ["peer-a"],
					redundancy: 1,
				}),
			}),
			data: payload,
		});

		const bytes = message.bytes();
		expect(bytes).to.be.instanceOf(Uint8ArrayList);
		expect([...((bytes as Uint8ArrayList) as Iterable<Uint8Array>)].at(-1)).to.equal(
			payload,
		);
		expect(Array.from(toByteArray(bytes))).to.deep.equal(
			Array.from(serialize(message)),
		);
	});

	it("decodes segmented payloads lazily from serialized data-messages", () => {
		const payload = new Uint8Array([10, 11, 12, 13]);
		const encoded = new DataMessage({
			header: new MessageHeader({
				session: 1,
				mode: new SilentDelivery({
					to: ["peer-a"],
					redundancy: 1,
				}),
			}),
			data: payload,
		}).bytes() as Uint8ArrayList;

		const decoded = DataMessage.from(encoded);
		const decodedAny = decoded as any;

		expect(decoded.hasData).to.equal(true);
		expect(decodedAny._data).to.equal(undefined);
		expect(decodedAny._dataBytes).to.be.instanceOf(Uint8ArrayList);
		expect(Array.from(toByteArray(decoded.bytes()))).to.deep.equal(
			Array.from(toByteArray(encoded)),
		);
		expect(Array.from(decoded.data!)).to.deep.equal(Array.from(payload));
	});

	it("materializes payloads from list-like byte sources", () => {
		const payload = new Uint8Array([31, 32, 33, 34]);
		const decoded = new DataMessage({
			header: new MessageHeader({
				session: 1,
				mode: new SilentDelivery({
					to: ["peer-a"],
					redundancy: 1,
				}),
			}),
			data: payload,
		});
		const decodedAny = decoded as any;

		decodedAny._data = undefined;
		decodedAny._dataBytes = {
			byteLength: payload.byteLength,
			subarray: () => payload,
		};

		expect(decoded.hasData).to.equal(true);
		expect(Array.from(decoded.data!)).to.deep.equal(Array.from(payload));
	});

	it("caches prepared signable sha256 bytes for a data-message", async () => {
		const message = new DataMessage({
			header: new MessageHeader({
				session: 1,
				mode: new SilentDelivery({
					to: ["peer-a"],
					redundancy: 1,
				}),
			}),
			data: new Uint8Array([21, 22, 23]),
		});

		const prepared = await message.getPreparedSignableBytes(PreHash.SHA_256);
		const again = await message.getPreparedSignableBytes(PreHash.SHA_256);

		expect(again).to.equal(prepared);
		expect(Array.from(prepared)).to.deep.equal(
			Array.from(await sha256(message.getSignableBytes())),
		);
	});

	it("defaults acknowledged responses onto the control lane", () => {
		const request = new MessageHeader({
			session: 1,
			mode: new AcknowledgeDelivery({
				to: ["peer-a"],
				redundancy: 1,
			}),
		});
		const ack = new MessageHeader({
			session: 1,
			mode: new TracedDelivery(["peer-a"]),
		});

		expect(request.priority).to.equal(CONVERGENCE_MESSAGE_PRIORITY);
		expect(request.responsePriority).to.equal(ACK_CONTROL_PRIORITY);
		expect(ack.priority).to.equal(ACK_CONTROL_PRIORITY);
	});
});
