import { serialize } from "@dao-xyz/borsh";
import { expect } from "chai";
import { PreHash, SignatureWithKey, sha256 } from "@peerbit/crypto";
import {
	ACK_CONTROL_PRIORITY,
	AcknowledgeDelivery,
	DataMessage,
	MessageHeader,
	SilentDelivery,
	Signatures,
	TracedDelivery,
} from "@peerbit/stream-interface";
import { cpSync, mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
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

const importForeignCrypto = async () => {
	const tempRoot = mkdtempSync(path.join(tmpdir(), "peerbit-crypto-duplicate-"));
	const cryptoSrc = path.resolve(process.cwd(), "../../utils/crypto/src");
	cpSync(cryptoSrc, path.join(tempRoot, "src"), { recursive: true });
	writeFileSync(
		path.join(tempRoot, "package.json"),
		JSON.stringify({ type: "module" }),
	);
	try {
		const moduleUrl =
			pathToFileURL(path.join(tempRoot, "src/index.ts")).href +
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

		expect(request.priority).to.equal(1);
		expect(request.responsePriority).to.equal(ACK_CONTROL_PRIORITY);
		expect(ack.priority).to.equal(ACK_CONTROL_PRIORITY);
	});
});
