import {
	type BinaryReader,
	type BinaryWriter,
	deserialize,
	field,
	serialize,
	variant,
	vec,
} from "@dao-xyz/borsh";
import { toId } from "@peerbit/indexer-interface";
import { Entry } from "@peerbit/log";
import { expect } from "chai";
import {
	detachEntryPayloadForCallback,
	detachOperationBytes,
} from "../src/callback-detachment.js";
import { DeleteOperation, Operation, PutOperation } from "../src/operation.js";

const byteMapField = {
	serialize: (value: Map<string, Uint8Array>, writer: BinaryWriter) => {
		writer.u32(value.size);
		for (const [key, bytes] of value) {
			writer.string(key);
			writer.uint8Array(bytes);
		}
	},
	deserialize: (reader: BinaryReader): Map<string, Uint8Array> => {
		const value = new Map<string, Uint8Array>();
		const size = reader.u32();
		for (let i = 0; i < size; i++) {
			value.set(reader.string(), reader.uint8Array());
		}
		return value;
	},
};

abstract class BrowserChoice {}

@variant("callback_detachment_browser_choice")
class BrowserChoiceValue extends BrowserChoice {
	@field({ type: Uint8Array })
	bytes: Uint8Array;

	constructor(bytes: Uint8Array) {
		super();
		this.bytes = bytes;
	}
}

@variant("callback_detachment_browser_document")
class BrowserDocument {
	@field({ type: Uint8Array })
	direct: Uint8Array;

	@field({ type: vec(Uint8Array) })
	vectors: Uint8Array[];

	@field({ type: vec(BrowserChoice) })
	choices: BrowserChoice[];

	@field(byteMapField)
	mapped: Map<string, Uint8Array>;

	constructor() {
		this.direct = new Uint8Array([1, 2]);
		this.vectors = [new Uint8Array([3, 4])];
		this.choices = [new BrowserChoiceValue(new Uint8Array([5, 6]))];
		this.mapped = new Map([["bytes", new Uint8Array([7, 8])]]);
	}
}

const firstBytes = (document: BrowserDocument): number[] => [
	document.direct[0]!,
	document.vectors[0]![0]!,
	(document.choices[0] as BrowserChoiceValue).bytes[0]!,
	document.mapped.get("bytes")![0]!,
];

const mutateBytes = (document: BrowserDocument): void => {
	document.direct[0] = 20;
	document.vectors[0]![0] = 21;
	(document.choices[0] as BrowserChoiceValue).bytes[0] = 22;
	document.mapped.get("bytes")![0] = 23;
};

describe("callback detachment browser boundary", () => {
	it("isolates nested decoded bytes through operation and Entry payload views", async () => {
		const documentBytes = serialize(new BrowserDocument());
		const operation = new PutOperation({ data: documentBytes });
		const payloadBytes = serialize(operation);
		const encoding = {
			encoder: (value: Operation) => serialize(value),
			decoder: (bytes: Uint8Array) => deserialize(bytes, Operation),
		};
		const payload = {
			data: payloadBytes,
			encoding,
			_value: operation as Operation | undefined,
			get isDecoded() {
				return this._value !== undefined;
			},
			get value() {
				return this._value!;
			},
			getValue() {
				return this._value ?? encoding.decoder(this.data);
			},
		};
		Object.freeze(payload);
		const metaData = new Uint8Array([9, 8]);
		const clockId = new Uint8Array([7, 6]);
		const metaBytes = new Uint8Array([5, 4]);
		const hashDigestBytes = new Uint8Array([3, 2]);
		const signatureBytes = new Uint8Array([11, 12]);
		const publicKeyBytes = new Uint8Array([13, 14]);
		const cachedPublicKeyBytes = new Uint8Array([15, 16]);
		const publicKey = {
			publicKey: publicKeyBytes,
			_bytes: cachedPublicKeyBytes,
			get bytes() {
				return this._bytes;
			},
		};
		const signature = {
			signature: signatureBytes,
			publicKey,
			prehash: 0,
		};
		const meta = {
			gid: "browser-gid",
			next: ["browser-next"],
			type: 0,
			data: metaData,
			clock: {
				id: clockId,
				timestamp: { wallTime: 1n, logical: 2 },
			},
		};
		const shallow = {
			hash: "browser-entry",
			head: true,
			payloadSize: payloadBytes.byteLength,
			meta,
			getMetaBytes: async () => metaBytes,
			getHashDigestBytes: async () => hashDigestBytes,
		};
		const entry = Object.assign(Object.create(Entry.prototype), {
			payload,
			meta,
			signatures: [signature],
			publicKeys: [publicKey],
			hash: "browser-entry",
			async getMeta() {
				return meta;
			},
			async getClock() {
				return meta.clock;
			},
			async getNext() {
				return meta.next;
			},
			async getMetaBytes() {
				return metaBytes;
			},
			async getHashDigestBytes() {
				return hashDigestBytes;
			},
			async getSignatures() {
				return [signature];
			},
			async getPublicKeys() {
				return [publicKey];
			},
			getPayloadValue() {
				return payload.getValue();
			},
			getStorageBytes() {
				return payload.data;
			},
			toMaterialized() {
				return this;
			},
			toSignable() {
				return this;
			},
			toShallow() {
				return shallow;
			},
			init() {
				return this;
			},
		}) as Entry<Operation>;
		const canonicalPayload = new Uint8Array(payloadBytes);
		const canonicalOperationData = new Uint8Array(operation.data);

		const callbackEntry = detachEntryPayloadForCallback(entry);
		expect(Object.isFrozen(callbackEntry.payload)).to.equal(true);
		expect(callbackEntry.valueOf()).to.equal(callbackEntry);
		const callbackOperation =
			(await callbackEntry.getPayloadValue()) as PutOperation;
		const callbackDocument = deserialize(
			callbackOperation.data,
			BrowserDocument,
		);
		mutateBytes(callbackDocument);
		callbackOperation.data[0] ^= 0xff;
		callbackEntry.payload.data[0] ^= 0xff;
		callbackEntry.getStorageBytes()[0] ^= 0xff;
		callbackEntry.meta.data![0] ^= 0xff;
		callbackEntry.meta.clock.id[0] ^= 0xff;
		callbackEntry.meta.next.push("callback");
		const callbackMeta = await (callbackEntry as any).getMeta();
		callbackMeta.data[0] ^= 0xff;
		const callbackClock = await (callbackEntry as any).getClock();
		callbackClock.id[0] ^= 0xff;
		const callbackNext = await (callbackEntry as any).getNext();
		callbackNext.push("async-callback");
		const callbackMetaBytes = await (callbackEntry as any).getMetaBytes();
		callbackMetaBytes[0] ^= 0xff;
		const callbackHashDigestBytes = await (
			callbackEntry as any
		).getHashDigestBytes();
		callbackHashDigestBytes[0] ^= 0xff;
		const callbackSignatures = await (callbackEntry as any).getSignatures();
		callbackSignatures[0].signature[0] ^= 0xff;
		callbackSignatures[0].publicKey.publicKey[0] ^= 0xff;
		const callbackPublicKeys = await (callbackEntry as any).getPublicKeys();
		callbackPublicKeys[0].publicKey[0] ^= 0xff;
		callbackPublicKeys[0].bytes[0] ^= 0xff;
		(callbackEntry.signatures[0] as any).signature[0] ^= 0xff;
		(callbackEntry.signatures[0] as any).publicKey.publicKey[0] ^= 0xff;
		(callbackEntry.publicKeys[0] as any).bytes[0] ^= 0xff;
		const callbackShallow = callbackEntry.toShallow(true) as any;
		callbackShallow.meta.data[0] ^= 0xff;
		callbackShallow.meta.clock.id[0] ^= 0xff;
		(await callbackShallow.getMetaBytes())[0] ^= 0xff;
		(await callbackShallow.getHashDigestBytes())[0] ^= 0xff;

		expect(payload.data).to.deep.equal(canonicalPayload);
		expect(operation.data).to.deep.equal(canonicalOperationData);
		expect(metaData).to.deep.equal(new Uint8Array([9, 8]));
		expect(clockId).to.deep.equal(new Uint8Array([7, 6]));
		expect(meta.next).to.deep.equal(["browser-next"]);
		expect(metaBytes).to.deep.equal(new Uint8Array([5, 4]));
		expect(hashDigestBytes).to.deep.equal(new Uint8Array([3, 2]));
		expect(signatureBytes).to.deep.equal(new Uint8Array([11, 12]));
		expect(publicKeyBytes).to.deep.equal(new Uint8Array([13, 14]));
		expect(cachedPublicKeyBytes).to.deep.equal(new Uint8Array([15, 16]));
		expect(
			firstBytes(deserialize(operation.data, BrowserDocument)),
		).to.deep.equal([1, 3, 5, 7]);
	});

	it("isolates scalar delete keys", () => {
		const operation = new DeleteOperation({ key: toId("signed") });
		const callbackOperation = detachOperationBytes(operation);
		(callbackOperation.key as any).key = "changed";
		(callbackOperation as any).key = toId("reassigned");

		expect((operation.key as any).key).to.equal("signed");
	});
});
