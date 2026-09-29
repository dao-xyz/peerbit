import { deserialize, serialize } from "@dao-xyz/borsh";
import { expect } from "chai";
import {
	CHECKPOINT_MAX_DOCUMENT_BYTES,
	CHECKPOINT_MAX_DOCUMENT_FIELDS,
	CHECKPOINT_MAX_KEY_BYTES,
	CHECKPOINT_MAX_OPERATION_BYTES,
	CheckpointDocument,
	CheckpointOperation,
	decodeCheckpointOperation,
} from "../src/checkpoint-operation.js";
import {
	DeleteOperation,
	Operation,
	PutOperation,
	isDeleteOperation,
	isPutOperation,
} from "../src/operation.js";

const encoder = new TextEncoder();
const doc = () => CheckpointDocument.from({ id: "key", name: "value" });
const put = () =>
	new CheckpointOperation({
		resource: Uint8Array.from({ length: 32 }, (_, index) => index),
		epoch: 0x123456789abcdefn,
		checkpoint: new Uint8Array(32).fill(7),
		kind: 0,
		key: "key",
		data: doc().value,
	});

describe("checkpoint document operation profile", () => {
	it("round trips the fixed envelope without changing legacy operation variants", () => {
		const operation = put();
		const bytes = serialize(operation);
		expect(bytes.slice(0, 2)).deep.equal(Uint8Array.of(0, 5));
		const decoded = decodeCheckpointOperation(bytes);
		expect(decoded).deep.equal(operation);
		expect(serialize(decoded)).deep.equal(bytes);
		expect(deserialize(bytes, Operation)).instanceOf(CheckpointOperation);
		expect(isPutOperation(decoded)).equal(false);
		expect(isDeleteOperation(decoded)).equal(false);
		const legacyPut = Uint8Array.of(0, 3, 1, 0, 0, 0, 42);
		expect(serialize(deserialize(legacyPut, Operation))).deep.equal(legacyPut);
		expect(deserialize(legacyPut, Operation)).instanceOf(PutOperation);
		const legacyDelete = Uint8Array.of(0, 4, 0, 1, 0, 0, 0, 107);
		expect(deserialize(legacyDelete, Operation)).instanceOf(DeleteOperation);
		expect(serialize(deserialize(legacyDelete, Operation))).deep.equal(
			legacyDelete,
		);
	});

	it("owns input and decoded buffers without calling caller iterators", () => {
		const operation = put();
		const resource = new Uint8Array(operation.resource);
		const checkpoint = new Uint8Array(operation.checkpoint);
		const data = new Uint8Array(operation.data);
		for (const bytes of [resource, checkpoint, data]) {
			Object.defineProperty(bytes, Symbol.iterator, {
				value: () => {
					throw new Error("Untrusted iterator was invoked");
				},
			});
		}
		const copied = new CheckpointOperation({
			resource,
			checkpoint,
			data,
			kind: 0,
			epoch: operation.epoch,
			key: "key",
		});
		resource.fill(0);
		checkpoint.fill(0);
		data.fill(0);
		expect(copied).deep.equal(operation);
		const wire = serialize(copied);
		Object.defineProperty(wire, "byteLength", { value: 1 });
		const decoded = decodeCheckpointOperation(wire);
		wire.fill(0);
		expect(decoded).deep.equal(operation);
	});

	it("rejects truncation, forged lengths, trailing bytes and invalid envelope fields", () => {
		const bytes = serialize(put());
		for (let length = 0; length < bytes.length; length++) {
			expect(() =>
				decodeCheckpointOperation(bytes.subarray(0, length)),
			).to.throw();
		}
		const trailing = new Uint8Array(bytes.length + 1);
		trailing.set(bytes);
		expect(() => decodeCheckpointOperation(trailing)).to.throw();
		for (const offset of [75, 82]) {
			const forged = new Uint8Array(bytes);
			new DataView(forged.buffer).setUint32(offset, 0xffffffff, true);
			expect(() => decodeCheckpointOperation(forged)).to.throw();
		}
		for (const [offset, value] of [
			[0, 1],
			[1, 4],
			[74, 2],
			[79, 0xff],
		] as const) {
			const forged = new Uint8Array(bytes);
			forged[offset] = value;
			expect(() => decodeCheckpointOperation(forged)).to.throw();
		}
		expect(() =>
			decodeCheckpointOperation(
				new Uint8Array(CHECKPOINT_MAX_OPERATION_BYTES + 1),
			),
		).to.throw("capacity");
		for (const epoch of [-1n, 0x10000000000000000n]) {
			expect(() => new CheckpointOperation({ ...put(), epoch })).to.throw(
				"epoch",
			);
		}
		expect(
			() =>
				new CheckpointOperation({
					...put(),
					key: "k".repeat(CHECKPOINT_MAX_KEY_BYTES + 1),
				}),
		).to.throw("key");
		expect(() => new CheckpointOperation({ ...put(), key: "\ud800" })).to.throw(
			"encoding",
		);
		expect(
			() => new CheckpointOperation({ ...put(), resource: new Uint8Array(31) }),
		).to.throw("32 bytes");
	});

	it("represents deletion without a value or a legacy CUT payload", () => {
		const deletion = new CheckpointOperation({
			...put(),
			kind: 1,
			data: new Uint8Array(0),
		});
		expect(decodeCheckpointOperation(serialize(deletion))).deep.equal(deletion);
		expect(isDeleteOperation(deletion)).equal(false);
		expect(() => new CheckpointOperation({ ...put(), kind: 1 })).to.throw(
			"tombstone",
		);
	});

	it("canonicalizes plain objects and preserves the independent identity-index wrapper", () => {
		const input = { id: "key", z: [true, null, 2], a: { y: "hello", x: 1 } };
		const document = CheckpointDocument.from(input);
		expect(new TextDecoder().decode(document.value)).equal(
			'{"a":{"x":1,"y":"hello"},"id":"key","z":[true,null,2]}',
		);
		input.z[0] = false;
		input.a.x = 99;
		expect(document.decode()).deep.equal({
			id: "key",
			z: [true, null, 2],
			a: { y: "hello", x: 1 },
		});
		const decoded = deserialize(serialize(document), CheckpointDocument);
		expect(decoded.decode()).deep.equal(document.decode());
		const firstRead = document.decode();
		firstRead.id = "changed";
		expect(document.decode().id).equal("key");
	});

	it("does not invoke getters or application serialization hooks", () => {
		let invoked = false;
		const accessor = {
			id: "key",
			get secret() {
				invoked = true;
				return "secret";
			},
		};
		expect(() => CheckpointDocument.from(accessor)).to.throw("ordinary");
		expect(invoked).equal(false);
		const accessorId = {
			get id() {
				invoked = true;
				return "key";
			},
		};
		expect(() => CheckpointDocument.from(accessorId)).to.throw("ordinary id");
		expect(invoked).equal(false);
		const input = {
			id: "key",
			toJSON() {
				invoked = true;
				return {};
			},
		};
		expect(() => CheckpointDocument.from(input as never)).to.throw(
			"JSON values",
		);
		expect(invoked).equal(false);
	});

	it("rejects noncanonical or oversized JSON and mismatched embedded identity", () => {
		for (const value of [
			'{"id":"key", "name":"value"}',
			'{"name":"value","id":"key"}',
			'{"id":"old","id":"key"}',
			'{"id":"key","value":1.0}',
			'{"id":"key","value":-0}',
			'{"id":"key","value":1e999}',
			'{"id":"different"}',
			'[{"id":"key"}]',
			'\ufeff{"id":"key"}',
		]) {
			expect(() =>
				CheckpointDocument.fromBytes("key", encoder.encode(value)),
			).to.throw();
		}
		expect(() =>
			CheckpointDocument.fromBytes("key", Uint8Array.of(0xff)),
		).to.throw();
		expect(() =>
			CheckpointDocument.fromBytes(
				"key",
				new Uint8Array(CHECKPOINT_MAX_DOCUMENT_BYTES + 1),
			),
		).to.throw("capacity");
	});

	it("bounds nesting, fields and values, and rejects non-plain local data", () => {
		const deep =
			'{"id":"key","v":' + "[".repeat(33) + "0" + "]".repeat(33) + "}";
		expect(() =>
			CheckpointDocument.fromBytes("key", encoder.encode(deep)),
		).to.throw("deep");
		const manyFields = Object.fromEntries(
			Array.from({ length: CHECKPOINT_MAX_DOCUMENT_FIELDS }, (_, index) => [
				`f${index}`,
				0,
			]),
		);
		expect(() =>
			CheckpointDocument.from({ id: "key", ...manyFields }),
		).to.throw("field capacity");
		expect(() =>
			CheckpointDocument.fromBytes(
				"key",
				encoder.encode(JSON.stringify({ id: "key", ...manyFields })),
			),
		).to.throw("field capacity");
		const sparse = new Array(3);
		sparse[0] = 1;
		sparse[2] = 3;
		for (const value of [
			undefined,
			NaN,
			Infinity,
			1n,
			new Date(),
			new Map(),
			sparse,
		]) {
			expect(() =>
				CheckpointDocument.from({ id: "key", value } as never),
			).to.throw();
		}
		const cyclic: { id: string; self?: unknown } = { id: "key" };
		cyclic.self = cyclic;
		expect(() => CheckpointDocument.from(cyclic as never)).to.throw("cyclic");
	});

	it("uses matching structural limits when encoding and receiving documents", () => {
		let nested: unknown = null;
		for (let index = 0; index < 31; index++) nested = [nested];
		const document = CheckpointDocument.from({ id: "key", nested } as never);
		expect(
			CheckpointDocument.fromBytes("key", document.value).value,
		).deep.equal(document.value);
		expect(() =>
			CheckpointDocument.from({ id: "key", nested: [nested] } as never),
		).to.throw("deep");
		const oversizedNodes = '{"id":"key","v":[' + "0,".repeat(65_532) + "0]}";
		expect(() =>
			CheckpointDocument.fromBytes("key", encoder.encode(oversizedNodes)),
		).to.throw("node capacity");
		const fixed = encoder.encode('{"id":"key","value":""}').byteLength;
		const full = CheckpointDocument.from({
			id: "key",
			value: "x".repeat(CHECKPOINT_MAX_DOCUMENT_BYTES - fixed),
		});
		expect(full.value.byteLength).equal(CHECKPOINT_MAX_DOCUMENT_BYTES);
		expect(CheckpointDocument.fromBytes("key", full.value).value).deep.equal(
			full.value,
		);
		expect(() =>
			CheckpointDocument.from({
				id: "key",
				value: "x".repeat(CHECKPOINT_MAX_DOCUMENT_BYTES - fixed + 1),
			}),
		).to.throw("capacity");
	});

	it("accepts special own-property names without prototype mutation", () => {
		const input = JSON.parse('{"__proto__":{"polluted":true},"id":"key"}');
		const result = CheckpointDocument.from(input).decode();
		expect(Object.getPrototypeOf(result)).equal(Object.prototype);
		expect(Object.hasOwn(result, "__proto__")).equal(true);
		expect(({} as { polluted?: boolean }).polluted).equal(undefined);
	});
});
