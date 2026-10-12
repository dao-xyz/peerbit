import {
	BinaryReader,
	BinaryWriter,
	deserialize,
	field,
	getSchema,
	serialize,
	variant,
} from "@dao-xyz/borsh";
import { Ed25519PublicKey } from "@peerbit/crypto";
import { calculateRawCidV1Batch } from "@peerbit/log";
import { Program } from "@peerbit/program";
import { SharedLog } from "@peerbit/shared-log";
import { expect } from "chai";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { Documents } from "../src/program.js";
import {
	type ProofDeleteContract as Contract,
	createProofDeleteLog,
	deriveProofDeleteScope,
	getProofDeleteContract,
	proofDeleteLogCodec as logCodec,
} from "../src/proof-delete-manifest.js";
import { DocumentIndex } from "../src/search.js";

// The independent old reader intentionally retains the pre-format SharedLog
// field. Current Documents understands both generations and is not an old reader.
// These tests prove manifest compatibility, not admission or durability.
@variant(0)
class OldProgram {}

@variant("documents")
class OldDocuments extends OldProgram {
	@field({ type: SharedLog })
	log = new SharedLog({ id: new Uint8Array(32).fill(42) });

	@field({ type: "bool" })
	immutable = false;

	@field({ type: DocumentIndex })
	_index = new DocumentIndex();
}

// Construct hostile wire variants without exporting the private representation.
const rawLogEnvelope = (
	log: SharedLog<any>,
	value: Contract,
	keyKind: number = value.keyKind,
) => {
	const writer = new BinaryWriter();
	writer.u8(0);
	writer.string("documents_proof_log_v1");
	writer.u8(value.version);
	writer.set(value.nonce);
	writer.set(value.owner);
	writer.u8(keyKind);
	serialize(log, writer);
	return writer.finalize();
};

const createFixtureDocuments = () => {
	@variant(0)
	class FixtureProgram {}

	@variant("documents")
	class FixtureDocuments extends FixtureProgram {
		@field(logCodec)
		log: SharedLog<any>;

		@field({ type: "bool" })
		immutable: boolean;

		@field({ type: DocumentIndex })
		_index: DocumentIndex<any, any, any>;

		constructor(original: Documents<any>, contract?: Contract) {
			super();
			this.log = contract ? createProofDeleteLog(contract) : original.log;
			this.immutable = original.immutable;
			this._index = original.index;
		}
	}
	return FixtureDocuments;
};
const FixtureDocuments = createFixtureDocuments();
type FixtureDocuments = InstanceType<typeof FixtureDocuments>;

@variant("test_nested_proof_manifest")
class OldParent {
	@field({ type: OldDocuments })
	docs: OldDocuments;
}

@variant("test_nested_proof_manifest")
class NewParent {
	@field({ type: FixtureDocuments })
	docs: FixtureDocuments;
}

@variant("test_nested_proof_manifest")
class CurrentParent {
	@field({ type: Documents })
	docs: Documents<any>;
}

const legacy = () => new Documents({ id: new Uint8Array(32).fill(42) });
const contract = (): Contract => ({
	version: 1,
	nonce: new Uint8Array(32).fill(11),
	owner: new Uint8Array(32).fill(7),
	keyKind: 0,
});
const proof = () => new FixtureDocuments(legacy(), contract());
const actualProof = (keyType: "string" | "bytes" = "string") =>
	new Documents({
		id: contract().nonce,
		format: {
			version: "owner-proofs-v1",
			owner: new Ed25519PublicKey({ publicKey: contract().owner }),
			keyType,
		},
	});
const cid = async (bytes: Uint8Array) =>
	(await calculateRawCidV1Batch([bytes]))[0];

describe("proof-delete manifest compatibility", () => {
	it("preserves actual legacy Documents bytes, address and field layout", async () => {
		const original = legacy();
		const bytes = serialize(original);
		// Fixed independently from the old layout with ID 0x2a repeated 32 times.
		const golden =
			"0009000000646f63756d656e7473000a0000007368617265645f6c6f67002a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a000300000072706300000f000000646f63756d656e74735f696e6465780003000000727063";
		expect(Buffer.from(serialize(new OldDocuments())).toString("hex")).to.equal(
			golden,
		);
		expect(Buffer.from(bytes).toString("hex")).to.equal(golden);
		expect(await cid(bytes)).to.equal(
			"zb2rhaEZWQkJGcz9u9ffn3bg56tk5uLbKjUdgJRMkSCwLE2Fr",
		);
		expect(serialize(deserialize(bytes, OldDocuments))).to.deep.equal(bytes);
		const decoded = deserialize(bytes, FixtureDocuments);
		expect(serialize(new FixtureDocuments(original))).to.deep.equal(bytes);
		expect(serialize(decoded)).to.deep.equal(bytes);
		expect(await cid(serialize(decoded))).to.equal(
			(await original.calculateAddress()).address,
		);
		expect(getSchema(FixtureDocuments).variant).to.equal("documents");
		expect(getSchema(FixtureDocuments).fields.map((f) => f.key)).to.deep.equal(
			getSchema(Documents).fields.map((f) => f.key),
		);
		expect(getProofDeleteContract(decoded.log)).to.equal(undefined);
		expect(deserialize(serialize(decoded), Documents)).to.be.instanceOf(
			Documents,
		);
	});

	it("preserves nested legacy fields and new-code clone bytes", () => {
		const original = Object.assign(new OldParent(), {
			docs: new OldDocuments(),
		});
		const decoded = deserialize(serialize(original), NewParent);
		expect(serialize(decoded)).to.deep.equal(serialize(original));
		expect(serialize(deserialize(serialize(decoded), NewParent))).to.deep.equal(
			serialize(original),
		);
		expect(
			serialize(deserialize(serialize(original), CurrentParent)),
		).to.deep.equal(serialize(original));
	});

	for (const keyType of ["string", "bytes"] as const) {
		it(`roundtrips the real ${keyType}-key constructor, clone and nested manifest`, async () => {
			const original = actualProof(keyType);
			const bytes = serialize(original);
			const expected: Contract = {
				...contract(),
				keyKind: keyType === "string" ? 0 : 1,
			};
			expect(bytes).to.deep.equal(
				serialize(new FixtureDocuments(legacy(), expected)),
			);
			const parent = Object.assign(new CurrentParent(), { docs: original });
			for (const decoded of [
				deserialize(bytes, Documents),
				deserialize(bytes, Program) as Documents<any>,
				original.clone(),
				deserialize(serialize(parent), CurrentParent).docs,
			]) {
				expect(decoded).to.be.instanceOf(Documents);
				expect(serialize(decoded)).to.deep.equal(bytes);
				expect(getProofDeleteContract(decoded.log)).to.deep.equal(expected);
				expect(decoded.programs).to.deep.equal([decoded.log, decoded.index]);
				expect(decoded.log.log.id).to.deep.equal(original.log.log.id);
				expect(decoded.log.topic).to.equal(original.log.topic);
				expect((await decoded.calculateAddress()).address).to.equal(
					await cid(bytes),
				);
			}
			expect(original.log.log.id).not.to.deep.equal(expected.nonce);
			expect(() => deserialize(bytes, OldDocuments)).to.throw();
			expect(() => deserialize(serialize(parent), OldParent)).to.throw();
		});
	}

	it("owns real constructor inputs and decoded source bytes", () => {
		const nonce = contract().nonce;
		const owner = new Ed25519PublicKey({ publicKey: contract().owner });
		const original = new Documents({
			id: nonce,
			format: { version: "owner-proofs-v1", owner, keyType: "string" },
		});
		const bytes = serialize(original);
		nonce.fill(0);
		owner.publicKey.fill(0);
		expect(serialize(original)).to.deep.equal(bytes);
		const source = new Uint8Array(bytes);
		const decoded = deserialize(source, Documents);
		source.fill(0);
		expect(serialize(decoded)).to.deep.equal(bytes);
		const exposed = getProofDeleteContract(decoded.log)!;
		exposed.owner.fill(0);
		exposed.nonce.fill(0);
		expect(serialize(decoded)).to.deep.equal(bytes);
		decoded.log.log.id = new Uint8Array(32).fill(42);
		expect(() => serialize(decoded)).to.throw("lower log identity mismatch");
	});

	it("rejects unsupported real constructor profiles before creating a store", () => {
		const format = {
			version: "owner-proofs-v1" as const,
			owner: new Ed25519PublicKey({ publicKey: contract().owner }),
			keyType: "string" as const,
		};
		for (const properties of [
			{ format, immutable: true },
			{ format, index: new DocumentIndex() },
			{ format: { ...format, version: "unknown" } },
			{ format: { ...format, keyType: "number" } },
			{ format: { ...format, owner: { publicKey: contract().owner } } },
		]) {
			expect(() => new Documents(properties as never)).to.throw(
				"Unsupported owner-proofs document format",
			);
		}
	});

	it("binds owner/key kind to new bytes while keeping real child programs", async () => {
		const value = proof();
		const bytes = serialize(value);
		const decoded = deserialize(bytes, FixtureDocuments);
		expect(decoded.log).to.be.instanceOf(SharedLog);
		expect(decoded._index).to.be.instanceOf(DocumentIndex);
		expect(getProofDeleteContract(decoded.log)).to.deep.equal(contract());
		expect(serialize(deserialize(bytes, FixtureDocuments))).to.deep.equal(
			bytes,
		);
		const clone = Program.prototype.clone.call(
			decoded as never,
		) as unknown as FixtureDocuments;
		expect(serialize(clone)).to.deep.equal(bytes);
		expect(getProofDeleteContract(clone.log)).to.deep.equal(contract());
		const programs = Object.getOwnPropertyDescriptor(
			Program.prototype,
			"programs",
		)!.get!.call(decoded);
		expect(programs).to.deep.equal([decoded.log, decoded._index]);
		for (const different of [
			{ ...contract(), owner: new Uint8Array(32).fill(8) },
			{ ...contract(), keyKind: 1 as const },
			{ ...contract(), nonce: new Uint8Array(32).fill(12) },
		]) {
			const other = new FixtureDocuments(legacy(), different);
			expect(await cid(serialize(other))).not.to.equal(await cid(bytes));
			expect(other.log.log.id).not.to.deep.equal(value.log.log.id);
			expect(other.log.topic).not.to.equal(value.log.topic);
			expect(deriveProofDeleteScope(different)).not.to.deep.equal(
				deriveProofDeleteScope(contract()),
			);
		}
		expect(await cid(bytes)).not.to.equal(await cid(serialize(legacy())));
		expect(value.log.log.id).not.to.deep.equal(legacy().log.log.id);
		expect(deriveProofDeleteScope(contract())).not.to.deep.equal(
			value.log.log.id,
		);
	});

	it("stripping the contract changes the outer manifest address", async () => {
		const value = proof();
		const scopedAddress = await cid(serialize(value));
		const stripped = new FixtureDocuments(legacy());
		stripped.log = deserialize(serialize(value.log), SharedLog);
		stripped._index = value._index;
		expect(getProofDeleteContract(stripped.log)).to.equal(undefined);
		expect(await cid(serialize(stripped))).not.to.equal(scopedAddress);
		// The missing descriptor cannot activate proof admission. Legacy Operation
		// rejects scoped operation bytes; the operations-module tests cover that
		// separate boundary. An outer address is NOT the signed protocol scope.
	});

	it("the explicit old Documents and Program layouts reject the new generation", () => {
		const bytes = serialize(proof());
		expect(() => deserialize(bytes, OldDocuments)).to.throw();
		expect(() => deserialize(bytes, OldProgram)).to.throw();
		const parent = Object.assign(new NewParent(), { docs: proof() });
		expect(() => deserialize(serialize(parent), OldParent)).to.throw();
	});

	it("owns construction, decode and exposed contract bytes", () => {
		const original = legacy();
		const existing = new FixtureDocuments(original);
		const oldBytes = serialize(existing);
		const input = contract();
		const value = new FixtureDocuments(original, input);
		const before = serialize(value);
		expect(value.log).not.to.equal(original.log);
		expect(getProofDeleteContract(original.log)).to.equal(undefined);
		expect(serialize(existing)).to.deep.equal(oldBytes);
		input.owner.fill(0);
		input.nonce.fill(0);
		expect(serialize(value)).to.deep.equal(before);
		const bytes = new Uint8Array(before);
		const decoded = deserialize(bytes, FixtureDocuments);
		bytes.fill(0);
		expect(serialize(decoded)).to.deep.equal(before);
		const exposed = getProofDeleteContract(value.log)!;
		exposed.owner.fill(0);
		exposed.nonce.fill(0);
		expect(serialize(value)).to.deep.equal(before);
	});

	it("rejects unrelated, invalid and truncated nested manifests", () => {
		const other = new BinaryReader(serialize(new DocumentIndex()));
		expect(() =>
			logCodec.serialize(new DocumentIndex() as never, new BinaryWriter()),
		).to.throw("Expected document SharedLog");
		expect(() => logCodec.deserialize(other)).to.throw(
			"Expected document log manifest",
		);
		expect(() =>
			logCodec.deserialize(
				new BinaryReader(rawLogEnvelope(legacy().log, contract(), 255)),
			),
		).to.throw("Unsupported proof-delete owner/key contract");
		const bytes = serialize(proof());
		expect(() =>
			deserialize(bytes.subarray(0, bytes.length - 1), FixtureDocuments),
		).to.throw();
		expect(() =>
			deserialize(new Uint8Array([...bytes, 0]), FixtureDocuments),
		).to.throw();
	});

	it("rejects unsupported contracts and spoofed byte lengths before construction", () => {
		for (const invalid of [
			{ ...contract(), version: 2 },
			{ ...contract(), keyKind: 2 },
			{ ...contract(), nonce: new Uint8Array(31) },
			{ ...contract(), owner: new Uint8Array(33) },
		]) {
			expect(() => createProofDeleteLog(invalid as Contract)).to.throw();
			expect(() => deriveProofDeleteScope(invalid as Contract)).to.throw();
		}
		const owner = new Uint8Array(33);
		Object.defineProperty(owner, "byteLength", { value: 32 });
		expect(() => createProofDeleteLog({ ...contract(), owner })).to.throw(
			"32-byte",
		);
	});

	it("derives an owned deterministic protocol scope from the complete descriptor", () => {
		const input = contract();
		const expected = deriveProofDeleteScope(input);
		const returned = deriveProofDeleteScope(input);
		expect(returned).to.deep.equal(expected);
		returned.fill(0);
		expect(deriveProofDeleteScope(input)).to.deep.equal(expected);
		input.owner.fill(0);
		expect(deriveProofDeleteScope(input)).not.to.deep.equal(expected);
	});

	it("rejects unrelated registered Program fields without evaluating their decoder", () => {
		let decodedFields = 0;
		@variant("test_unrelated_proof_manifest")
		class Unrelated extends Program {
			@field({
				serialize(_value: number, writer: BinaryWriter) {
					writer.u8(0);
				},
				deserialize(_reader: BinaryReader) {
					decodedFields++;
					throw new Error("Unrelated field must not be decoded");
				},
			})
			value = 0;
			async open() {}
		}
		const unrelated = new Unrelated();
		expect(() =>
			logCodec.deserialize(new BinaryReader(serialize(unrelated))),
		).to.throw("Expected document log manifest");
		expect(() =>
			logCodec.deserialize(
				new BinaryReader(rawLogEnvelope(unrelated as never, contract())),
			),
		).to.throw("Expected document SharedLog");
		expect(decodedFields).to.equal(0);
	});

	it("preserves inherited SharedLog discriminator and external subclass fields", async () => {
		await promisify(execFile)(
			process.execPath,
			[
				"--input-type=module",
				"-e",
				`
import { BinaryReader, BinaryWriter, field, variant, serialize } from '@dao-xyz/borsh';
import { SharedLog } from '@peerbit/shared-log';
import { proofDeleteLogCodec } from './dist/src/proof-delete-manifest.js';
class CustomLog extends SharedLog {}
variant('test_app_proof_log')(CustomLog);
field({ type: 'string' })(CustomLog.prototype, 'label');
const log = new CustomLog({ id: new Uint8Array(32).fill(42) });
log.label = 'preserved log field';
const bytes = serialize(log);
const decoded = proofDeleteLogCodec.deserialize(new BinaryReader(bytes));
if (decoded.constructor !== CustomLog || decoded.label !== log.label) throw Error('Log subclass lost');
const writer = new BinaryWriter();
proofDeleteLogCodec.serialize(decoded, writer);
if (!Buffer.from(bytes).equals(Buffer.from(writer.finalize()))) throw Error('Log bytes changed');
`,
			],
			{ cwd: new URL("../..", import.meta.url), timeout: 10_000 },
		);
	});

	it("rejects a contract with reused or mutated lower replication/storage identity", () => {
		expect(() =>
			logCodec.deserialize(
				new BinaryReader(rawLogEnvelope(legacy().log, contract())),
			),
		).to.throw("lower log identity mismatch");
		const value = proof();
		value.log.log.id = new Uint8Array(32).fill(42);
		expect(() => serialize(value)).to.throw("lower log identity mismatch");
	});

	// Actual Documents subclass registration is isolated: Borsh registers decorated
	// subclasses process-wide, and this test must not modify other document suites.
	it("preserves actual external subclass bytes, added fields and nested fields", async () => {
		const { stdout } = await promisify(execFile)(
			process.execPath,
			[
				"--input-type=module",
				"-e",
				`
import { field, variant, serialize, deserialize } from '@dao-xyz/borsh';
import { Ed25519PublicKey } from '@peerbit/crypto';
import { SharedLog } from '@peerbit/shared-log';
import { Documents } from './dist/src/program.js';
import { DocumentIndex } from './dist/src/search.js';
class OldProgram {}
variant(0)(OldProgram);
class OldDocuments extends OldProgram {
  constructor() {
    super();
    this.log = new SharedLog({ id: new Uint8Array(32).fill(42) });
    this.immutable = false;
    this._index = new DocumentIndex();
  }
}
variant('documents')(OldDocuments);
field({ type: SharedLog })(OldDocuments.prototype, 'log');
field({ type: 'bool' })(OldDocuments.prototype, 'immutable');
field({ type: DocumentIndex })(OldDocuments.prototype, '_index');
let rejected = false;
try { deserialize(new Uint8Array(JSON.parse(process.argv[1])), OldDocuments); } catch { rejected = true; }
if (!rejected) throw Error('Old code accepted proof manifest without its codec');
class Custom extends OldDocuments {}
variant('test_app_documents')(Custom);
field({ type: 'string' })(Custom.prototype, 'label');
class Parent {}
variant('test_nested_proof_manifest')(Parent);
field({ type: OldDocuments })(Parent.prototype, 'docs');
const docs = new Custom();
docs.label = 'preserved application field';
const parent = Object.assign(new Parent(), { docs });
const bytes = serialize(docs);
const decoded = deserialize(bytes, Custom);
if (!(decoded instanceof Custom) || decoded.label !== docs.label) throw Error('Old subclass decode failed');
class CurrentCustom extends Documents {}
variant('test_app_documents')(CurrentCustom);
field({ type: 'string' })(CurrentCustom.prototype, 'label');
const currentLegacy = new CurrentCustom({ id: new Uint8Array(32).fill(42) });
currentLegacy.label = docs.label;
if (!Buffer.from(serialize(currentLegacy)).equals(Buffer.from(bytes))) throw Error('Current subclass changed old bytes');
if (deserialize(bytes, CurrentCustom).label !== docs.label) throw Error('Current subclass cannot read old bytes');
const currentProof = new CurrentCustom({
  id: new Uint8Array(32).fill(11),
  format: { version: 'owner-proofs-v1', owner: new Ed25519PublicKey({ publicKey: new Uint8Array(32).fill(7) }), keyType: 'string' }
});
currentProof.label = docs.label;
const proofBytes = serialize(currentProof);
if (!Buffer.from(serialize(currentProof.clone())).equals(Buffer.from(proofBytes))) throw Error('Current proof subclass clone changed bytes');
if (deserialize(proofBytes, CurrentCustom).label !== docs.label) throw Error('Current proof subclass field lost');
console.log(JSON.stringify({ bytes: [...bytes], parent: [...serialize(parent)] }));
`,
				JSON.stringify([...serialize(proof())]),
			],
			{ cwd: new URL("../..", import.meta.url), timeout: 10_000 },
		);
		const old = JSON.parse(stdout) as { bytes: number[]; parent: number[] };
		// Separate module-style class graph: all subclasses register before the
		// first Borsh decoder is compiled, just as in the old-code child process.
		const CustomDocuments = createFixtureDocuments();
		@variant("test_app_documents")
		class Custom extends CustomDocuments {
			@field({ type: "string" })
			label: string;
		}
		@variant("test_nested_proof_manifest")
		class CustomParent {
			@field({ type: CustomDocuments })
			docs: InstanceType<typeof CustomDocuments>;
		}
		const bytes = new Uint8Array(old.bytes);
		const value = deserialize(bytes, Custom);
		expect(value).to.be.instanceOf(Custom);
		expect(value.label).to.equal("preserved application field");
		expect(serialize(value)).to.deep.equal(bytes);
		expect(await cid(serialize(value))).to.equal(await cid(bytes));
		expect(
			serialize(deserialize(new Uint8Array(old.parent), CustomParent)),
		).to.deep.equal(new Uint8Array(old.parent));
		const newGeneration = new Custom(legacy(), contract());
		newGeneration.label = value.label;
		const upgraded = deserialize(serialize(newGeneration), Custom);
		expect(upgraded.label).to.equal(value.label);
		expect(getProofDeleteContract(upgraded.log)).to.deep.equal(contract());
		const otherView = deserialize(serialize(upgraded), Custom);
		otherView.label = "another presentation of the same protocol store";
		expect(await cid(serialize(otherView))).not.to.equal(
			await cid(serialize(upgraded)),
		);
		expect(otherView.log.topic).to.equal(upgraded.log.topic);
		expect(
			deriveProofDeleteScope(getProofDeleteContract(otherView.log)!),
		).to.deep.equal(
			deriveProofDeleteScope(getProofDeleteContract(upgraded.log)!),
		);
	});
});
