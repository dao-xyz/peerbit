import {
	BinaryReader,
	BinaryWriter,
	type CustomField,
	deserialize,
	deserializeStruct,
	field,
	fixedArray,
	serialize,
	variant,
} from "@dao-xyz/borsh";
import { sha256Sync } from "@peerbit/crypto";
import { Program } from "@peerbit/program";
import { SharedLog } from "@peerbit/shared-log";
import { equals } from "uint8arrays";
import type { Operation } from "./operation.js";

// Persisted protocol descriptor owned by Documents. Internal helpers are not
// package exports; callers select the store format through the constructor.
export type ProofDeleteContract = {
	readonly version: 1;
	readonly nonce: Uint8Array;
	readonly owner: Uint8Array;
	readonly keyKind: 0 | 1; // StringKey | Uint8ArrayKey
};
type DocumentLog = SharedLog<Operation, any, any>;
const contracts = new WeakMap<DocumentLog, ProofDeleteContract>();
const byteLength = Object.getOwnPropertyDescriptor(
	Object.getPrototypeOf(Uint8Array.prototype),
	"byteLength",
)!.get!;
const copyBytes = Uint8Array.prototype.set;
const copy32 = (value: Uint8Array): Uint8Array => {
	if (!(value instanceof Uint8Array) || byteLength.call(value) !== 32) {
		throw new Error("Expected 32-byte proof-delete contract field");
	}
	const copy = new Uint8Array(32);
	copyBytes.call(copy, value);
	return copy;
};
export const copyProofDeleteContract = (
	contract: ProofDeleteContract,
): ProofDeleteContract => {
	const { version, nonce, owner, keyKind } = contract;
	if (version !== 1 || (keyKind !== 0 && keyKind !== 1)) {
		throw new Error("Unsupported proof-delete owner/key contract");
	}
	return { version, nonce: copy32(nonce), owner: copy32(owner), keyKind };
};
const digestContract = (
	contract: ProofDeleteContract,
	domain: "log" | "scope",
): Uint8Array => {
	const writer = new BinaryWriter();
	writer.string(`peerbit/document/proof-delete/${domain}/v1`);
	writer.u8(contract.version);
	writer.set(contract.nonce);
	writer.set(contract.owner);
	writer.u8(contract.keyKind);
	return sha256Sync(writer.finalize());
};
const deriveLogId = (contract: ProofDeleteContract): Uint8Array =>
	digestContract(contract, "log");

// V1 fixes protocol authorization and overwrite/delete semantics. Presentation
// fields on an outer Documents/application manifest do not define another store.
// Scope and lower-log IDs bind the same descriptor with separate hash domains.
export const deriveProofDeleteScope = (
	contract: ProofDeleteContract,
): Uint8Array => digestContract(copyProofDeleteContract(contract), "scope");
const assertLogId = (log: DocumentLog, contract: ProofDeleteContract) => {
	if (!equals(log.log.id, deriveLogId(contract))) {
		throw new Error("Proof-delete contract/lower log identity mismatch");
	}
};

const isSharedLog = (value: unknown): value is DocumentLog => {
	const constructor = value?.constructor;
	return (
		constructor === SharedLog ||
		(typeof constructor === "function" && SharedLog.isPrototypeOf(constructor))
	);
};
const readLogVariant = (reader: BinaryReader): string => {
	const offset = reader._offset;
	try {
		if (reader.u8() !== 0) throw new Error("Expected document log manifest");
		return reader.string();
	} finally {
		reader._offset = offset;
	}
};
const sharedLogCodec: CustomField<DocumentLog> = {
	serialize(log, writer) {
		if (!isSharedLog(log)) throw new Error("Expected document SharedLog");
		serialize(log, writer);
	},
	deserialize(reader) {
		if (readLogVariant(reader) !== "shared_log") {
			throw new Error("Expected document SharedLog");
		}
		return deserializeStruct(SharedLog, false)(reader);
	},
};

// A wire representation only: runtime Documents still owns an ordinary SharedLog.
@variant("documents_proof_log_v1")
class ProofLogManifest extends Program {
	@field({ type: "u8" })
	version: 1;

	@field({ type: fixedArray("u8", 32) })
	nonce: Uint8Array;

	@field({ type: fixedArray("u8", 32) })
	owner: Uint8Array;

	@field({ type: "u8" })
	keyKind: 0 | 1;

	@field(sharedLogCodec)
	log: DocumentLog;

	constructor(log: DocumentLog, contract: ProofDeleteContract) {
		super();
		this.log = log;
		Object.assign(this, copyProofDeleteContract(contract));
	}

	async open(): Promise<void> {
		throw new Error(
			"Proof-delete log manifests cannot be opened independently",
		);
	}
}

export const proofDeleteLogCodec: CustomField<DocumentLog> = {
	serialize(log, writer) {
		if (!isSharedLog(log)) throw new Error("Expected document SharedLog");
		const contract = contracts.get(log);
		if (contract) assertLogId(log, contract);
		serialize(contract ? new ProofLogManifest(log, contract) : log, writer);
	},
	deserialize(reader: BinaryReader): DocumentLog {
		const discriminator = readLogVariant(reader);
		if (discriminator === "shared_log")
			return sharedLogCodec.deserialize(reader);
		if (discriminator !== "documents_proof_log_v1") {
			throw new Error("Expected document log manifest");
		}
		// Restrict dispatch before parsing fields of an unrelated registered Program.
		// Program's inherited Symbol.hasInstance cannot identify a concrete subtype.
		const value = deserializeStruct(ProofLogManifest, false)(reader);
		const contract = copyProofDeleteContract(value);
		assertLogId(value.log, contract);
		// Borsh may retain slices of the input block; the new contract/log must own
		// their bytes independently of that caller-owned deserialization buffer.
		const owned = deserialize(serialize(value.log), SharedLog) as DocumentLog;
		contracts.set(owned, contract);
		return owned;
	},
};

export const createProofDeleteLog = (
	contract: ProofDeleteContract,
): DocumentLog => {
	const owned = copyProofDeleteContract(contract);
	const log = new SharedLog<Operation>({ id: deriveLogId(owned) });
	contracts.set(log, owned);
	return log;
};

export const getProofDeleteContract = (
	log: DocumentLog,
): ProofDeleteContract | undefined => {
	const contract = contracts.get(log);
	if (!contract) return;
	assertLogId(log, contract);
	return copyProofDeleteContract(contract);
};
