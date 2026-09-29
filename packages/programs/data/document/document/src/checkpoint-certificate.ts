import {
	type Blocks,
	calculateRawCid,
	cidifyString,
	codecMap,
	defaultHasher,
	stringifyCid,
	verifyBlockBytes,
} from "@peerbit/blocks-interface";
import {
	Ed25519PublicKey,
	type Identity,
	PreHash,
	SignatureWithKey,
	verify,
} from "@peerbit/crypto";
import { equals, fromString, toString } from "uint8arrays";
import {
	boundCheckpointJSON,
	captureCheckpointBytes,
} from "./checkpoint-operation.js";

export const CHECKPOINT_MAX_WRITERS = 32;
export const CHECKPOINT_MAX_CERTIFICATE_BYTES = 16 * 1024;
const PROFILE = "peerbit-document-checkpoint-certificate-v1";
const APPROVAL_PROFILE = "peerbit-document-checkpoint-approval-v1";
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

export type CheckpointApproval = Readonly<{
	writer: Uint8Array;
	signature: Uint8Array;
}>;

export type CheckpointCertificate = Readonly<{
	cid: string;
	proposal: string;
	genesis: boolean;
	approvals: readonly CheckpointApproval[];
	/** Retention is not authority publication or a durability receipt. */
	retain(): Promise<void>;
}>;

type Certificate = {
	profile: typeof PROFILE;
	resource: string;
	proposal: string;
	genesis: boolean;
	approvals: Array<[string, string]>;
};

const encode = (value: unknown) => encoder.encode(JSON.stringify(value));
const exactBytes = (value: Uint8Array, length: number): Uint8Array => {
	const captured = captureCheckpointBytes(value, length);
	if (captured.byteLength !== length) {
		throw new Error(`Checkpoint certificate requires exactly ${length} bytes`);
	}
	return captured;
};

const assertCid = (value: unknown): string => {
	if (typeof value !== "string" || value.length === 0 || value.length > 128) {
		throw new Error("Invalid checkpoint proposal CID");
	}
	const cid = cidifyString(value);
	if (
		cid.version !== 1 ||
		cid.code !== codecMap.raw.code ||
		cid.multihash.code !== defaultHasher.code ||
		cid.multihash.digest.byteLength !== 32 ||
		stringifyCid(cid) !== value
	) {
		throw new Error(
			"Checkpoint certificate requires canonical raw SHA-256 CIDs",
		);
	}
	return value;
};

const captureWriters = (values: readonly Uint8Array[]): string[] => {
	if (
		!Array.isArray(values) ||
		values.length === 0 ||
		values.length > CHECKPOINT_MAX_WRITERS
	) {
		throw new Error("Invalid checkpoint writer roster size");
	}
	const writers: string[] = [];
	for (let index = 0; index < values.length; index++) {
		writers.push(toString(exactBytes(values[index]!, 32), "base16"));
	}
	writers.sort();
	if (new Set(writers).size !== writers.length) {
		throw new Error("Duplicate checkpoint writer");
	}
	return writers;
};

const approvalBytes = (resource: string, proposal: string): Uint8Array =>
	encode({ profile: APPROVAL_PROFILE, resource, proposal });

const verifyApprovals = async (
	certificate: Certificate,
	writers: readonly string[],
): Promise<void> => {
	if (certificate.genesis) {
		if (certificate.approvals.length !== 0) {
			throw new Error("Genesis checkpoint must not carry writer approvals");
		}
		return;
	}
	if (certificate.approvals.length !== writers.length) {
		throw new Error("Checkpoint requires an approval from every writer");
	}
	const signable = approvalBytes(certificate.resource, certificate.proposal);
	await Promise.all(
		certificate.approvals.map(async ([writer, signature], index) => {
			if (writer !== writers[index]) {
				throw new Error(
					"Checkpoint approvals differ from the fixed writer roster",
				);
			}
			if (
				!(await verify(
					new SignatureWithKey({
						publicKey: new Ed25519PublicKey({
							publicKey: fromString(writer, "base16"),
						}),
						signature: fromString(signature, "base16"),
						prehash: PreHash.NONE,
					}),
					signable,
				))
			) {
				throw new Error("Invalid checkpoint writer approval signature");
			}
		}),
	);
};

const certificateView = (
	blocks: Blocks,
	cid: string,
	certificate: Certificate,
	bytes: Uint8Array,
): CheckpointCertificate =>
	Object.freeze({
		cid,
		proposal: certificate.proposal,
		genesis: certificate.genesis,
		get approvals() {
			return Object.freeze(
				certificate.approvals.map(([writer, signature]) =>
					Object.freeze({
						writer: fromString(writer, "base16"),
						signature: fromString(signature, "base16"),
					}),
				),
			);
		},
		retain: async () => {
			const owned = new Uint8Array(bytes);
			const stored = blocks.putKnown
				? await blocks.putKnown(cid, owned)
				: await blocks.put(owned);
			if (stored !== cid) {
				throw new Error(
					"Checkpoint certificate store returned a different CID",
				);
			}
		},
	});

/**
 * The resource runtime must freeze writes durably and verify coverage of its
 * accepted frontier before requesting this signature. Signing alone does not
 * inspect local history and cannot certify that an omitted write is covered.
 */
export const signCheckpointApproval = async (properties: {
	resource: Uint8Array;
	proposal: string;
	identity: Identity<Ed25519PublicKey>;
}): Promise<CheckpointApproval> => {
	const resource = toString(exactBytes(properties.resource, 32), "base16");
	const proposal = assertCid(properties.proposal);
	const identity = properties.identity;
	if (!(identity.publicKey instanceof Ed25519PublicKey)) {
		throw new Error("Checkpoint writer must use Ed25519");
	}
	const writer = exactBytes(identity.publicKey.publicKey, 32);
	const key = new Ed25519PublicKey({ publicKey: writer });
	const signable = approvalBytes(resource, proposal);
	const signed = await identity.sign(new Uint8Array(signable), PreHash.NONE);
	const signature = exactBytes(signed.signature, 64);
	if (
		signed.prehash !== PreHash.NONE ||
		!key.equals(signed.publicKey) ||
		!(await verify(
			new SignatureWithKey({
				signature,
				publicKey: key,
				prehash: PreHash.NONE,
			}),
			signable,
		))
	) {
		throw new Error("Invalid checkpoint writer signature result");
	}
	return Object.freeze({ writer, signature });
};

/**
 * A genesis exemption is valid only after the resource separately authenticates
 * the proposal as epoch=0, previous=null and count=0. Normal seals require every
 * immutable writer, including an owner who is also a writer, to approve.
 */
export const createCheckpointCertificate = async (properties: {
	blocks: Blocks;
	resource: Uint8Array;
	proposal: string;
	writers: readonly Uint8Array[];
	approvals: readonly CheckpointApproval[];
	genesis?: boolean;
}): Promise<CheckpointCertificate> => {
	const blocks = properties.blocks;
	const resource = toString(exactBytes(properties.resource, 32), "base16");
	const proposal = assertCid(properties.proposal);
	const writers = captureWriters(properties.writers);
	const genesis = properties.genesis ?? false;
	if (typeof genesis !== "boolean") throw new Error("Invalid genesis marker");
	const supplied = properties.approvals;
	if (!Array.isArray(supplied) || supplied.length > CHECKPOINT_MAX_WRITERS) {
		throw new Error("Checkpoint approval capacity exceeded");
	}
	const approvals: Certificate["approvals"] = [];
	for (let index = 0; index < supplied.length; index++) {
		const approval = supplied[index]!;
		approvals.push([
			toString(exactBytes(approval.writer, 32), "base16"),
			toString(exactBytes(approval.signature, 64), "base16"),
		]);
	}
	approvals.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
	const certificate: Certificate = {
		profile: PROFILE,
		resource,
		proposal,
		genesis,
		approvals,
	};
	await verifyApprovals(certificate, writers);
	const bytes = encode(certificate);
	if (bytes.byteLength > CHECKPOINT_MAX_CERTIFICATE_BYTES) {
		throw new Error("Checkpoint certificate capacity exceeded");
	}
	const { cid } = await calculateRawCid(bytes);
	const result = certificateView(blocks, cid, certificate, bytes);
	await result.retain();
	return result;
};

/** Verifies the certificate only; the signed proposal must also be verified. */
export const readCheckpointCertificate = async (properties: {
	blocks: Blocks;
	cid: string;
	resource: Uint8Array;
	writers: readonly Uint8Array[];
	remote?: boolean;
}): Promise<CheckpointCertificate> => {
	const blocks = properties.blocks;
	const cid = assertCid(properties.cid);
	const resource = toString(exactBytes(properties.resource, 32), "base16");
	const writers = captureWriters(properties.writers);
	const bytes = await blocks.get(cid, {
		remote: properties.remote ? { replicate: false } : false,
	});
	if (!bytes) throw new Error(`Missing checkpoint certificate: ${cid}`);
	const owned = captureCheckpointBytes(bytes, CHECKPOINT_MAX_CERTIFICATE_BYTES);
	await verifyBlockBytes(cid, owned, { codec: codecMap.raw });
	boundCheckpointJSON(owned);
	const parsed = JSON.parse(decoder.decode(owned));
	if (
		parsed?.profile !== PROFILE ||
		parsed.resource !== resource ||
		typeof parsed.genesis !== "boolean" ||
		!Array.isArray(parsed.approvals) ||
		parsed.approvals.length > CHECKPOINT_MAX_WRITERS
	) {
		throw new Error("Invalid checkpoint certificate");
	}
	const proposal = assertCid(parsed.proposal);
	const approvals: Certificate["approvals"] = [];
	let previous: string | undefined;
	for (const approval of parsed.approvals) {
		if (
			!Array.isArray(approval) ||
			approval.length !== 2 ||
			typeof approval[0] !== "string" ||
			!/^[0-9a-f]{64}$/.test(approval[0]) ||
			typeof approval[1] !== "string" ||
			!/^[0-9a-f]{128}$/.test(approval[1]) ||
			(previous !== undefined && approval[0] <= previous)
		) {
			throw new Error("Invalid or unordered checkpoint approval");
		}
		previous = approval[0];
		approvals.push([approval[0], approval[1]]);
	}
	const certificate: Certificate = {
		profile: PROFILE,
		resource,
		proposal,
		genesis: parsed.genesis,
		approvals,
	};
	if (!equals(owned, encode(certificate))) {
		throw new Error("Noncanonical checkpoint certificate");
	}
	await verifyApprovals(certificate, writers);
	return certificateView(blocks, cid, certificate, owned);
};
