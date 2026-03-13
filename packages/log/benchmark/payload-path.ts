import { field, serialize, variant } from "@dao-xyz/borsh";
import { AnyBlockStore } from "@peerbit/blocks";
import {
	DecryptedThing,
	Ed25519Keypair,
	createLocalEncryptProvider,
	randomBytes,
	ready,
} from "@peerbit/crypto";
import type { SignatureWithKey } from "@peerbit/crypto";
import {
	DataMessage,
	MessageHeader,
	SilentDelivery,
} from "@peerbit/stream-interface";
import { BORSH_ENCODING } from "../src/encoding.js";
import { EntryV0, Meta, Signatures } from "../src/entry-v0.js";
import { EntryType } from "../src/entry-type.js";
import { Payload } from "../src/payload.js";

@variant("bench-document")
class BenchDocument {
	@field({ type: "string" })
	id: string;

	@field({ type: Uint8Array })
	bytes: Uint8Array;

	constructor(props: { id: string; bytes: Uint8Array }) {
		this.id = props.id;
		this.bytes = props.bytes;
	}
}

@variant(3)
class BenchPutOperation {
	@field({ type: Uint8Array })
	data: Uint8Array;

	constructor(props: { data: Uint8Array }) {
		this.data = props.data;
	}
}

const BENCH_OPERATION_ENCODING = BORSH_ENCODING(BenchPutOperation);

const parseArgs = (argv: string[]) => {
	const out: Record<string, string> = {};
	for (let i = 0; i < argv.length; i++) {
		const arg = argv[i];
		if (arg === "--" || !arg.startsWith("--")) {
			continue;
		}
		const value = argv[i + 1];
		if (value == null || value.startsWith("--")) {
			throw new Error(`Missing value for ${arg}`);
		}
		out[arg.slice(2)] = value;
		i++;
	}
	return out;
};

const measure = async <T>(fn: () => Promise<T> | T) => {
	const started = performance.now();
	const value = await fn();
	return {
		value,
		ms: Number((performance.now() - started).toFixed(3)),
	};
};

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

const formatBytes = (value: number) => Number(value.toFixed(2));

const summarize = async (payloadBytes: number) => {
	const key = await Ed25519Keypair.create();
	const symmetricKey = randomBytes(32);
	const encryptionProvider = createLocalEncryptProvider(symmetricKey);
	const documentEncoding = BORSH_ENCODING(BenchDocument);
	const document = new BenchDocument({
		id: `doc-${payloadBytes}`,
		bytes: randomBytes(payloadBytes),
	});
	const store = new AnyBlockStore();
	await store.start();

	try {
		const encodedDocument = await measure(() => documentEncoding.encoder(document));
		const operation = new BenchPutOperation({ data: encodedDocument.value });
		const encodedOperation = await measure(() =>
			BENCH_OPERATION_ENCODING.encoder(operation),
		);
		const payload = new Payload({
			data: encodedOperation.value,
			value: operation,
			encoding: BENCH_OPERATION_ENCODING,
		});
		const encodedPayload = await measure(() => serialize(payload));
		const meta = new Meta({
			clock: new (await import("../src/clock.js")).LamportClock({
				id: key.publicKey.bytes,
			}),
			gid: "gid-bench",
			type: EntryType.APPEND,
			next: [],
		});
		const encodedMeta = await measure(() => serialize(meta));
		const unsignedEntry = new EntryV0<BenchPutOperation>({
			meta: new DecryptedThing({ data: encodedMeta.value, value: meta }),
			payload: new DecryptedThing({ data: encodedPayload.value, value: payload }),
			signatures: undefined,
			createdLocally: true,
		});
		const signable = await measure(() => unsignedEntry.getSignableBytes());
		const signature = await measure<Promise<SignatureWithKey>>(() =>
			key.sign(signable.value),
		);
		const encodedSignature = await measure(() => serialize(signature.value));
		const entry = new EntryV0<BenchPutOperation>({
			meta: unsignedEntry._meta,
			payload: unsignedEntry._payload,
			signatures: new Signatures({
				signatures: [
					new DecryptedThing({
						data: encodedSignature.value,
						value: signature.value,
					}),
				],
			}),
			createdLocally: true,
		});
		const encodedEntry = await measure(() => entry.getStorageBytes());
		const storedEntry = await measure(async () => store.put(encodedEntry.value));
		const message = new DataMessage({
			header: new MessageHeader({
				session: 1,
				mode: new SilentDelivery({ to: [], redundancy: 1 }),
			}),
			data: encodedEntry.value,
		});
		const genericMessageSignable = await measure(() =>
			serializeUnsignedMessage(message),
		);
		const messageSignable = await measure(() => message.getSignableBytes());
		if (
			genericMessageSignable.value.byteLength !==
				messageSignable.value.byteLength ||
			!genericMessageSignable.value.every(
				(byte, index) => byte === messageSignable.value[index],
			)
		) {
			throw new Error("Optimized message signable bytes do not match");
		}
		const encodedMessage = await measure(() => serialize(message));
		const encryptedMessage = await measure(() =>
			encryptionProvider(encodedMessage.value, { type: "hash" }),
		);

		const totalSerializedBytes =
			encodedDocument.value.byteLength +
			encodedOperation.value.byteLength +
			encodedPayload.value.byteLength +
			signable.value.byteLength +
			encodedEntry.value.byteLength +
			encodedMessage.value.byteLength +
			encryptedMessage.value.cipher.byteLength;

		return {
			payloadBytes,
			stages: {
				documentEncodeMs: encodedDocument.ms,
				operationEncodeMs: encodedOperation.ms,
				payloadEncodeMs: encodedPayload.ms,
				metaEncodeMs: encodedMeta.ms,
				signableEncodeMs: signable.ms,
				signMs: signature.ms,
				signatureEncodeMs: encodedSignature.ms,
				entryEncodeMs: encodedEntry.ms,
				blockPutMs: storedEntry.ms,
				messageSignableEncodeGenericMs: genericMessageSignable.ms,
				messageSignableEncodeMs: messageSignable.ms,
				messageEncodeMs: encodedMessage.ms,
				symmetricEncryptMs: encryptedMessage.ms,
			},
			sizes: {
				documentBytes: encodedDocument.value.byteLength,
				operationBytes: encodedOperation.value.byteLength,
				payloadBytes: encodedPayload.value.byteLength,
				signableBytes: signable.value.byteLength,
				entryBytes: encodedEntry.value.byteLength,
				messageBytes: encodedMessage.value.byteLength,
				encryptedBytes: encryptedMessage.value.cipher.byteLength,
			},
			amplification: {
				operationOverDocument: formatBytes(
					encodedOperation.value.byteLength / encodedDocument.value.byteLength,
				),
				payloadOverDocument: formatBytes(
					encodedPayload.value.byteLength / encodedDocument.value.byteLength,
				),
				entryOverDocument: formatBytes(
					encodedEntry.value.byteLength / encodedDocument.value.byteLength,
				),
				messageOverDocument: formatBytes(
					encodedMessage.value.byteLength / encodedDocument.value.byteLength,
				),
				totalSerializedOverDocument: formatBytes(
					totalSerializedBytes / encodedDocument.value.byteLength,
				),
			},
		};
	} finally {
		await store.stop();
	}
};

const main = async () => {
	await ready;
	const args = parseArgs(process.argv.slice(2));
	const sizes = (args.sizes ?? "4096,262144,1048576")
		.split(",")
		.map((value) => Number(value.trim()))
		.filter((value) => Number.isFinite(value) && value > 0);
	const results = [];
	for (const payloadBytes of sizes) {
		console.log(`Running payload-path benchmark payloadBytes=${payloadBytes}`);
		results.push(await summarize(payloadBytes));
	}

	console.table(
		results.map((result) => ({
			payloadBytes: result.payloadBytes,
			documentEncodeMs: result.stages.documentEncodeMs,
			operationEncodeMs: result.stages.operationEncodeMs,
			payloadEncodeMs: result.stages.payloadEncodeMs,
			signableEncodeMs: result.stages.signableEncodeMs,
			entryEncodeMs: result.stages.entryEncodeMs,
			blockPutMs: result.stages.blockPutMs,
			messageSignableEncodeGenericMs:
				result.stages.messageSignableEncodeGenericMs,
			messageSignableEncodeMs: result.stages.messageSignableEncodeMs,
			messageEncodeMs: result.stages.messageEncodeMs,
			symmetricEncryptMs: result.stages.symmetricEncryptMs,
			totalSerializedOverDocument:
				result.amplification.totalSerializedOverDocument,
		})),
	);
	console.log(JSON.stringify(results, null, 2));
};

main().catch((error) => {
	console.error(error);
	process.exitCode = 1;
});
