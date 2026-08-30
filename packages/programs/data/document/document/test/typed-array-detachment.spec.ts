import {
	type BinaryReader,
	type BinaryWriter,
	deserialize,
	field,
	serialize,
	variant,
	vec,
} from "@dao-xyz/borsh";
import { randomBytes } from "@peerbit/crypto";
import { Uint8ArrayKey, toId } from "@peerbit/indexer-interface";
import { Entry } from "@peerbit/log";
import { Program } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import {
	detachCanPerformCallbackProperties,
	detachEntryPayloadForCallback,
	detachOperationBytes,
} from "../src/callback-detachment.js";
import { createDocumentDomain } from "../src/domain.js";
import { policy, transform } from "../src/index.js";
import {
	DeleteByStringKeyOperation,
	DeleteOperation,
	Operation,
	PutOperation,
	PutWithKeyOperation,
	isPutOperation,
} from "../src/operation.js";
import { getCanPerformPolicyDescriptor } from "../src/policy.js";
import {
	type CanPerform,
	Documents,
	type SetupOptions,
} from "../src/program.js";

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

abstract class ByteChoice {}

@variant("typed_array_detachment_choice")
class ByteChoiceValue extends ByteChoice {
	@field({ type: Uint8Array })
	bytes: Uint8Array;

	constructor(bytes: Uint8Array) {
		super();
		this.bytes = bytes;
	}
}

@variant("typed_array_detachment_document")
class AliasingDocument {
	@field({ type: "string" })
	id: string;

	@field({ type: Uint8Array })
	direct: Uint8Array;

	@field({ type: vec(Uint8Array) })
	vectors: Uint8Array[];

	@field({ type: vec(ByteChoice) })
	choices: ByteChoice[];

	@field(byteMapField)
	mapped: Map<string, Uint8Array>;

	constructor(properties: {
		id: string;
		direct: Uint8Array;
		vectors: Uint8Array[];
		choices: ByteChoice[];
		mapped: Map<string, Uint8Array>;
	}) {
		this.id = properties.id;
		this.direct = properties.direct;
		this.vectors = properties.vectors;
		this.choices = properties.choices;
		this.mapped = properties.mapped;
	}
}

@variant("typed_array_detachment_projection")
class AliasingProjection {
	@field({ type: "string" })
	id: string;

	@field({ type: "u8" })
	marker: number;

	constructor(id: string, marker: number) {
		this.id = id;
		this.marker = marker;
	}
}

@variant("typed_array_detachment_delete_policy_projection")
class DeletePolicyProjection {
	@field({ type: "string" })
	id: string;

	@field({ type: Uint8Array })
	direct: Uint8Array;

	constructor(id: string, direct: Uint8Array) {
		this.id = id;
		this.direct = direct;
	}
}

@variant("typed_array_detachment_byte_id_document")
class ByteIdDocument {
	@field({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: Uint8Array })
	bytes: Uint8Array;

	constructor(id: Uint8Array, bytes: Uint8Array) {
		this.id = id;
		this.bytes = bytes;
	}
}

@variant("typed_array_detachment_byte_id_projection")
class ByteIdProjection {
	@field({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: "u8" })
	marker: number;

	constructor(id: Uint8Array, marker: number) {
		this.id = id;
		this.marker = marker;
	}
}

@variant("typed_array_detachment_store")
class AliasingStore<
	I extends Record<string, any> = AliasingDocument,
> extends Program<Partial<SetupOptions<AliasingDocument, I>>> {
	@field({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: Documents })
	docs: Documents<AliasingDocument, I>;

	constructor() {
		super();
		this.id = randomBytes(32);
		this.docs = new Documents<AliasingDocument, I>();
	}

	async open(
		options?: Partial<SetupOptions<AliasingDocument, I>>,
	): Promise<void> {
		await this.docs.open({
			...options,
			type: AliasingDocument,
			index: { ...options?.index, idProperty: "id" },
		});
	}
}

@variant("typed_array_detachment_byte_id_store")
class ByteIdStore extends Program<
	Partial<SetupOptions<ByteIdDocument, ByteIdProjection>>
> {
	@field({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: Documents })
	docs: Documents<ByteIdDocument, ByteIdProjection>;

	constructor() {
		super();
		this.id = randomBytes(32);
		this.docs = new Documents<ByteIdDocument, ByteIdProjection>();
	}

	async open(
		options?: Partial<SetupOptions<ByteIdDocument, ByteIdProjection>>,
	): Promise<void> {
		await this.docs.open({
			...options,
			type: ByteIdDocument,
			index: {
				...options?.index,
				idProperty: "id",
				type: ByteIdProjection,
				transform: (document) =>
					new ByteIdProjection(document.id, document.bytes[0]!),
			},
		});
	}
}

const createDocument = (id: string): AliasingDocument =>
	new AliasingDocument({
		id,
		direct: new Uint8Array([1, 2]),
		vectors: [new Uint8Array([3, 4])],
		choices: [new ByteChoiceValue(new Uint8Array([5, 6]))],
		mapped: new Map([["bytes", new Uint8Array([7, 8])]]),
	});

const projection = {
	type: AliasingProjection,
	transform: (document: AliasingDocument) =>
		new AliasingProjection(document.id, document.direct[0]!),
};

const firstBytes = (document: AliasingDocument): number[] => [
	document.direct[0]!,
	document.vectors[0]![0]!,
	(document.choices[0] as ByteChoiceValue).bytes[0]!,
	document.mapped.get("bytes")![0]!,
];

const mutateBytes = (document: AliasingDocument, marker: number): void => {
	document.direct[0] = marker;
	document.vectors[0]![0] = marker + 1;
	(document.choices[0] as ByteChoiceValue).bytes[0] = marker + 2;
	document.mapped.get("bytes")![0] = marker + 3;
};

const materialize = (entry: Entry<Operation>): Entry<Operation> =>
	(deserialize(serialize(entry), Entry) as Entry<Operation>).init(entry);

type EntryBytesSnapshot = {
	serialized: Uint8Array;
	payload: Uint8Array;
	operationData?: Uint8Array;
};

const snapshotEntryBytes = (entry: Entry<Operation>): EntryBytesSnapshot => {
	const operation = entry.payload.getValue();
	return {
		serialized: serialize(entry),
		payload: new Uint8Array(entry.payload.data),
		operationData: isPutOperation(operation)
			? new Uint8Array(operation.data)
			: undefined,
	};
};

const expectBytesUnchanged = (
	entry: Entry<Operation>,
	before: EntryBytesSnapshot,
): void => {
	expect(entry.payload.data).to.deep.equal(before.payload);
	const operation = entry.payload.getValue();
	if (before.operationData) {
		expect(isPutOperation(operation)).to.equal(true);
		expect((operation as PutOperation).data).to.deep.equal(
			before.operationData,
		);
	}
	expect(serialize(entry)).to.deep.equal(before.serialized);
};

const expectRejected = async (promise: Promise<unknown>): Promise<void> => {
	try {
		await promise;
	} catch {
		return;
	}
	throw new Error("Expected promise to reject");
};

const withPolicyDescriptor = <T>(
	callback: CanPerform<T>,
	descriptor: unknown,
): CanPerform<T> => {
	Object.defineProperty(
		callback,
		Symbol.for("@peerbit/document/native-can-perform-policy"),
		{ value: Object.freeze(descriptor) },
	);
	return callback;
};

describe("document callback typed-array detachment", () => {
	let session: TestSession;

	beforeEach(async () => {
		session = await TestSession.disconnected(2);
	});

	afterEach(async () => {
		await session.stop();
	});

	it("retains exact operation variants and extension fields", () => {
		const marker = Symbol("marker");
		class ExtendedPutOperation extends PutOperation {}
		const original = new ExtendedPutOperation({ data: new Uint8Array([1, 2]) });
		Object.defineProperty(original, "hidden", {
			value: 7,
			enumerable: false,
			writable: true,
		});
		(original as any)[marker] = "kept";
		const detached = detachOperationBytes(original);

		expect(detached).to.be.instanceOf(ExtendedPutOperation);
		expect(Object.getPrototypeOf(detached)).to.equal(
			Object.getPrototypeOf(original),
		);
		expect((detached as any).hidden).to.equal(7);
		expect(
			Object.getOwnPropertyDescriptor(detached, "hidden")?.enumerable,
		).to.equal(false);
		expect((detached as any)[marker]).to.equal("kept");
		expect(detached.data).not.to.equal(original.data);
		detached.data[0] = 9;
		expect(original.data).to.deep.equal(new Uint8Array([1, 2]));

		const legacy = deserialize(
			Uint8Array.from(
				"0000060000006c65676163790100000003"
					.match(/.{2}/g)!
					.map((byte) => Number.parseInt(byte, 16)),
			),
			Operation,
		) as PutWithKeyOperation;
		const detachedLegacy = detachOperationBytes(legacy);
		expect(detachedLegacy).to.be.instanceOf(PutWithKeyOperation);
		expect(detachedLegacy.key).to.equal("legacy");
		expect(detachedLegacy.data).not.to.equal(legacy.data);

		const legacyDelete = deserialize(
			Uint8Array.from(
				"0002060000006c6567616379"
					.match(/.{2}/g)!
					.map((byte) => Number.parseInt(byte, 16)),
			),
			Operation,
		) as DeleteByStringKeyOperation;
		const detachedLegacyDelete = detachOperationBytes(legacyDelete);
		expect(detachedLegacyDelete).to.be.instanceOf(DeleteByStringKeyOperation);
		expect(detachedLegacyDelete).not.to.equal(legacyDelete);
		(detachedLegacyDelete as any).key = "changed";
		expect(legacyDelete.key).to.equal("legacy");

		const deletion = new DeleteOperation({
			key: new Uint8ArrayKey(new Uint8Array([4, 5])),
		});
		const detachedDeletion = detachOperationBytes(deletion);
		expect(detachedDeletion).to.be.instanceOf(DeleteOperation);
		expect(detachedDeletion.key).not.to.equal(deletion.key);
		expect((detachedDeletion.key as Uint8ArrayKey).key).not.to.equal(
			(deletion.key as Uint8ArrayKey).key,
		);

		const stringDeletion = new DeleteOperation({ key: toId("signed") });
		const detachedStringDeletion = detachOperationBytes(stringDeletion);
		expect(detachedStringDeletion.key).not.to.equal(stringDeletion.key);
		(detachedStringDeletion.key as any).key = "changed";
		expect((stringDeletion.key as any).key).to.equal("signed");

		const frozenPut = Object.freeze(
			new PutOperation({ data: new Uint8Array([6, 7]) }),
		);
		const detachedFrozenPut = detachOperationBytes(frozenPut);
		expect(Object.isFrozen(detachedFrozenPut)).to.equal(true);
		expect(Object.isExtensible(detachedFrozenPut)).to.equal(
			Object.isExtensible(frozenPut),
		);
		expect(detachedFrozenPut.data).not.to.equal(frozenPut.data);

		const frozenKey = Object.freeze(new Uint8ArrayKey(new Uint8Array([8, 9])));
		const frozenDelete = Object.freeze(new DeleteOperation({ key: frozenKey }));
		const detachedFrozenDelete = detachOperationBytes(frozenDelete);
		expect(Object.isFrozen(detachedFrozenDelete)).to.equal(true);
		expect(Object.isFrozen(detachedFrozenDelete.key)).to.equal(true);
		expect(Object.isExtensible(detachedFrozenDelete)).to.equal(
			Object.isExtensible(frozenDelete),
		);
		expect(Object.isExtensible(detachedFrozenDelete.key)).to.equal(
			Object.isExtensible(frozenKey),
		);
		expect((detachedFrozenDelete.key as Uint8ArrayKey).key).not.to.equal(
			frozenKey.key,
		);
	});

	it("materializes callback operation and entry fields only when observed", () => {
		const createProperties = () => {
			let dataReads = 0;
			const operation = new Proxy(
				new PutOperation({ data: new Uint8Array([1, 2]) }),
				{
					get(target, property, receiver) {
						if (property === "data") {
							dataReads++;
						}
						return Reflect.get(target, property, receiver);
					},
				},
			);
			const entry = Object.create(Entry.prototype) as Entry<Operation>;
			const properties = detachCanPerformCallbackProperties({
				type: "put" as const,
				value: 1,
				operation,
				entry,
			});
			return { properties, operation, entry, dataReads: () => dataReads };
		};

		const observed = createProperties();
		expect(observed.properties.type).to.equal("put");
		expect(observed.properties.value).to.equal(1);
		expect(observed.dataReads()).to.equal(0);
		const callbackEntry = observed.properties.entry;
		expect(callbackEntry).not.to.equal(observed.entry);
		expect(observed.dataReads()).to.equal(0);
		const callbackOperation = observed.properties.operation;
		expect(callbackOperation).not.to.equal(observed.operation);
		expect(observed.properties.operation).to.equal(callbackOperation);
		expect(observed.dataReads()).to.equal(1);
		const operationDescriptor = Object.getOwnPropertyDescriptor(
			observed.properties,
			"operation",
		)!;
		expect(operationDescriptor.value).to.equal(callbackOperation);
		expect(operationDescriptor).to.include({
			configurable: true,
			enumerable: true,
			writable: true,
		});

		const reflected = createProperties();
		const reflectedOperation = Object.getOwnPropertyDescriptor(
			reflected.properties,
			"operation",
		)!.value;
		expect(reflectedOperation).not.to.equal(reflected.operation);
		expect(reflected.dataReads()).to.equal(1);

		const assigned = createProperties();
		const replacement = new PutOperation({ data: new Uint8Array([3]) });
		assigned.properties.operation = replacement;
		expect(assigned.properties.operation).to.equal(replacement);
		expect(assigned.dataReads()).to.equal(0);
		assigned.properties.entry = assigned.entry;
		expect(assigned.properties.entry).to.equal(assigned.entry);

		const deleted = createProperties();
		delete (deleted.properties as any).operation;
		delete (deleted.properties as any).entry;
		expect((deleted.properties as any).operation).to.equal(undefined);
		expect((deleted.properties as any).entry).to.equal(undefined);
		expect(deleted.dataReads()).to.equal(0);
	});

	it("isolates custom id and canPerform mutations from each other and projection", async () => {
		const source = await session.peers[0].open(
			new AliasingStore<AliasingProjection>(),
			{
				args: { replicate: false, index: projection },
			},
		);
		const idInputs: number[][] = [];
		const canPerformInputs: number[][] = [];
		const entryObservations: Array<{
			isEntry: boolean;
			hash: string;
			shallowHash: string;
			signatures: number;
			publicKeys: number;
			verified: boolean;
			materializedSame: boolean;
			constructorSame: boolean;
		}> = [];
		let callbackStorageBytes: Uint8Array | undefined;
		let callbackSerializedBytes: Uint8Array | undefined;
		const target = await session.peers[1].open(source.clone(), {
			args: {
				replicate: false,
				index: projection,
				id: (document: AliasingDocument) => {
					idInputs.push(firstBytes(document));
					mutateBytes(document, 20);
					return document.id;
				},
				canPerform: async (properties) => {
					if (properties.type === "put") {
						entryObservations.push({
							isEntry: properties.entry instanceof Entry,
							hash: properties.entry.hash,
							shallowHash: properties.entry.toShallow(true).hash,
							signatures: (await properties.entry.getSignatures()).length,
							publicKeys: (await properties.entry.getPublicKeys()).length,
							verified: await properties.entry.verifySignatures(),
							materializedSame:
								properties.entry.toMaterialized() === properties.entry,
							constructorSame:
								properties.entry.constructor === received.constructor,
						});
						callbackStorageBytes = properties.entry.getStorageBytes();
						callbackSerializedBytes = serialize(properties.entry);
						void (await properties.entry.getClock()).timestamp.wallTime;
						const valueOfEntry =
							properties.entry.valueOf() as typeof properties.entry;
						expect(valueOfEntry).to.equal(properties.entry);
						const valueOfOperation = await valueOfEntry.getPayloadValue();
						if (isPutOperation(valueOfOperation)) {
							valueOfOperation.data[0] ^= 0xff;
						}
						valueOfEntry.payload.data[0] ^= 0xff;
						properties.entry.toMaterialized().payload.data[0] ^= 0xff;
						canPerformInputs.push(firstBytes(properties.value));
						mutateBytes(properties.value, 30);
						properties.operation.data[0] ^= 0xff;
						const callbackEntryOperation =
							await properties.entry.getPayloadValue();
						if (isPutOperation(callbackEntryOperation)) {
							callbackEntryOperation.data[0] ^= 0xff;
						}
						properties.entry.payload.data[0] ^= 0xff;
					}
					return true;
				},
			},
		});
		const appended = await source.docs.put(createDocument("isolated"), {
			unique: true,
			replicate: false,
			target: "none",
		});
		const received = materialize(appended.entry);
		const serializedBefore = snapshotEntryBytes(received);

		await target.docs.log.log.join([received]);

		expect(idInputs.length).to.be.greaterThanOrEqual(2);
		expect(idInputs.every((bytes) => bytes.join() === "1,3,5,7")).to.equal(
			true,
		);
		expect(canPerformInputs).to.deep.equal([[1, 3, 5, 7]]);
		expect(entryObservations).to.deep.equal([
			{
				isEntry: true,
				hash: received.hash,
				shallowHash: received.hash,
				signatures: 1,
				publicKeys: 1,
				verified: true,
				materializedSame: true,
				constructorSame: true,
			},
		]);
		expect(callbackStorageBytes).to.deep.equal(serializedBefore.serialized);
		expect(callbackSerializedBytes).to.deep.equal(serializedBefore.serialized);
		expect(firstBytes((await target.docs.get("isolated"))!)).to.deep.equal([
			1, 3, 5, 7,
		]);
		expectBytesUnchanged(received, serializedBefore);
	});

	it("preserves the Entry interface for hollow entries without getPayload", async () => {
		const store = await session.peers[0].open(
			new AliasingStore<AliasingProjection>(),
			{ args: { replicate: false, index: projection } },
		);
		const appended = await store.docs.put(createDocument("hollow"), {
			unique: true,
			replicate: false,
			target: "none",
		});
		const materialized = appended.entry;
		const serializedBefore = snapshotEntryBytes(materialized);
		const rawStorageBytes = materialized.getStorageBytes();
		let materializeCalls = 0;
		let initCalls = 0;
		const hollow = Object.assign(Object.create(Entry.prototype), {
			hash: materialized.hash,
			size: materialized.size,
			createdLocally: materialized.createdLocally,
			init() {
				initCalls++;
				return this;
			},
			getMeta: () => materialized.getMeta(),
			getNext: () => materialized.getNext(),
			verifySignatures: () => materialized.verifySignatures(),
			getSignatures: () => materialized.getSignatures(),
			getClock: () => materialized.getClock(),
			equals: (other: Entry<Operation>) => materialized.equals(other),
			getPayloadValue: () => materialized.getPayloadValue(),
			toSignable: () => materialized,
			getSignableBytes: () => materialized.getSignableBytes(),
			getStorageBytes: () => rawStorageBytes,
			toMaterialized: () => {
				materializeCalls++;
				return materialized;
			},
			toShallow: (isHead: boolean) => materialized.toShallow(isHead),
		}) as Entry<Operation>;
		Object.defineProperties(hollow, {
			meta: { get: () => materialized.meta },
			payload: { get: () => materialized.payload },
			signatures: { get: () => materialized.signatures },
			publicKeys: { get: () => materialized.publicKeys },
		});
		const callbackEntry = detachEntryPayloadForCallback(hollow);

		expect(callbackEntry instanceof Entry).to.equal(true);
		expect((callbackEntry as any).getPayload).to.equal(undefined);
		const operation = await callbackEntry.getPayloadValue();
		expect(isPutOperation(operation)).to.equal(true);
		(operation as any).data[0] ^= 0xff;
		callbackEntry.toMaterialized().payload.data[0] ^= 0xff;
		callbackEntry.toSignable().payload.data[0] ^= 0xff;
		const callbackStorageBytes = callbackEntry.getStorageBytes();
		expect(callbackStorageBytes).not.to.equal(rawStorageBytes);
		callbackStorageBytes[0] ^= 0xff;
		expect(callbackEntry.init(materialized)).to.equal(callbackEntry);
		expect(materializeCalls).to.be.greaterThan(0);
		expect(initCalls).to.equal(1);
		expect(rawStorageBytes).to.deep.equal(serializedBefore.serialized);
		expectBytesUnchanged(materialized, serializedBefore);
	});

	it("uses canonical signed bytes across the local authorization boundary", async () => {
		const idInputs: number[][] = [];
		const canPerformInputs: number[][] = [];
		let callbackWasCaller = false;
		let callerDocument!: AliasingDocument;
		const store = await session.peers[0].open(
			new AliasingStore<AliasingProjection>(),
			{
				args: {
					replicate: false,
					index: projection,
					id: (document: AliasingDocument) => {
						idInputs.push(firstBytes(document));
						mutateBytes(document, idInputs.length === 1 ? 10 : 20);
						return document.id;
					},
					canPerform: (properties) => {
						if (properties.type === "put") {
							callbackWasCaller = properties.value === callerDocument;
							canPerformInputs.push(firstBytes(properties.value));
							mutateBytes(properties.value, 30);
							properties.operation.data[0] ^= 0xff;
						}
						return true;
					},
				},
			},
		);
		callerDocument = createDocument("local-reference");
		const appended = await store.docs.put(callerDocument, {
			unique: true,
			replicate: false,
			target: "none",
		});
		const serializedBefore = snapshotEntryBytes(appended.entry);
		const operation = await appended.entry.getPayloadValue();
		expect(isPutOperation(operation)).to.equal(true);
		const signedDocument = store.docs.index.valueEncoding.decoder(
			(operation as any).data,
		);

		expect(idInputs).to.deep.equal([
			[1, 3, 5, 7],
			[10, 11, 12, 13],
		]);
		expect(canPerformInputs).to.deep.equal([[10, 11, 12, 13]]);
		expect(callbackWasCaller).to.equal(false);
		expect(firstBytes(callerDocument)).to.deep.equal([10, 11, 12, 13]);
		expect(firstBytes(signedDocument)).to.deep.equal([10, 11, 12, 13]);
		expect(
			firstBytes((await store.docs.get("local-reference"))!),
		).to.deep.equal([10, 11, 12, 13]);
		expect(
			(await store.docs.index.index.get(toId("local-reference")))?.value.marker,
		).to.equal(10);
		expectBytesUnchanged(appended.entry, serializedBefore);
	});

	it("retries custom-id resolution from the original remote bytes", async () => {
		const source = await session.peers[0].open(
			new AliasingStore<AliasingProjection>(),
			{ args: { replicate: false, index: projection } },
		);
		const inputs: number[][] = [];
		let calls = 0;
		const target = await session.peers[1].open(source.clone(), {
			args: {
				replicate: false,
				index: projection,
				id: (document: AliasingDocument) => {
					inputs.push(firstBytes(document));
					mutateBytes(document, 70);
					calls++;
					if (calls === 1) {
						throw new Error("retry id callback");
					}
					return document.id;
				},
			},
		});
		const appended = await source.docs.put(createDocument("retry-id"), {
			unique: true,
			replicate: false,
			target: "none",
		});
		const received = materialize(appended.entry);
		const serializedBefore = snapshotEntryBytes(received);

		await expectRejected(target.docs.log.log.join([received]));
		expectBytesUnchanged(received, serializedBefore);
		await target.docs.log.log.join([received]);

		expect(inputs.length).to.be.greaterThanOrEqual(3);
		expect(inputs.every((bytes) => bytes.join() === "1,3,5,7")).to.equal(true);
		expect(firstBytes((await target.docs.get("retry-id"))!)).to.deep.equal([
			1, 3, 5, 7,
		]);
		expectBytesUnchanged(received, serializedBefore);
	});

	it("keeps explicit index-transform mutations while protecting entry bytes", async () => {
		const source = await session.peers[0].open(
			new AliasingStore<AliasingProjection>(),
			{
				args: {
					replicate: false,
					index: {
						type: AliasingProjection,
						transform: (document) =>
							new AliasingProjection(document.id, document.direct[0]!),
					},
				},
			},
		);
		const transformInputs: number[][] = [];
		const target = await session.peers[1].open(source.clone(), {
			args: {
				replicate: false,
				index: {
					type: AliasingProjection,
					transform: (document) => {
						transformInputs.push(firstBytes(document));
						mutateBytes(document, 40);
						return new AliasingProjection(document.id, document.direct[0]!);
					},
				},
			},
		});
		const appended = await source.docs.put(createDocument("transformed"), {
			unique: true,
			replicate: false,
			target: "none",
		});
		const received = materialize(appended.entry);
		const serializedBefore = snapshotEntryBytes(received);

		await target.docs.log.log.join([received]);

		expect(transformInputs).to.deep.equal([[1, 3, 5, 7]]);
		expect(firstBytes((await target.docs.get("transformed"))!)).to.deep.equal([
			40, 41, 42, 43,
		]);
		expect(
			(await target.docs.index.index.get(toId("transformed")))?.value.marker,
		).to.equal(40);
		expectBytesUnchanged(received, serializedBefore);
	});

	it("keeps event mutations off the entry payload", async () => {
		const source = await session.peers[0].open(
			new AliasingStore<AliasingProjection>(),
			{ args: { replicate: false, index: projection } },
		);
		const target = await session.peers[1].open(source.clone(), {
			args: { replicate: false, index: projection },
		});
		const eventInputs: number[][] = [];
		target.docs.events.addEventListener("change", (event) => {
			for (const document of event.detail.added) {
				eventInputs.push(firstBytes(document));
				mutateBytes(document, 60);
			}
		});
		const appended = await source.docs.put(
			createDocument("consumer-mutation"),
			{
				unique: true,
				replicate: false,
				target: "none",
			},
		);
		const received = materialize(appended.entry);
		const serializedBefore = snapshotEntryBytes(received);

		await target.docs.log.log.join([received]);
		expect(eventInputs).to.deep.equal([[1, 3, 5, 7]]);
		expectBytesUnchanged(received, serializedBefore);
	});

	it("retries from the original bytes after canPerform mutates and throws", async () => {
		const source = await session.peers[0].open(
			new AliasingStore<AliasingProjection>(),
			{
				args: { replicate: false, index: projection },
			},
		);
		const inputs: number[][] = [];
		let attempts = 0;
		const target = await session.peers[1].open(source.clone(), {
			args: {
				replicate: false,
				index: projection,
				canPerform: (properties) => {
					if (properties.type !== "put") {
						return true;
					}
					inputs.push(firstBytes(properties.value));
					mutateBytes(properties.value, 50);
					properties.operation.data[properties.operation.data.length - 1] ^=
						0xff;
					attempts++;
					if (attempts === 1) {
						throw new Error("retry callback");
					}
					return attempts > 2;
				},
			},
		});
		const appended = await source.docs.put(createDocument("retry"), {
			unique: true,
			replicate: false,
			target: "none",
		});
		const received = materialize(appended.entry);
		const serializedBefore = snapshotEntryBytes(received);
		const receivedOperation = await received.getPayloadValue();
		expect(isPutOperation(receivedOperation)).to.equal(true);
		const decodedDocuments = (target.docs as any)
			._canAppendDecodedDocuments as WeakMap<PutOperation, AliasingDocument>;

		await expectRejected(target.docs.log.log.join([received]));
		expect(decodedDocuments.has(receivedOperation as PutOperation)).to.equal(
			false,
		);
		expectBytesUnchanged(received, serializedBefore);
		await target.docs.log.log.join([received]);
		expect(decodedDocuments.has(receivedOperation as PutOperation)).to.equal(
			false,
		);
		expect(await target.docs.get("retry")).to.equal(undefined);
		expectBytesUnchanged(received, serializedBefore);
		await target.docs.log.log.join([received]);
		expect(decodedDocuments.has(receivedOperation as PutOperation)).to.equal(
			false,
		);

		const update = createDocument("retry");
		update.direct[0] = 2;
		const updated = await source.docs.put(update, {
			replicate: false,
			target: "none",
		});
		const rejectedUpdate = materialize(updated.entry);
		const rejectedOperation = await rejectedUpdate.getPayloadValue();
		expect(isPutOperation(rejectedOperation)).to.equal(true);
		target.docs.immutable = true;
		const attemptsBeforePreCallbackRejection = attempts;
		await target.docs.log.log.join([rejectedUpdate]);
		expect(attempts).to.equal(attemptsBeforePreCallbackRejection);
		expect(decodedDocuments.has(rejectedOperation as PutOperation)).to.equal(
			false,
		);
		expect(firstBytes((await target.docs.get("retry"))!)).to.deep.equal([
			1, 3, 5, 7,
		]);

		expect(inputs).to.deep.equal([
			[1, 3, 5, 7],
			[1, 3, 5, 7],
			[1, 3, 5, 7],
		]);
		expect(firstBytes((await target.docs.get("retry"))!)).to.deep.equal([
			1, 3, 5, 7,
		]);
		expectBytesUnchanged(received, serializedBefore);
	});

	it("isolates previous-entry payloads supplied to canPerform", async () => {
		const previousInputs: number[][] = [];
		let updateAttempts = 0;
		let store!: AliasingStore<AliasingProjection>;
		const canPerform = withPolicyDescriptor<AliasingDocument>(
			async (properties) => {
				if (properties.type !== "put" || !properties.previousEntries?.length) {
					return true;
				}
				const operation =
					await properties.previousEntries[0]!.getPayloadValue();
				if (!isPutOperation(operation)) {
					return false;
				}
				const document = store.docs.index.valueEncoding.decoder(
					operation.data,
				) as AliasingDocument;
				previousInputs.push(firstBytes(document));
				mutateBytes(document, 90);
				operation.data[0] ^= 0xff;
				properties.previousEntries[0]!.payload.data[0] ^= 0xff;
				updateAttempts++;
				if (updateAttempts === 1) {
					throw new Error("retry previous entry callback");
				}
				return true;
			},
			{ kind: "sameSignersAsPrevious" },
		);
		store = await session.peers[0].open(
			new AliasingStore<AliasingProjection>(),
			{
				args: { replicate: false, index: projection, canPerform },
			},
		);
		const first = await store.docs.put(createDocument("previous"), {
			unique: true,
			replicate: false,
			target: "none",
		});
		const firstSerialized = snapshotEntryBytes(first.entry);
		const update = createDocument("previous");
		update.direct[0] = 2;

		await expectRejected(
			store.docs.put(update, { replicate: false, target: "none" }),
		);
		expectBytesUnchanged(first.entry, firstSerialized);
		await store.docs.put(update, { replicate: false, target: "none" });

		expect(previousInputs).to.deep.equal([
			[1, 3, 5, 7],
			[1, 3, 5, 7],
		]);
		expectBytesUnchanged(first.entry, firstSerialized);
	});

	it("isolates rejected delete values, fallback decodes, and byte keys", async () => {
		const deleteInputs: number[] = [];
		let attempts = 0;
		const key = new Uint8Array([9, 8, 7]);
		const canPerform = withPolicyDescriptor<ByteIdDocument>(
			(properties) => {
				if (properties.type === "put") {
					return true;
				}
				deleteInputs.push(properties.value!.bytes[0]!);
				properties.value!.bytes[0] = 100;
				if (properties.operation.key instanceof Uint8ArrayKey) {
					properties.operation.key.key[0] = 0;
				}
				attempts++;
				if (attempts === 1) {
					return false;
				}
				if (attempts === 2) {
					throw new Error("retry delete callback");
				}
				return true;
			},
			{ kind: "deleteSignedByExistingField", path: "id" },
		);
		const store = await session.peers[0].open(new ByteIdStore(), {
			args: { replicate: false, canPerform },
		});
		const appended = await store.docs.put(
			new ByteIdDocument(key, new Uint8Array([1, 2])),
			{
				unique: true,
				replicate: false,
				target: "none",
			},
		);
		const serializedBefore = snapshotEntryBytes(appended.entry);

		await expectRejected(store.docs.del(key, { replicate: false }));
		expect(key).to.deep.equal(new Uint8Array([9, 8, 7]));
		expect((await store.docs.get(key))!.bytes).to.deep.equal(
			new Uint8Array([1, 2]),
		);
		expectBytesUnchanged(appended.entry, serializedBefore);

		const internals = store.docs as any;
		const getLocalIdentityDocumentByHead =
			internals.getLocalIdentityDocumentByHead.bind(internals);
		const getLocalIndexedDocumentForNativeDeletePolicy =
			internals.getLocalIndexedDocumentForNativeDeletePolicy.bind(internals);
		internals.getLocalIdentityDocumentByHead = async () => undefined;
		internals.getLocalIndexedDocumentForNativeDeletePolicy = async () =>
			undefined;
		await expectRejected(store.docs.del(key, { replicate: false }));
		expect(key).to.deep.equal(new Uint8Array([9, 8, 7]));
		expect((await store.docs.get(key))!.bytes).to.deep.equal(
			new Uint8Array([1, 2]),
		);
		expectBytesUnchanged(appended.entry, serializedBefore);

		internals.getLocalIdentityDocumentByHead = getLocalIdentityDocumentByHead;
		internals.getLocalIndexedDocumentForNativeDeletePolicy =
			getLocalIndexedDocumentForNativeDeletePolicy;
		await store.docs.del(key, { replicate: false });
		expect(deleteInputs).to.deep.equal([1, 1, 1]);
		expect(key).to.deep.equal(new Uint8Array([9, 8, 7]));
		expectBytesUnchanged(appended.entry, serializedBefore);
	});

	it("keeps scalar delete keys canonical across callback rejection and retry", async () => {
		const deleteKeys: string[] = [];
		let deleteAttempts = 0;
		const store = await session.peers[0].open(
			new AliasingStore<AliasingProjection>(),
			{
				args: {
					replicate: false,
					index: projection,
					canPerform: (properties) => {
						if (properties.type === "put") {
							return true;
						}
						deleteKeys.push(String((properties.operation.key as any).key));
						(properties.operation.key as any).key = "redirected-delete";
						(properties.operation as any).key = toId("reassigned-delete");
						deleteAttempts++;
						return deleteAttempts > 1;
					},
				},
			},
		);
		for (const id of [
			"scalar-delete",
			"redirected-delete",
			"reassigned-delete",
		]) {
			await store.docs.put(createDocument(id), {
				unique: true,
				replicate: false,
				target: "none",
			});
		}

		await expectRejected(store.docs.del("scalar-delete", { replicate: false }));
		expect(await store.docs.get("scalar-delete")).not.to.equal(undefined);
		await store.docs.del("scalar-delete", { replicate: false });

		expect(deleteKeys).to.deep.equal(["scalar-delete", "scalar-delete"]);
		expect(await store.docs.get("scalar-delete")).to.equal(undefined);
		expect(await store.docs.get("redirected-delete")).not.to.equal(undefined);
		expect(await store.docs.get("reassigned-delete")).not.to.equal(undefined);
	});

	it("detaches projected delete values without resolving the prior entry", async () => {
		const basePolicy = policy.or(
			policy.put(policy.allowAll<AliasingDocument>()),
			policy.deleteSignedByExistingField<AliasingDocument>("direct"),
		);
		let deleteAttempts = 0;
		const deleteInputs: number[] = [];
		const canPerform = withPolicyDescriptor<AliasingDocument>(
			async (properties) => {
				const allowed = await basePolicy(properties);
				if (properties.type === "delete" && properties.value) {
					deleteInputs.push(properties.value.direct[0]!);
					properties.value.direct[0] = 0;
					deleteAttempts++;
					return deleteAttempts > 1 && allowed;
				}
				return allowed;
			},
			getCanPerformPolicyDescriptor(basePolicy)!,
		);
		const store = await session.peers[0].open(
			new AliasingStore<DeletePolicyProjection>(),
			{
				args: {
					replicate: false,
					canPerform,
					index: {
						type: DeletePolicyProjection,
						transform: transform.pick<AliasingDocument, DeletePolicyProjection>(
							["id", "direct"],
						),
					},
				},
			},
		);
		const document = createDocument("projected-delete");
		document.direct = session.peers[0].identity.publicKey.bytes;
		const appended = await store.docs.put(document, {
			unique: true,
			replicate: false,
			target: "none",
		});
		const serializedBefore = snapshotEntryBytes(appended.entry);
		const internals = store.docs as any;
		internals.getLocalIdentityDocumentByHead = async () => undefined;
		let resolveEntryCalls = 0;
		const resolveEntry = internals._resolveEntry.bind(internals);
		internals._resolveEntry = async (...args: any[]) => {
			resolveEntryCalls++;
			if (resolveEntryCalls > 2) {
				throw new Error("delete policy must not resolve the prior entry");
			}
			return resolveEntry(...args);
		};

		await expectRejected(
			store.docs.del(document.id, {
				replicate: false,
				target: "none",
			}),
		);
		const indexed = (await store.docs.index.get(document.id, {
			local: true,
			remote: false,
			resolve: false,
		} as any)) as unknown as DeletePolicyProjection;
		expect(indexed.direct).to.deep.equal(
			session.peers[0].identity.publicKey.bytes,
		);
		await store.docs.del(document.id, {
			replicate: false,
			target: "none",
		});

		expect(deleteInputs).to.deep.equal([
			session.peers[0].identity.publicKey.bytes[0],
			session.peers[0].identity.publicKey.bytes[0],
		]);
		expect(resolveEntryCalls).to.equal(2);
		expect(await store.docs.get(document.id)).to.equal(undefined);
		expectBytesUnchanged(appended.entry, serializedBefore);
	});

	it("isolates keep and frozen class-domain entry callbacks", async () => {
		const callbackInputs: string[] = [];
		let keepReceiver: unknown;
		class PrivateDomain {
			#base: any;
			#calls = 0;

			constructor(base: any) {
				this.#base = base;
			}

			get type() {
				return this.#base.type;
			}

			get resolution() {
				return this.#base.resolution;
			}

			get calls() {
				return this.#calls;
			}

			get canMerge() {
				return this.#base.canMerge;
			}

			canProjectToOneSegment(request: any) {
				this.#calls++;
				return this.#base.canProjectToOneSegment(request);
			}

			fromArgs(args: any) {
				this.#calls++;
				return this.#base.fromArgs(args);
			}

			async fromEntry(entry: any) {
				this.#calls++;
				if (entry instanceof Entry) {
					callbackInputs.push("domain");
					const operation = await entry.getPayloadValue();
					if (isPutOperation(operation)) {
						operation.data[0] ^= 0xff;
					}
					entry.payload.data[0] ^= 0xff;
					entry.getStorageBytes()[0] ^= 0xff;
				}
				return 0;
			}
		}

		const store = await session.peers[0].open(
			new AliasingStore<AliasingProjection>(),
			{
				args: {
					replicate: false,
					index: projection,
					keep: async function (entry) {
						keepReceiver = this;
						if (entry instanceof Entry) {
							callbackInputs.push("keep");
							const operation = await entry.getPayloadValue();
							if (isPutOperation(operation)) {
								operation.data[0] ^= 0xff;
							}
							entry.payload.data[0] ^= 0xff;
							entry.getStorageBytes()[0] ^= 0xff;
						}
						return true;
					},
					domain: (db) => {
						const base = createDocumentDomain({
							resolution: "u32",
							canProjectToOneSegment: () => false,
							fromValue: () => 0,
						} as any)(db as any);
						const domain = new PrivateDomain(base);
						Object.defineProperty(domain, "fromEntry", {
							value: domain.fromEntry.bind(domain),
							configurable: false,
							writable: false,
						});
						return Object.freeze(domain) as any;
					},
				},
			},
		);
		const appended = await store.docs.put(createDocument("callback-options"), {
			unique: true,
			replicate: false,
			target: "none",
		});
		const serializedBefore = snapshotEntryBytes(appended.entry);
		const openedDomain = store.docs.log.domain as any;
		expect(openedDomain.constructor).to.equal(PrivateDomain);
		expect(openedDomain.resolution).to.equal("u32");
		expect(
			openedDomain.canProjectToOneSegment({ query: [], sort: [] }),
		).to.equal(false);
		const valueOfDomain = openedDomain.valueOf();
		expect(valueOfDomain).to.equal(openedDomain);
		await valueOfDomain.fromEntry(appended.entry);
		await openedDomain.fromEntry(appended.entry);
		const reflectedFromEntry = Object.getOwnPropertyDescriptor(
			openedDomain,
			"fromEntry",
		)?.value;
		expect(reflectedFromEntry).to.equal(openedDomain.fromEntry);
		await reflectedFromEntry(appended.entry);
		await (store.docs.log as any).keep(appended.entry);

		expect(callbackInputs).to.include("domain");
		expect(callbackInputs).to.include("keep");
		expect(keepReceiver).to.equal(store.docs.log);
		expect(openedDomain.calls).to.be.greaterThan(0);
		expect(await appended.entry.verifySignatures()).to.equal(true);
		expectBytesUnchanged(appended.entry, serializedBefore);

		const mutableStore = await session.peers[0].open(
			new AliasingStore<AliasingProjection>(),
			{
				args: {
					replicate: false,
					index: projection,
					domain: createDocumentDomain({
						resolution: "u32",
						canProjectToOneSegment: () => false,
						fromEntry: () => 0,
					} as any),
				},
			},
		);
		const mutableDomain = mutableStore.docs.log.domain as any;
		const stableFromEntry = mutableDomain.fromEntry;
		let replacementCalls = 0;
		mutableDomain.fromEntry = async function (entry: any) {
			replacementCalls++;
			const operation = await entry.getPayloadValue();
			if (isPutOperation(operation)) {
				operation.data[0] ^= 0xff;
			}
			return 0;
		};
		expect(mutableDomain.fromEntry).to.equal(stableFromEntry);
		await mutableDomain.fromEntry(appended.entry);
		expect(replacementCalls).to.equal(1);
		expectBytesUnchanged(appended.entry, serializedBefore);
	});

	it("isolates custom document-domain value and entry callbacks", async () => {
		const store = await session.peers[0].open(
			new AliasingStore<AliasingProjection>(),
			{ args: { replicate: false, index: projection } },
		);
		const appended = await store.docs.put(createDocument("domain"), {
			unique: true,
			replicate: false,
			target: "none",
		});
		const serializedBefore = snapshotEntryBytes(appended.entry);
		const valueInputs: number[][] = [];
		const fromValue = createDocumentDomain({
			resolution: "u32",
			canProjectToOneSegment: () => false,
			fromValue: (document: AliasingDocument | undefined, entry: any) => {
				valueInputs.push(firstBytes(document!));
				mutateBytes(document!, 110);
				entry.payload.data[0] ^= 0xff;
				return 0;
			},
		} as any)(store.docs as any);
		expect(await fromValue.fromEntry(appended.entry)).to.equal(0);
		expect(valueInputs).to.deep.equal([[1, 3, 5, 7]]);
		expectBytesUnchanged(appended.entry, serializedBefore);

		const fromEntry = createDocumentDomain({
			resolution: "u32",
			canProjectToOneSegment: () => false,
			fromEntry: (entry: any) => {
				entry.payload.data[0] ^= 0xff;
				return 1;
			},
		} as any)(store.docs as any);
		expect(await fromEntry.fromEntry(appended.entry)).to.equal(1);
		expectBytesUnchanged(appended.entry, serializedBefore);
	});
});
