import { serialize } from "@dao-xyz/borsh";
import {
	Entry,
	EntryType,
	LamportClock,
	Meta,
	ShallowEntry,
	ShallowMeta,
	Timestamp,
} from "@peerbit/log";
import {
	EntryReplicatedU32,
	EntryReplicatedU64,
	RawExchangeHeadsMessage,
} from "@peerbit/shared-log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import {
	detachEntryForCallback,
	detachEntryPayloadForCallback,
} from "../src/callback-detachment.js";
import { createDocumentDomain } from "../src/domain.js";
import { Documents } from "../src/program.js";
import type { DocumentTransformFacts } from "../src/transform.js";
import { Document, TestStore } from "./data.js";

const copy = (value: Uint8Array | undefined): Uint8Array | undefined =>
	value == null ? undefined : new Uint8Array(value);

const mutate = (value: Uint8Array | undefined, marker: number): void => {
	if (value?.length) {
		value[0] = marker;
	}
};

describe("document callback signed-entry detachment", () => {
	let session: TestSession;

	beforeEach(async () => {
		session = await TestSession.disconnected(2);
	});

	afterEach(async () => {
		await session.stop();
	});

	it("isolates materialized metadata, signatures, public keys, and digest facts", async () => {
		const store = await session.peers[0].open(
			new TestStore({ docs: new Documents<Document>() }),
			{ args: { replicate: false } },
		);
		const appended = await store.docs.put(
			new Document({ id: "signed", data: new Uint8Array([1, 2]) }),
			{ unique: true, replicate: false, target: "none" },
		);
		const entry = appended.entry;
		const digest = Uint8Array.from({ length: 32 }, (_, index) => index + 1);
		(entry as any)._hashDigestBytes = digest;
		const canonicalDigest = copy(digest)!;

		const canonicalSignatures = await entry.getSignatures();
		const canonicalPublicKeys = await entry.getPublicKeys();
		const canonicalKey = canonicalPublicKeys[0]!;
		const canonicalKeyBytes = copy(canonicalKey.bytes)!;
		const canonicalRawKey = copy((canonicalKey as any).publicKey)!;
		const canonicalSignature = copy(canonicalSignatures[0]!.signature)!;
		const canonicalStorage = copy(entry.getStorageBytes())!;
		const canonicalSignable = copy(entry.getSignableBytes())!;
		const canonicalMetaBytes = copy((entry as any).getMetaBytes())!;
		const canonicalMetaData = copy(entry.meta.data);
		const canonicalClockId = copy(entry.meta.clock.id)!;
		const canonicalNext = [...entry.meta.next];
		const canonicalTimestamp = entry.meta.clock.timestamp.logical;
		const canonicalShallow = serialize(entry.toShallow(true));
		const canonicalIdentityBytes = copy(store.node.identity.publicKey.bytes)!;
		const hashNumber = (store.docs.log as any).getEntryHashNumber(entry);
		const coordinates = await store.docs.log.createCoordinates(entry, 2);
		const appendFacts = (store.docs.log.log as any).createPreparedAppendFacts([
			entry,
		])[0];
		const appendFactsDigest = copy(appendFacts.hashDigestBytes)!;

		const callbackEntry = detachEntryPayloadForCallback(entry);
		const callbackMeta = callbackEntry.meta;
		const callbackMetaAsync = await callbackEntry.getMeta();
		const callbackClock = await callbackEntry.getClock();
		expect(callbackMeta).to.equal(callbackMetaAsync);
		expect(callbackMeta.clock).to.equal(callbackClock);
		expect(callbackMeta).not.to.equal(entry.meta);
		expect(callbackMeta.constructor).to.equal(entry.meta.constructor);
		expect(callbackClock.constructor).to.equal(entry.meta.clock.constructor);

		mutate(callbackMeta.data, 101);
		mutate(callbackClock.id, 102);
		callbackMeta.next.push("callback-only");
		callbackClock.timestamp.logical += 1000;
		mutate((callbackEntry as any).getMetaBytes(), 103);
		mutate((callbackEntry as any).getHashDigestBytes(), 104);

		const callbackShallow = callbackEntry.toShallow(true);
		expect(callbackShallow).to.be.instanceOf(ShallowEntry);
		mutate(callbackShallow.meta.data, 105);
		mutate(callbackShallow.meta.clock.id, 106);
		callbackShallow.meta.next.push("shallow-only");

		const callbackSignatures = callbackEntry.signatures;
		const callbackSignaturesAsync = await callbackEntry.getSignatures();
		const callbackKeys = callbackEntry.publicKeys;
		const callbackKeysAsync = await callbackEntry.getPublicKeys();
		expect(callbackSignatures[0]).to.equal(callbackSignaturesAsync[0]);
		expect(callbackKeys[0]).to.equal(callbackKeysAsync[0]);
		expect(callbackSignatures[0]!.publicKey).to.equal(callbackKeys[0]);
		expect(callbackKeys[0]).not.to.equal(canonicalKey);
		expect(callbackKeys[0]!.constructor).to.equal(canonicalKey.constructor);
		mutate(callbackSignatures[0]!.signature, 107);
		mutate((callbackSignatures[0]!.publicKey as any).publicKey, 108);
		mutate(callbackKeys[0]!.bytes, 109);
		mutate((callbackKeys[0] as any).publicKey, 110);

		mutate(callbackEntry.toMaterialized().meta.data, 111);

		expect(entry.getStorageBytes()).to.deep.equal(canonicalStorage);
		expect(entry.getSignableBytes()).to.deep.equal(canonicalSignable);
		expect((entry as any).getMetaBytes()).to.deep.equal(canonicalMetaBytes);
		expect((entry as any).getHashDigestBytes()).to.deep.equal(canonicalDigest);
		expect(entry.meta.data).to.deep.equal(canonicalMetaData);
		expect(entry.meta.clock.id).to.deep.equal(canonicalClockId);
		expect(entry.meta.next).to.deep.equal(canonicalNext);
		expect(entry.meta.clock.timestamp.logical).to.equal(canonicalTimestamp);
		expect(serialize(entry.toShallow(true))).to.deep.equal(canonicalShallow);
		expect((await entry.getSignatures())[0]!.signature).to.deep.equal(
			canonicalSignature,
		);
		expect((await entry.getPublicKeys())[0]!.bytes).to.deep.equal(
			canonicalKeyBytes,
		);
		expect((await entry.getPublicKeys())[0] as any).to.have.property(
			"publicKey",
		);
		expect(((await entry.getPublicKeys())[0] as any).publicKey).to.deep.equal(
			canonicalRawKey,
		);
		expect(store.node.identity.publicKey.bytes).to.deep.equal(
			canonicalIdentityBytes,
		);
		expect(await entry.verifySignatures()).to.equal(true);
		expect((store.docs.log as any).getEntryHashNumber(entry)).to.equal(
			hashNumber,
		);
		expect(await store.docs.log.createCoordinates(entry, 2)).to.deep.equal(
			coordinates,
		);
		const appendFactsAfter = (
			store.docs.log.log as any
		).createPreparedAppendFacts([entry])[0];
		expect(appendFactsAfter.hashDigestBytes).to.deep.equal(appendFactsDigest);

		const next = await store.docs.put(new Document({ id: "next" }), {
			unique: true,
			replicate: false,
			target: "none",
		});
		expect(next.entry.meta.data).to.deep.equal(canonicalMetaData);
		expect(store.node.identity.publicKey.bytes).to.deep.equal(
			canonicalIdentityBytes,
		);
	});

	it("isolates signed entry state through local canPerform", async () => {
		let callbackCalls = 0;
		let metaData: Uint8Array | undefined;
		let clockId: Uint8Array | undefined;
		let metaBytes: Uint8Array | undefined;
		let digestBytes: Uint8Array | undefined;
		let signatureBytes: Uint8Array | undefined;
		let keyBytes: Uint8Array | undefined;
		let rawKeyBytes: Uint8Array | undefined;
		const identity = session.peers[0].identity.publicKey;
		const identityBytes = copy(identity.bytes)!;
		const identityRawKey = copy((identity as any).publicKey)!;
		const store = await session.peers[0].open(
			new TestStore({ docs: new Documents<Document>() }),
			{
				args: {
					replicate: false,
					canPerform: async (properties) => {
						if (properties.type !== "put") {
							return true;
						}
						callbackCalls++;
						const entry = properties.entry;
						const meta = await entry.getMeta();
						const clock = await entry.getClock();
						const signatures = await entry.getSignatures();
						const keys = await entry.getPublicKeys();
						expect(meta).to.equal(entry.meta);
						expect(clock).to.equal(meta.clock);
						expect(signatures[0]).to.equal(entry.signatures[0]);
						expect(keys[0]).to.equal(entry.publicKeys[0]);
						expect(signatures[0]!.publicKey).to.equal(keys[0]);
						metaData = copy(meta.data);
						clockId = copy(clock.id);
						metaBytes = copy((entry as any).getMetaBytes());
						digestBytes = copy((entry as any).getHashDigestBytes());
						signatureBytes = copy(signatures[0]!.signature);
						keyBytes = copy(keys[0]!.bytes);
						rawKeyBytes = copy((keys[0] as any).publicKey);
						mutate(meta.data, 120);
						mutate(clock.id, 121);
						meta.next.push("callback");
						mutate((entry as any).getMetaBytes(), 122);
						mutate((entry as any).getHashDigestBytes(), 123);
						mutate(signatures[0]!.signature, 124);
						mutate((signatures[0]!.publicKey as any).publicKey, 125);
						mutate(keys[0]!.bytes, 126);
						mutate((keys[0] as any).publicKey, 127);
						const shallow = entry.toShallow(true);
						mutate(shallow.meta.data, 128);
						mutate(shallow.meta.clock.id, 129);
						return true;
					},
				},
			},
		);
		const appended = await store.docs.put(
			new Document({ id: "local-policy", data: new Uint8Array([1]) }),
			{ unique: true, replicate: false, target: "none" },
		);
		expect(callbackCalls).to.equal(1);
		expect(appended.entry.meta.data).to.deep.equal(metaData);
		expect(appended.entry.meta.clock.id).to.deep.equal(clockId);
		expect((appended.entry as any).getMetaBytes()).to.deep.equal(metaBytes);
		expect((appended.entry as any).getHashDigestBytes()).to.deep.equal(
			digestBytes,
		);
		expect(appended.entry.signatures[0]!.signature).to.deep.equal(
			signatureBytes,
		);
		expect(appended.entry.publicKeys[0]!.bytes).to.deep.equal(keyBytes);
		expect((appended.entry.publicKeys[0] as any).publicKey).to.deep.equal(
			rawKeyBytes,
		);
		expect(identity.bytes).to.deep.equal(identityBytes);
		expect((identity as any).publicKey).to.deep.equal(identityRawKey);
		expect(await appended.entry.verifySignatures()).to.equal(true);
	});

	it("keeps prepared/hollow scalar metadata lazy and byte access synchronous", () => {
		const metaData = new Uint8Array([9, 8]);
		const clockId = new Uint8Array([7, 6]);
		const metaBytes = new Uint8Array([5, 4, 3]);
		const digestBytes = new Uint8Array([2, 1, 0]);
		let metaDataReads = 0;
		let clockIdReads = 0;
		let storageReads = 0;
		let materializeCalls = 0;

		const clock = Object.create(LamportClock.prototype) as LamportClock;
		Object.defineProperties(clock, {
			id: {
				configurable: true,
				enumerable: true,
				get: () => {
					clockIdReads++;
					return clockId;
				},
			},
			timestamp: {
				configurable: true,
				enumerable: true,
				value: new Timestamp({ wallTime: 10n, logical: 2 }),
				writable: true,
			},
		});
		const meta = Object.create(Meta.prototype) as Meta;
		Object.defineProperties(meta, {
			clock: { configurable: true, enumerable: true, value: clock },
			data: {
				configurable: true,
				enumerable: true,
				get: () => {
					metaDataReads++;
					return metaData;
				},
			},
			gid: { configurable: true, enumerable: true, value: "gid" },
			next: {
				configurable: true,
				enumerable: true,
				value: ["next"],
				writable: true,
			},
			type: {
				configurable: true,
				enumerable: true,
				value: EntryType.APPEND,
			},
		});
		const shallow = new ShallowEntry({
			hash: "prepared",
			head: true,
			payloadSize: 1,
			meta: new ShallowMeta({
				gid: "gid",
				next: ["next"],
				type: EntryType.APPEND,
				data: metaData,
				clock: new LamportClock({
					id: clockId,
					timestamp: new Timestamp({ wallTime: 10n, logical: 2 }),
				}),
			}),
		});
		Object.defineProperties(shallow, {
			getMetaBytes: { value: () => metaBytes, configurable: true },
			getHashDigestBytes: { value: () => digestBytes, configurable: true },
		});
		let hollow: Entry<unknown>;
		hollow = Object.assign(Object.create(Entry.prototype), {
			hash: "prepared",
			size: 1,
			init() {
				return this;
			},
			getMeta: () => meta,
			getClock: () => clock,
			getNext: () => meta.next,
			getMetaBytes: () => metaBytes,
			getHashDigestBytes: () => digestBytes,
			getSignatures: () => [],
			getPublicKeys: () => [],
			verifySignatures: () => true,
			equals: () => false,
			getPayloadValue: () => undefined,
			getStorageBytes: () => {
				storageReads++;
				return new Uint8Array([1]);
			},
			toMaterialized: () => {
				materializeCalls++;
				return hollow;
			},
			toSignable: () => hollow,
			toShallow: () => shallow,
		}) as Entry<unknown>;
		Object.defineProperties(hollow, {
			meta: { get: () => meta },
			signatures: { get: () => [] },
			publicKeys: { get: () => [] },
		});

		const callbackEntry = detachEntryPayloadForCallback(hollow);
		expect(callbackEntry.hash).to.equal("prepared");
		expect(callbackEntry.meta.gid).to.equal("gid");
		expect(metaDataReads).to.equal(0);
		expect(clockIdReads).to.equal(0);
		expect(storageReads).to.equal(0);
		expect(materializeCalls).to.equal(0);

		const callbackMeta = callbackEntry.getMeta() as Meta;
		const callbackClock = callbackEntry.getClock() as LamportClock;
		expect(callbackMeta).to.equal(callbackEntry.meta);
		expect(callbackClock).to.equal(callbackMeta.clock);
		expect(callbackClock.timestamp.wallTime).to.equal(10n);
		expect(metaDataReads).to.equal(0);
		expect(clockIdReads).to.equal(0);
		mutate(callbackClock.id, 20);
		mutate(callbackMeta.data, 21);
		expect(clockIdReads).to.equal(1);
		expect(metaDataReads).to.equal(1);
		expect(callbackEntry.getNext()).to.equal(callbackMeta.next);
		callbackMeta.next.push("callback");
		mutate((callbackEntry as any).getMetaBytes(), 22);
		mutate((callbackEntry as any).getHashDigestBytes(), 23);

		const callbackShallow = callbackEntry.toShallow(true) as ShallowEntry & {
			getMetaBytes(): Uint8Array;
			getHashDigestBytes(): Uint8Array;
		};
		mutate(callbackShallow.meta.data, 24);
		mutate(callbackShallow.meta.clock.id, 25);
		mutate(callbackShallow.getMetaBytes(), 26);
		mutate(callbackShallow.getHashDigestBytes(), 27);

		expect(metaData).to.deep.equal(new Uint8Array([9, 8]));
		expect(clockId).to.deep.equal(new Uint8Array([7, 6]));
		expect(meta.next).to.deep.equal(["next"]);
		expect(metaBytes).to.deep.equal(new Uint8Array([5, 4, 3]));
		expect(digestBytes).to.deep.equal(new Uint8Array([2, 1, 0]));
		expect(shallow.meta.data).to.deep.equal(new Uint8Array([9, 8]));
		expect(shallow.meta.clock.id).to.deep.equal(new Uint8Array([7, 6]));
		expect(storageReads).to.equal(0);
		expect(materializeCalls).to.equal(0);
	});

	it("isolates real prepared raw facts without forcing JS entry decode", async () => {
		const profileEvents: any[] = [];
		const profile = (event: any) => profileEvents.push(event);
		const countLazyDecodes = () =>
			profileEvents.filter(
				(event) =>
					event.name === "sharedLog.rawReceive.jsEntryDecode" &&
					event.details?.lazy === true,
			).length;
		let callbackCalls = 0;
		let callbackLazyDecodesAtEntry = -1;
		let scalarLazyDecodes = -1;
		let signatureLazyDecodes = -1;
		let callbackMetaData: Uint8Array | undefined;
		let callbackClockId: Uint8Array | undefined;
		let callbackMetaBytes: Uint8Array | undefined;
		let callbackDigest: Uint8Array | undefined;
		let callbackSignatureBytes: Uint8Array | undefined;
		let callbackKeyBytes: Uint8Array | undefined;

		const source = await session.peers[0].open(
			new TestStore({ docs: new Documents<Document>() }),
			{ args: { replicate: false, nativeGraph: true } },
		);
		const target = await session.peers[1].open(source.clone(), {
			args: {
				replicate: false,
				nativeGraph: true,
				sync: { rawExchangeHeads: true, profile },
				canPerform: async (properties) => {
					if (properties.type !== "put") {
						return true;
					}
					callbackCalls++;
					const entry = properties.entry;
					callbackLazyDecodesAtEntry = countLazyDecodes();
					const callbackMeta = await entry.getMeta();
					const callbackClock = await entry.getClock();
					const callbackNext = await entry.getNext();
					expect(callbackMeta).to.equal(entry.meta);
					expect(callbackClock).to.equal(callbackMeta.clock);
					expect(callbackNext).to.equal(callbackMeta.next);
					callbackMetaData = copy(callbackMeta.data);
					callbackClockId = copy(callbackClock.id);
					callbackMetaBytes = copy((entry as any).getMetaBytes());
					callbackDigest = copy((entry as any).getHashDigestBytes());
					const callbackShallow = entry.toShallow(true) as ShallowEntry & {
						getMetaBytes(): Uint8Array;
						getHashDigestBytes(): Uint8Array;
					};
					mutate(callbackMeta.data, 80);
					mutate(callbackClock.id, 81);
					callbackMeta.next.push("callback");
					mutate((entry as any).getMetaBytes(), 82);
					mutate((entry as any).getHashDigestBytes(), 83);
					mutate(callbackShallow.meta.data, 84);
					mutate(callbackShallow.meta.clock.id, 85);
					mutate(callbackShallow.getMetaBytes(), 86);
					mutate(callbackShallow.getHashDigestBytes(), 87);
					scalarLazyDecodes = countLazyDecodes();

					const callbackSignatures = await entry.getSignatures();
					const callbackKeys = await entry.getPublicKeys();
					expect(callbackSignatures[0]!.publicKey).to.equal(callbackKeys[0]);
					callbackSignatureBytes = copy(callbackSignatures[0]!.signature);
					callbackKeyBytes = copy(callbackKeys[0]!.bytes);
					mutate(callbackSignatures[0]!.signature, 88);
					mutate((callbackSignatures[0]!.publicKey as any).publicKey, 89);
					mutate(callbackKeys[0]!.bytes, 90);
					mutate((callbackKeys[0] as any).publicKey, 91);
					signatureLazyDecodes = countLazyDecodes();
					return true;
				},
			},
		});

		const appended = await source.docs.put(
			new Document({ id: "prepared", data: new Uint8Array([1, 2]) }),
			{ unique: true, replicate: false, target: "none" },
		);
		const rawStorage = await source.docs.log.log.blocks.get(
			appended.entry.hash,
		);
		expect(rawStorage).to.not.equal(undefined);
		const sourceStorageBytes = copy(appended.entry.getStorageBytes())!;
		const sourceHashNumber = (source.docs.log as any).getEntryHashNumber(
			appended.entry,
		);
		const sourceCoordinates = await source.docs.log.createCoordinates(
			appended.entry,
			2,
		);
		const rawMessage = new RawExchangeHeadsMessage({
			heads: [
				{
					hash: appended.entry.hash,
					bytes: rawStorage!,
					gidRefrences: [],
				} as any,
			],
		});
		const materializedRaw = await (
			target.docs.log as any
		).materializeRawReceiveMessage(rawMessage, {
			from: source.node.identity.publicKey,
			syncProfile: profile,
			receiveOwnershipRevision:
				(target.docs.log as any)._instanceLifecycle
					?._receiveOwnershipRevision ?? 0,
		});
		const prepared = materializedRaw.message.heads[0].entry as Entry<unknown>;
		const canonicalMetaData = copy(prepared.meta.data);
		const canonicalClockId = copy(prepared.meta.clock.id)!;
		const canonicalNext = [...prepared.meta.next];
		const canonicalMetaBytes = copy((prepared as any).getMetaBytes())!;
		const canonicalDigest = copy((prepared as any).getHashDigestBytes())!;
		const canonicalShallow = serialize(prepared.toShallow(true));
		expect(countLazyDecodes()).to.equal(0);

		const detachedPrepared = detachEntryPayloadForCallback(prepared);
		const detachedMeta = detachedPrepared.getMeta() as Meta;
		const detachedClock = detachedPrepared.getClock() as LamportClock;
		expect(detachedMeta).to.equal(detachedPrepared.meta);
		expect(detachedClock).to.equal(detachedMeta.clock);
		mutate(detachedMeta.data, 60);
		mutate(detachedClock.id, 61);
		detachedMeta.next.push("detached");
		mutate((detachedPrepared as any).getMetaBytes(), 62);
		mutate((detachedPrepared as any).getHashDigestBytes(), 63);
		const detachedShallow = detachedPrepared.toShallow(true) as ShallowEntry & {
			getMetaBytes(): Uint8Array;
			getHashDigestBytes(): Uint8Array;
		};
		mutate(detachedShallow.meta.data, 64);
		mutate(detachedShallow.meta.clock.id, 65);
		mutate(detachedShallow.getMetaBytes(), 66);
		mutate(detachedShallow.getHashDigestBytes(), 67);
		const detachedPreparedFacts = (
			detachedPrepared as any
		).toPreparedAppendJoinFacts();
		expect(detachedPreparedFacts.getShallowEntry()).to.equal(detachedShallow);
		expect(detachedPreparedFacts.materializeEntry()).to.equal(detachedPrepared);
		mutate(detachedPreparedFacts.bytes, 72);
		mutate(detachedPreparedFacts.meta.data, 73);
		mutate(detachedPreparedFacts.meta.clock.id, 74);
		detachedPreparedFacts.meta.next.push("facts");
		mutate(detachedPreparedFacts.getShallowEntry().meta.data, 75);
		const detachedInternalNext = (detachedPrepared as any)
			.__peerbitNext as string[];
		detachedInternalNext.push("internal");
		expect(countLazyDecodes()).to.equal(0);
		expect(prepared.meta.data).to.deep.equal(canonicalMetaData);
		expect(prepared.meta.clock.id).to.deep.equal(canonicalClockId);
		expect(prepared.meta.next).to.deep.equal(canonicalNext);
		expect(prepared.getStorageBytes()).to.deep.equal(rawStorage);
		expect((prepared as any).getMetaBytes()).to.deep.equal(canonicalMetaBytes);
		expect((prepared as any).getHashDigestBytes()).to.deep.equal(
			canonicalDigest,
		);
		expect(serialize(prepared.toShallow(true))).to.deep.equal(canonicalShallow);

		const detachedSignatures =
			(await detachedPrepared.getSignatures()) as any[];
		const detachedKeys = (await detachedPrepared.getPublicKeys()) as any[];
		mutate(detachedSignatures[0]!.signature, 68);
		mutate(detachedSignatures[0]!.publicKey.publicKey, 69);
		mutate(detachedKeys[0]!.bytes, 70);
		mutate(detachedKeys[0]!.publicKey, 71);
		expect(countLazyDecodes()).to.equal(1);
		expect(prepared.getStorageBytes()).to.deep.equal(rawStorage);
		expect(await prepared.verifySignatures()).to.equal(true);
		expect((target.docs.log as any).getEntryHashNumber(prepared)).to.equal(
			sourceHashNumber,
		);
		expect(await target.docs.log.createCoordinates(prepared, 2)).to.deep.equal(
			sourceCoordinates,
		);
		const appendFactsAfterScalar = (
			target.docs.log.log as any
		).createPreparedAppendFacts([prepared])[0];
		expect(appendFactsAfterScalar.hashDigestBytes).to.deep.equal(
			canonicalDigest,
		);

		await target.docs.log.log.join([prepared as any]);
		expect(callbackCalls).to.equal(1);
		expect(scalarLazyDecodes).to.equal(callbackLazyDecodesAtEntry);
		expect(signatureLazyDecodes).to.equal(scalarLazyDecodes);
		const received = await target.docs.log.log.get(appended.entry.hash);
		expect(received).to.not.equal(undefined);
		expect(received!.meta.data).to.deep.equal(callbackMetaData);
		expect(received!.meta.clock.id).to.deep.equal(callbackClockId);
		expect((received as any).getMetaBytes()).to.deep.equal(callbackMetaBytes);
		expect(callbackDigest).to.deep.equal(canonicalDigest);
		expect(received!.signatures[0]!.signature).to.deep.equal(
			callbackSignatureBytes,
		);
		expect(received!.publicKeys[0]!.bytes).to.deep.equal(callbackKeyBytes);
		expect(copy(received!.getStorageBytes())).to.deep.equal(sourceStorageBytes);
		expect(await received!.verifySignatures()).to.equal(true);
		expect((target.docs.log as any).getEntryHashNumber(received)).to.equal(
			sourceHashNumber,
		);
		expect(await target.docs.log.createCoordinates(received!, 2)).to.deep.equal(
			sourceCoordinates,
		);
		expect(await target.docs.get("prepared")).to.not.equal(undefined);
	});

	it("isolates shallow and replicated entry callback containers", () => {
		const createMeta = () =>
			new ShallowMeta({
				gid: "gid",
				next: ["next"],
				type: EntryType.APPEND,
				data: new Uint8Array([1, 2]),
				clock: new LamportClock({
					id: new Uint8Array([3, 4]),
					timestamp: new Timestamp({ wallTime: 5n, logical: 6 }),
				}),
			});
		const metaBytes = new Uint8Array([7, 8]);
		const digestBytes = new Uint8Array([9, 10]);
		const shallow = new ShallowEntry({
			hash: "shallow",
			head: true,
			payloadSize: 1,
			meta: createMeta(),
		}) as ShallowEntry & {
			getMetaBytes(): Uint8Array;
			getHashDigestBytes(): Uint8Array;
		};
		Object.defineProperties(shallow, {
			getMetaBytes: { value: () => metaBytes, configurable: true },
			getHashDigestBytes: { value: () => digestBytes, configurable: true },
		});
		const callbackShallow = detachEntryForCallback(shallow);
		mutate(callbackShallow.meta.data, 20);
		mutate(callbackShallow.meta.clock.id, 21);
		callbackShallow.meta.next.push("callback");
		mutate(callbackShallow.getMetaBytes(), 22);
		mutate(callbackShallow.getHashDigestBytes(), 23);
		expect(shallow.meta.data).to.deep.equal(new Uint8Array([1, 2]));
		expect(shallow.meta.clock.id).to.deep.equal(new Uint8Array([3, 4]));
		expect(shallow.meta.next).to.deep.equal(["next"]);
		expect(metaBytes).to.deep.equal(new Uint8Array([7, 8]));
		expect(digestBytes).to.deep.equal(new Uint8Array([9, 10]));

		const replicatedU32 = new EntryReplicatedU32({
			assignedToRangeBoundary: false,
			coordinates: [1, 2],
			hash: "u32",
			hashNumber: 3,
			meta: createMeta(),
		});
		const replicatedU64 = new EntryReplicatedU64({
			assignedToRangeBoundary: false,
			coordinates: [1n, 2n],
			hash: "u64",
			hashNumber: 3n,
			meta: createMeta(),
		});
		for (const replicated of [replicatedU32, replicatedU64]) {
			const canonicalReplicatedMetaBytes = copy(replicated.getMetaBytes())!;
			const enumerableKeys = Object.keys(replicated);
			expect((replicated as any)._metaResolved).to.equal(undefined);
			const callbackReplicated = detachEntryForCallback(replicated) as any;
			expect(Object.keys(callbackReplicated)).to.deep.equal(enumerableKeys);
			expect(
				Object.getOwnPropertyDescriptor(callbackReplicated, "meta")?.enumerable,
			).to.equal(false);
			expect(
				Object.getOwnPropertyDescriptor(callbackReplicated, "getMetaBytes")
					?.enumerable,
			).to.equal(false);
			expect((replicated as any)._metaResolved).to.equal(undefined);
			callbackReplicated.coordinates[0] =
				typeof callbackReplicated.coordinates[0] === "bigint" ? 99n : 99;
			mutate(callbackReplicated.meta.data, 30);
			mutate(callbackReplicated.meta.clock.id, 31);
			mutate(callbackReplicated.getMetaBytes(), 32);
			expect(replicated.coordinates[0]).to.equal(
				replicated instanceof EntryReplicatedU64 ? 1n : 1,
			);
			expect(replicated.meta.data).to.deep.equal(new Uint8Array([1, 2]));
			expect(replicated.meta.clock.id).to.deep.equal(new Uint8Array([3, 4]));
			expect(replicated.getMetaBytes()).to.deep.equal(
				canonicalReplicatedMetaBytes,
			);
		}

		for (const integrity of ["sealed", "frozen"] as const) {
			const replicated = new EntryReplicatedU32({
				assignedToRangeBoundary: false,
				coordinates: [1, 2],
				hash: integrity,
				hashNumber: 3,
				meta: createMeta(),
			});
			const canonicalMetaBytes = copy(replicated.getMetaBytes())!;
			const enumerableKeys = Object.keys(replicated);
			expect((replicated as any)._metaResolved).to.equal(undefined);
			integrity === "frozen"
				? Object.freeze(replicated)
				: Object.seal(replicated);
			const callbackReplicated = detachEntryForCallback(replicated);
			expect(Object.keys(callbackReplicated)).to.deep.equal(enumerableKeys);
			expect(Object.isSealed(callbackReplicated)).to.equal(true);
			expect(Object.isFrozen(callbackReplicated)).to.equal(
				integrity === "frozen",
			);
			expect((replicated as any)._metaResolved).to.equal(undefined);
			mutate(callbackReplicated.getMetaBytes(), 40);
			expect(replicated.getMetaBytes()).to.deep.equal(canonicalMetaBytes);
		}
	});

	it("isolates signer facts passed to arbitrary index transforms", async () => {
		const identity = session.peers[0].identity.publicKey;
		const identityBytes = copy(identity.bytes)!;
		const identityRawKey = copy((identity as any).publicKey)!;
		let signerCallbacks = 0;
		const transformDocument = (
			document: Document,
			_context: unknown,
			facts?: DocumentTransformFacts,
		) => {
			const signer = facts?.entryPublicKeys?.[0];
			if (signer) {
				signerCallbacks++;
				expect(signer).not.to.equal(identity);
				expect(signer.equals(identity)).to.equal(true);
				mutate((signer as any).publicKey, 40);
				mutate(signer.bytes, 41);
			}
			return new Document(document);
		};
		Object.defineProperty(
			transformDocument,
			Symbol.for("@peerbit/document/native-document-transform"),
			{
				value: Object.freeze({ kind: "pick", fields: ["id"] }),
			},
		);
		const store = await session.peers[0].open(
			new TestStore({ docs: new Documents<Document>() }),
			{
				args: {
					replicate: false,
					index: {
						type: Document,
						transform: transformDocument,
					},
				},
			},
		);
		const appended = await store.docs.put(new Document({ id: "transform" }), {
			unique: true,
			replicate: false,
			target: "none",
		});
		expect(signerCallbacks).to.be.greaterThan(0);
		expect(identity.bytes).to.deep.equal(identityBytes);
		expect((identity as any).publicKey).to.deep.equal(identityRawKey);
		expect(await appended.entry.verifySignatures()).to.equal(true);
	});

	it("routes custom keep and domain entry-like callbacks through detached views", async () => {
		let keepCalls = 0;
		let domainCalls = 0;
		const mutateEntryLike = (entry: any, marker: number) => {
			mutate(entry.meta?.data, marker);
			mutate(entry.meta?.clock?.id, marker + 1);
			entry.meta?.next?.push("callback");
			mutate(entry.getMetaBytes?.(), marker + 2);
			mutate(entry.getHashDigestBytes?.(), marker + 3);
		};
		const store = await session.peers[0].open(
			new TestStore({ docs: new Documents<Document>() }),
			{
				args: {
					replicate: false,
					keep: (entry) => {
						keepCalls++;
						mutateEntryLike(entry, 50);
						return true;
					},
					domain: createDocumentDomain({
						resolution: "u32",
						canProjectToOneSegment: () => false,
						fromEntry: (entry: any) => {
							domainCalls++;
							mutateEntryLike(entry, 60);
							return 0;
						},
					} as any),
				},
			},
		);
		const appended = await store.docs.put(new Document({ id: "callbacks" }), {
			unique: true,
			replicate: false,
			target: "none",
		});
		const shallow = appended.entry.toShallow(true) as ShallowEntry & {
			getMetaBytes(): Uint8Array;
			getHashDigestBytes(): Uint8Array;
		};
		const shallowMetaBytes = serialize(shallow.meta);
		const shallowDigest = new Uint8Array([70, 71]);
		Object.defineProperties(shallow, {
			getMetaBytes: { value: () => shallowMetaBytes, configurable: true },
			getHashDigestBytes: { value: () => shallowDigest, configurable: true },
		});
		const canonicalShallow = serialize(shallow);

		await (store.docs.log as any).keep(shallow);
		await (store.docs.log.domain as any).fromEntry(shallow);
		expect(keepCalls).to.be.greaterThan(0);
		expect(domainCalls).to.be.greaterThan(0);
		expect(serialize(shallow)).to.deep.equal(canonicalShallow);
		expect(shallowMetaBytes).to.deep.equal(serialize(shallow.meta));
		expect(shallowDigest).to.deep.equal(new Uint8Array([70, 71]));
	});
});
