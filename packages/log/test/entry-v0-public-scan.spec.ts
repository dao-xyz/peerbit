import { serialize } from "@dao-xyz/borsh";
import { verify } from "@peerbit/crypto";
import { expect } from "chai";
import {
	type CanonicalPublicEntryV0ScanLimits,
	Entry,
	EntryType,
	EntryV0,
	LamportClock,
	Timestamp,
	scanCanonicalPublicEntryV0,
} from "../src/index.js";
import { signKey, signKey2 } from "./fixtures/privateKey.js";

const limits: CanonicalPublicEntryV0ScanLimits = {
	label: "Scanner test",
	minimumSignatures: 1,
	maximumSignatures: 1,
	maximumDirectParents: 1,
	maximumMetadataBytes: 2,
};

const create = async (
	parents: EntryV0<Uint8Array>[] = [],
	multipleSignatures = false,
	type = EntryType.APPEND,
) => {
	const entry = (await EntryV0.create({
		store: {} as never,
		identity: signKey,
		data: Uint8Array.of(1, 2, 3),
		deferStore: true,
		meta: {
			type,
			gid: parents.length === 0 ? "public-scan-test" : undefined,
			next: parents,
			clock: new LamportClock({
				id: signKey.publicKey.bytes,
				timestamp: new Timestamp({ wallTime: BigInt(parents.length + 1) }),
			}),
			data: Uint8Array.of(4, 5),
		},
		signers: multipleSignatures
			? [signKey.sign.bind(signKey), signKey2.sign.bind(signKey2)]
			: undefined,
	})) as EntryV0<Uint8Array>;
	const prepared = Entry.getPreparedStorageBytes(entry);
	if (!prepared) throw new Error("Fixture has no prepared storage bytes");
	return { entry, bytes: new Uint8Array(prepared) };
};

describe("canonical public EntryV0 scanner", () => {
	it("exports canonical fields and the original signable bytes", async () => {
		const parent = await create();
		const { bytes } = await create([parent.entry]);
		const scanned = scanCanonicalPublicEntryV0(bytes, limits);
		expect(serialize(scanned.entry)).deep.equal(bytes);
		expect(scanned.meta.next).deep.equal([parent.entry.hash]);
		expect(scanned.metaBytes).deep.equal(serialize(scanned.meta));
		expect(scanned.payloadBytes).deep.equal(Uint8Array.of(1, 2, 3));
		expect(scanned.reservedBytes).deep.equal(new Uint8Array(4));
		expect(scanned.hasHash).equal(false);
		expect(scanned.directParentCount).equal(1);
		expect(scanned.signatureCount).equal(1);
		expect(await verify(scanned.signatures[0]!, scanned.signableBytes)).equal(
			true,
		);
	});

	it("enforces configured signature, direct-parent and metadata bounds", async () => {
		const parent = await create();
		const { bytes } = await create([parent.entry]);
		expect(() =>
			scanCanonicalPublicEntryV0(bytes, { ...limits, maximumDirectParents: 0 }),
		).to.throw("at most 0 direct parents");
		expect(() =>
			scanCanonicalPublicEntryV0(bytes, { ...limits, maximumMetadataBytes: 1 }),
		).to.throw("at most 1 bytes");
		const multiple = await create([], true);
		expect(() => scanCanonicalPublicEntryV0(multiple.bytes, limits)).to.throw(
			"exactly one signature",
		);
		expect(
			scanCanonicalPublicEntryV0(multiple.bytes, {
				...limits,
				maximumSignatures: 2,
			}).signatureCount,
		).equal(2);
		expect(() =>
			scanCanonicalPublicEntryV0(bytes, { ...limits, minimumSignatures: 0 }),
		).to.throw("Invalid internal EntryV0 signature bounds");
	});

	it("rejects every truncation, forged lengths, private wrappers and trailing bytes", async () => {
		const { bytes } = await create();
		for (let length = 0; length < bytes.length; length++)
			expect(() =>
				scanCanonicalPublicEntryV0(bytes.subarray(0, length), limits),
			).to.throw();
		const forged = new Uint8Array(bytes);
		new DataView(forged.buffer).setUint32(3, 0xffffffff, true);
		expect(() => scanCanonicalPublicEntryV0(forged, limits)).to.throw(
			"truncated metadata",
		);
		const privateWrapper = new Uint8Array(bytes);
		privateWrapper[1] = 1;
		expect(() => scanCanonicalPublicEntryV0(privateWrapper, limits)).to.throw(
			"metadata must be public",
		);
		const trailing = new Uint8Array(bytes.length + 1);
		trailing.set(bytes);
		expect(() => scanCanonicalPublicEntryV0(trailing, limits)).to.throw(
			"trailing storage",
		);
	});

	it("leaves hash, reserved-byte and entry-type meaning to the caller", async () => {
		const parent = await create();
		const { entry } = await create([parent.entry], false, EntryType.CUT);
		const bytes = serialize(entry);
		const scanned = scanCanonicalPublicEntryV0(bytes, limits);
		expect(scanned.hasHash).equal(true);
		// Locate reserved bytes relative to the captured storage, not a wire constant.
		bytes[scanned.reservedBytes.byteOffset - bytes.byteOffset] = 1;
		expect(scanCanonicalPublicEntryV0(bytes, limits).reservedBytes[0]).equal(1);
		expect(scanned.meta.type).equal(EntryType.CUT);
	});

	it("does not mistake canonical framing for signature authentication", async () => {
		const { bytes } = await create();
		const scanned = scanCanonicalPublicEntryV0(bytes, limits);
		bytes[scanned.payloadBytes.byteOffset - bytes.byteOffset] ^= 1;
		const tampered = scanCanonicalPublicEntryV0(bytes, limits);
		expect(await verify(tampered.signatures[0]!, tampered.signableBytes)).equal(
			false,
		);
	});
});
