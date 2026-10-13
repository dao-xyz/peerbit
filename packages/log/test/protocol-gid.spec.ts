import { deserialize } from "@dao-xyz/borsh";
import { AnyBlockStore } from "@peerbit/blocks";
import { expect } from "chai";
import { LamportClock, Timestamp } from "../src/clock.js";
import { EntryV0 } from "../src/entry-v0.js";
import { Entry } from "../src/entry.js";
import { signKey } from "./fixtures/privateKey.js";

const createWithGid = <T>(
	properties: Parameters<typeof EntryV0.create<T>>[0],
	gid: string,
): Promise<Entry<T>> =>
	(
		EntryV0 as unknown as {
			createWithExplicitGid: typeof createWithGid;
		}
	).createWithExplicitGid(properties, gid);

describe("private protocol-defined entry GID", () => {
	let store: AnyBlockStore;
	beforeEach(async () => {
		store = new AnyBlockStore();
		await store.start();
	});
	afterEach(async () => {
		await store.stop();
	});
	const clock = (wallTime: bigint) =>
		new LamportClock({
			id: signKey.publicKey.bytes,
			timestamp: new Timestamp({ wallTime }),
		});
	const parent = (gid: string) =>
		EntryV0.create({
			store,
			identity: signKey,
			data: Uint8Array.of(1),
			meta: { gid, clock: clock(1n) },
		});

	for (const withAdmissionCallback of [false, true]) {
		it(`preserves canonical bytes when the GID already agrees (callback=${withAdmissionCallback})`, async () => {
			const next = [await parent("owner-key")];
			const properties = {
				store,
				identity: signKey,
				data: Uint8Array.of(2),
				meta: { next, clock: clock(2n) },
				canAppend: withAdmissionCallback ? () => true : undefined,
			};
			const ordinary = await EntryV0.create(properties);
			const protocol = await createWithGid(properties, "owner-key");
			expect(protocol.hash).equal(ordinary.hash);
			expect(await store.get(protocol.hash)).deep.equal(
				await store.get(ordinary.hash),
			);
		});
	}

	it("signs real cross-key parent hashes without rewriting their groups", async () => {
		const parents = [await parent("a-key"), await parent("b-key")];
		const child = await createWithGid(
			{
				store,
				identity: signKey,
				data: Uint8Array.of(3),
				meta: { next: parents, clock: clock(2n) },
			},
			"c-key",
		);
		expect(child.meta.gid).equal("c-key");
		expect(child.meta.next).deep.equal(parents.map((entry) => entry.hash));
		expect(parents.map((entry) => entry.meta.gid)).deep.equal([
			"a-key",
			"b-key",
		]);
		expect(await child.verifySignatures()).equal(true);
		const reopened = deserialize((await store.get(child.hash))!, Entry);
		expect(reopened.meta.gid).equal("c-key");
		expect(reopened.meta.next).deep.equal(child.meta.next);
		expect(await reopened.verifySignatures()).equal(true);
	});

	it("keeps ordinary inheritance and the explicit-GID rejection unchanged", async () => {
		const next = [await parent("b-key"), await parent("a-key")];
		const properties = {
			store,
			identity: signKey,
			data: Uint8Array.of(3),
			meta: { next, clock: clock(2n) },
		};
		expect((await EntryV0.create(properties)).meta.gid).equal("a-key");
		await expect(
			EntryV0.create({
				...properties,
				meta: { ...properties.meta, gid: "c-key" },
			}),
		).rejectedWith("Expecting '.meta.gid' property to be undefined");
	});

	it("does not bypass parent clock validation", async () => {
		await expect(
			createWithGid(
				{
					store,
					identity: signKey,
					data: Uint8Array.of(2),
					meta: { next: [await parent("other-key")], clock: clock(1n) },
				},
				"owner-key",
			),
		).rejectedWith("Expecting next(s) to happen before entry");
	});

	it("validates the sole GID before admission or persistence", async () => {
		const properties = { store, identity: signKey, data: Uint8Array.of(1) };
		await expect(createWithGid(properties, "")).rejectedWith("protocol GID");
		await expect(
			createWithGid({ ...properties, meta: { gid: "duplicate" } }, "owner-key"),
		).rejectedWith("protocol GID");
	});
});
