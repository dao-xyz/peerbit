import { deserialize } from "@dao-xyz/borsh";
import { Ed25519Keypair } from "@peerbit/crypto";
import { Entry } from "@peerbit/log";
import { expect } from "chai";
import {
	makeRecoveryData,
	signRecoveryManifest,
} from "./utils/checkpoint-recovery-data.js";
import {
	CheckpointRecoveryFixture,
	MAX_RECOVERY_RECORD_BYTES,
	type RecoveryInput,
	type RecoveryManifest,
	type RecoveryTrust,
	decodeRecoveryInput,
	encodeRecoveryInput,
} from "./utils/checkpoint-recovery.js";

const clone = <T>(value: T): T => structuredClone(value);

// All cases use an explicit finite owner-certified recovery profile. These are
// not claims about arbitrary Documents options or live network entry arrivals.
describe("Documents finite checkpoint recovery fixture", function () {
	this.timeout(60_000);
	let data: Awaited<ReturnType<typeof makeRecoveryData>>;
	before(async () => {
		data = await makeRecoveryData();
	});

	const reject = async (
		input: RecoveryInput,
		trust: RecoveryTrust,
		options: { beforeReplay?: boolean; message?: RegExp } = {},
	) => {
		const fixture = new CheckpointRecoveryFixture();
		const phases: string[] = [];
		let failure: unknown;
		try {
			await fixture.recover(input, trust, undefined, {
				phase: (name) => {
					phases.push(name);
					expect(() => fixture.snapshot()).to.throw();
				},
			});
		} catch (error) {
			failure = error;
		}
		expect(failure, "recovery must reject").instanceOf(Error);
		if (options.message)
			expect((failure as Error).message).to.match(options.message);
		if (options.beforeReplay) expect(phases).deep.equal([]);
		expect(phases).not.to.include("published");
		expect(() => fixture.snapshot()).to.throw();
		return fixture;
	};

	const replaceC = (
		manifest: RecoveryManifest,
		input: RecoveryInput,
		replacement: RecoveryInput["blocks"][number],
		id = "k",
	) => {
		const record = manifest.records.find(
			(record) => record.cid === data.labels.C,
		)!;
		record.cid = replacement.cid;
		record.id = id;
		manifest.order = manifest.order.map((cid) =>
			cid === data.labels.C ? replacement.cid : cid,
		);
		for (const list of [manifest.expected.entries, manifest.expected.heads]) {
			list[list.indexOf(data.labels.C)] = replacement.cid;
			list.sort();
		}
		manifest.expected.rows[0].id = id;
		manifest.expected.rows[0].context.head = replacement.cid;
		input.blocks = input.blocks.map((block) =>
			block.cid === data.labels.C ? clone(replacement) : block,
		);
	};

	it("recovers the source row/context and frontier for shuffled block inventories", async () => {
		expect(data.input.blocks.some(({ cid }) => cid === data.labels.P)).equal(
			false,
		);
		expect(
			data.expected.rows.map((row) => [row.name, row.context.created]),
		).deep.equal([["C", "1000"]]);
		expect(data.expected.entries).deep.equal(
			[data.labels.A, data.labels.C, data.labels.D].sort(),
		);
		expect(data.expected.heads).deep.equal(
			[data.labels.C, data.labels.D].sort(),
		);
		for (const permutation of [
			[0, 1, 2, 3],
			[3, 2, 1, 0],
			[1, 3, 0, 2],
		]) {
			const input = clone(data.input);
			input.blocks = permutation.map((index) => input.blocks[index]);
			const fixture = new CheckpointRecoveryFixture();
			const phases: string[] = [];
			expect(() => fixture.snapshot()).to.throw();
			const result = await fixture.recover(input, data.trust, undefined, {
				phase: (phase) => {
					phases.push(phase);
					expect(() => fixture.snapshot()).to.throw();
				},
			});
			expect(phases).deep.equal([
				"verified",
				"retained",
				"boundary",
				"entry:0",
				"entry:1",
				"entry:2",
				"replayed",
				"published",
			]);
			expect(result).deep.equal(data.expected);
			expect(fixture.omittedPrefixReads).equal(0);
			result.rows[0].context.created = "999";
			result.entries.length = 0;
			expect(fixture.snapshot()).deep.equal(data.expected);
		}
	});

	it("owns its authenticated input across asynchronous replay", async () => {
		const input = clone(data.input);
		const trust = clone(data.trust);
		const fixture = new CheckpointRecoveryFixture();
		const result = await fixture.recover(input, trust, undefined, {
			phase: (name) => {
				if (name !== "verified") return;
				input.manifest.fill(0);
				input.signature.fill(0);
				for (const block of input.blocks) block.bytes.fill(0);
				input.blocks.length = 0;
				trust.owner.fill(0);
				trust.manifestDigest = "00".repeat(32);
			},
		});
		expect(result).deep.equal(data.expected);
		expect(fixture.omittedPrefixReads).equal(0);
	});

	it("round-trips owned retained-input bytes", () => {
		const bytes = encodeRecoveryInput(data.input);
		const decoded = decodeRecoveryInput(bytes);
		expect(decoded).deep.equal(data.input);
		decoded.blocks[0].bytes.fill(0);
		expect(decodeRecoveryInput(bytes)).deep.equal(data.input);
	});

	it("enforces intrinsic byte capacities despite shadowed byteLength properties", async () => {
		const oversized = (length: number) => {
			const bytes = new Uint8Array(length);
			Object.defineProperty(bytes, "byteLength", { value: 0 });
			return bytes;
		};
		expect(() =>
			decodeRecoveryInput(oversized(MAX_RECOVERY_RECORD_BYTES + 1)),
		).to.throw(/byte capacity/i);
		const manifest = clone(data.input);
		manifest.manifest = oversized(64 * 1024 + 1);
		await reject(manifest, data.trust, {
			beforeReplay: true,
			message: /byte capacity/i,
		});
		const block = clone(data.input);
		block.blocks[0].bytes = oversized(64 * 1024 + 1);
		await reject(block, data.trust, {
			beforeReplay: true,
			message: /byte capacity/i,
		});
	});

	it("enforces supplied block count and aggregate byte capacities before replay", async () => {
		const count = clone(data.input);
		count.blocks = Array.from({ length: 33 }, (_, index) => ({
			cid: `block-${index}`,
			bytes: new Uint8Array(),
		}));
		await reject(count, data.trust, {
			beforeReplay: true,
			message: /inventory capacity/i,
		});
		const total = clone(data.input);
		total.blocks = Array.from({ length: 17 }, (_, index) => ({
			cid: `block-${index}`,
			bytes: new Uint8Array(64 * 1024),
		}));
		await reject(total, data.trust, {
			beforeReplay: true,
			message: /byte capacity/i,
		});
	});

	it("captures valid inputs without invoking caller iterators or byte species hooks", async () => {
		const forbidden = () => {
			throw new Error("Caller-owned hook invoked");
		};
		class HostileBytes extends Uint8Array {}
		Object.defineProperty(HostileBytes, Symbol.species, { get: forbidden });
		const hostile = (bytes: Uint8Array) => {
			const value = new HostileBytes(bytes);
			Object.defineProperty(value, Symbol.iterator, { value: forbidden });
			return value;
		};
		const input = clone(data.input);
		input.manifest = hostile(input.manifest);
		input.signature = hostile(input.signature);
		input.blocks = input.blocks.map(({ cid, bytes }) => ({
			cid,
			bytes: hostile(bytes),
		}));
		Object.defineProperty(input.blocks, Symbol.iterator, { value: forbidden });
		const trust = { ...data.trust, owner: hostile(data.trust.owner) };
		const fixture = new CheckpointRecoveryFixture();
		expect(await fixture.recover(input, trust)).deep.equal(data.expected);
		expect(fixture.omittedPrefixReads).equal(0);
	});

	it("rejects altered blocks and owner certificates before replay", async () => {
		const badBlock = clone(data.input);
		badBlock.blocks[1].bytes[10] ^= 1;
		await reject(badBlock, data.trust, { beforeReplay: true });
		const badCertificate = clone(data.input);
		badCertificate.signature[0] ^= 1;
		await reject(badCertificate, data.trust, {
			beforeReplay: true,
			message: /owner certificate/i,
		});
	});

	it("rejects wrong owner, resource, and externally pinned digest", async () => {
		const stranger = await Ed25519Keypair.create();
		await reject(
			clone(data.input),
			{
				...data.trust,
				owner: stranger.publicKey.publicKey,
			},
			{ beforeReplay: true, message: /owner certificate/i },
		);
		await reject(
			clone(data.input),
			{
				...data.trust,
				logId: "ff".repeat(32),
			},
			{ beforeReplay: true, message: /resource/i },
		);
		await reject(
			clone(data.input),
			{
				...data.trust,
				manifestDigest: "00".repeat(32),
			},
			{ beforeReplay: true, message: /digest/i },
		);
	});

	it("rejects missing, duplicate, or foreign supplied blocks before replay", async () => {
		const missing = clone(data.input);
		missing.blocks.pop();
		await reject(missing, data.trust, { beforeReplay: true });
		const duplicate = clone(data.input);
		duplicate.blocks.push(clone(duplicate.blocks[0]));
		await reject(duplicate, data.trust, { beforeReplay: true });
		const foreign = clone(data.input);
		foreign.blocks.push(clone(data.prefix));
		await reject(foreign, data.trust, { beforeReplay: true });
	});

	it("rejects malformed entry framing before replay", async () => {
		const input = clone(data.input);
		input.blocks[0].bytes = input.blocks[0].bytes.subarray(0, 8);
		await reject(input, data.trust, { beforeReplay: true });
	});

	it("rejects an invalid entry signature even with a matching CID and valid owner manifest", async () => {
		const input = clone(data.input);
		const manifest = clone(data.manifest);
		const block = input.blocks.find((block) => block.cid === data.labels.C)!;
		const entry = deserialize(block.bytes, Entry);
		entry.signatures[0].signature[0] ^= 1;
		const bytes = new Uint8Array(entry.getStorageBytes());
		const cid = await Entry.prepareMultihash(entry);
		expect(cid).not.equal(block.cid);
		replaceC(manifest, input, { cid, bytes });
		const signed = await signRecoveryManifest(
			manifest,
			input.blocks,
			data.owner,
		);
		await reject(signed.input, signed.trust, {
			beforeReplay: true,
			message: /entry signer\/signature/i,
		});
	});

	for (const variant of ["nonclosed", "crossKey"] as const) {
		it(`rejects an authentic ${variant} suffix dependency before replay`, async () => {
			const input = clone(data.input);
			const manifest = clone(data.manifest);
			replaceC(
				manifest,
				input,
				data.alternatives[variant],
				variant === "crossKey" ? "other" : "k",
			);
			const signed = await signRecoveryManifest(
				manifest,
				input.blocks,
				data.owner,
			);
			await reject(signed.input, signed.trust, {
				beforeReplay: true,
				message: variant === "crossKey" ? /cross-key/i : /causally closed/i,
			});
		});
	}

	it("does not publish an unsupported reordered replay or incorrect final frontier", async () => {
		const reordered = clone(data.manifest);
		reordered.order = [data.labels.B, data.labels.C, data.labels.D];
		const first = await signRecoveryManifest(
			reordered,
			clone(data.input.blocks),
			data.owner,
		);
		await reject(first.input, first.trust);
		const wrongFrontier = clone(data.manifest);
		wrongFrontier.expected.heads = [data.labels.D];
		const second = await signRecoveryManifest(
			wrongFrontier,
			clone(data.input.blocks),
			data.owner,
		);
		await reject(second.input, second.trust, { message: /frontier mismatch/i });
	});

	it("keeps every interrupted replay phase unpublished", async () => {
		for (const stopAt of ["boundary", "entry:1", "replayed", "published"]) {
			const fixture = new CheckpointRecoveryFixture();
			const interruption = new Error(`Stop at ${stopAt}`);
			let failure: unknown;
			try {
				await fixture.recover(clone(data.input), data.trust, undefined, {
					phase: (name) => {
						expect(() => fixture.snapshot()).to.throw();
						if (name === stopAt) throw interruption;
					},
				});
			} catch (error) {
				failure = error;
			}
			expect(failure).equal(interruption);
			expect(() => fixture.snapshot()).to.throw();
		}
	});
});
