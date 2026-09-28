import { serialize } from "@dao-xyz/borsh";
import { type Ed25519Keypair, PreHash, sha256 } from "@peerbit/crypto";
import { Entry, EntryV0, LamportClock, Timestamp } from "@peerbit/log";
import { Peerbit } from "peerbit";
import { Documents, type Operation, PutOperation } from "../../src/index.js";
import { Document, TestStore } from "../data.js";
import {
	type RecoveryInput,
	type RecoveryManifest,
	type RecoveryTrust,
	encodeManifest,
	hex,
	readRecoveryView,
} from "./checkpoint-recovery.js";

export const signRecoveryManifest = async (
	manifest: RecoveryManifest,
	blocks: RecoveryInput["blocks"],
	owner: Ed25519Keypair,
): Promise<{ input: RecoveryInput; trust: RecoveryTrust }> => {
	const bytes = encodeManifest(manifest);
	return {
		input: {
			manifest: bytes,
			signature: new Uint8Array(
				(await owner.sign(bytes, PreHash.NONE)).signature,
			),
			blocks,
		},
		trust: {
			owner: new Uint8Array(owner.publicKey.publicKey),
			manifestDigest: hex(await sha256(bytes)),
			logId: manifest.logId,
		},
	};
};

// Generate evidence through real Documents operations, keeping raw entries before
// CUT removes them. The owner retains P only for the full-history reference run;
// it is intentionally absent from the recovery inventory.
export const makeRecoveryData = async () => {
	const source = await Peerbit.create();
	try {
		const owner = source.identity;
		const store = await source.open(
			new TestStore({ docs: new Documents<Document>() }),
			{
				args: {
					mode: "compat",
					replicate: false,
					keep: "self",
					nativeGraph: false,
					nativeBackbone: false,
					nativeRangePlanner: false,
					index: { cache: { resolver: 0 } },
				},
			},
		);
		const log = store.docs.log.log;
		const options = (wallTime: bigint) => ({
			target: "none" as const,
			meta: { timestamp: new Timestamp({ wallTime }) },
		});
		const capture = async (entry: Entry<Operation>) => {
			const bytes = await log.blocks.get(entry.hash, { remote: false });
			if (!bytes) throw new Error("Missing source evidence block");
			return { cid: entry.hash, bytes: new Uint8Array(bytes) };
		};
		const p = await store.docs.put(
			new Document({ id: "k", name: "P" }),
			options(1_000n),
		);
		const prefix = await capture(p.entry);
		const a = await store.docs.put(
			new Document({ id: "k", name: "A" }),
			options(2_000n),
		);
		const aBlock = await capture(a.entry);
		const rows = (await readRecoveryView(store.docs)).rows;
		const b = await store.docs.put(
			new Document({ id: "k", name: "B" }),
			options(3_000n),
		);
		const bBlock = await capture(b.entry);
		const branch = async (id: string, parent: Entry<Operation>) =>
			EntryV0.create({
				store: log.blocks,
				identity: owner,
				encoding: log.encoding,
				data: new PutOperation({
					data: serialize(new Document({ id, name: "C" })),
				}),
				meta: {
					next: [parent],
					data: b.entry.meta.data,
					clock: new LamportClock({
						id: owner.publicKey.bytes,
						timestamp: new Timestamp({ wallTime: 5_000n }),
					}),
				},
			});
		const c = await branch("k", a.entry);
		const cBlock = await capture(c);
		const alternatives = {
			crossKey: await capture(await branch("other", a.entry)),
			nonclosed: await capture(await branch("k", p.entry)),
		};
		const d = await store.docs.del("k", options(4_000n));
		const dBlock = await capture(d.entry);
		if ((await readRecoveryView(store.docs)).rows.length !== 0)
			throw new Error("Source CUT did not remove the document");
		// The reference has the entire source prefix available. Ordinary joining
		// reintroduces A and P before C, preserving the original creation context.
		await log.join([{ entry: a.entry, references: [p.entry] }], {
			verifySignatures: true,
		});
		await log.join([{ entry: c, references: [a.entry, p.entry] }], {
			verifySignatures: true,
		});
		const sourceView = await readRecoveryView(store.docs);
		if (
			sourceView.rows.length !== 1 ||
			sourceView.rows[0].name !== "C" ||
			sourceView.rows[0].context.created !== "1000" ||
			!sourceView.entries.includes(p.entry.hash)
		)
			throw new Error("Unexpected full-history source reference state");
		const expected = {
			...sourceView,
			entries: sourceView.entries.filter((cid) => cid !== p.entry.hash),
		};
		const labels = {
			P: p.entry.hash,
			A: a.entry.hash,
			B: b.entry.hash,
			D: d.entry.hash,
			C: c.hash,
		};
		const manifest: RecoveryManifest = {
			profile: "peerbit-checkpoint-recovery-fixture-v1",
			logId: hex(log.id),
			records: [
				{ cid: labels.A, kind: "put", id: "k", name: "A" },
				{ cid: labels.B, kind: "put", id: "k", name: "B" },
				{ cid: labels.D, kind: "delete", id: "k" },
				{ cid: labels.C, kind: "put", id: "k", name: "C" },
			],
			boundary: [labels.A],
			rows,
			order: [labels.B, labels.D, labels.C],
			expected,
		};
		return {
			...(await signRecoveryManifest(
				manifest,
				[aBlock, bBlock, dBlock, cBlock],
				owner,
			)),
			manifest,
			expected,
			owner,
			labels,
			prefix,
			alternatives,
		};
	} finally {
		await source.stop();
	}
};
