import { type Blocks, calculateRawCid } from "@peerbit/blocks-interface";
import { Ed25519Keypair, randomBytes } from "@peerbit/crypto";
import { expect } from "chai";
import pDefer from "p-defer";
import {
	CHECKPOINT_MAX_CERTIFICATE_BYTES,
	createCheckpointCertificate,
	readCheckpointCertificate,
	signCheckpointApproval,
} from "../src/checkpoint-certificate.js";

const memoryBlocks = () => {
	const values = new Map<string, Uint8Array>();
	const blocks = {
		get: async (cid: string) => {
			const bytes = values.get(cid);
			return bytes ? new Uint8Array(bytes) : undefined;
		},
		put: async (bytes: Uint8Array) => {
			const { cid } = await calculateRawCid(bytes);
			values.set(cid, new Uint8Array(bytes));
			return cid;
		},
		putKnown: async (cid: string, bytes: Uint8Array) => {
			values.set(cid, new Uint8Array(bytes));
			return cid;
		},
	} as unknown as Blocks;
	return { blocks, values };
};

const fixture = async () => {
	const { blocks, values } = memoryBlocks();
	const identities = await Promise.all([
		Ed25519Keypair.create(),
		Ed25519Keypair.create(),
		Ed25519Keypair.create(),
	]);
	const resource = randomBytes(32);
	const proposal = (await calculateRawCid(new Uint8Array([1, 2, 3]))).cid;
	const writers = identities.map((identity) => identity.publicKey.publicKey);
	const approvals = await Promise.all(
		identities.map((identity) =>
			signCheckpointApproval({ resource, proposal, identity }),
		),
	);
	return { blocks, values, identities, resource, proposal, writers, approvals };
};

describe("Documents checkpoint unanimous writer certificate", () => {
	it("rejects CID-valid excessive JSON depth before parsing an unsigned certificate", async () => {
		const input = await fixture();
		const text = `${"[".repeat(64)}0${"]".repeat(64)}`;
		const cid = await input.blocks.put(new TextEncoder().encode(text));
		const parse = JSON.parse;
		let parsedDeepInput = 0;
		JSON.parse = (value, reviver) => {
			if (value === text) parsedDeepInput++;
			return parse(value, reviver);
		};
		try {
			await expect(readCheckpointCertificate({ ...input, cid })).rejectedWith(
				"too deep",
			);
			expect(parsedDeepInput).equal(0);
		} finally {
			JSON.parse = parse;
		}
	});

	it("canonicalizes every writer approval and retains detached bytes", async () => {
		const input = await fixture();
		const certificate = await createCheckpointCertificate(input);
		const permuted = await createCheckpointCertificate({
			...input,
			writers: [...input.writers].reverse(),
			approvals: [...input.approvals].reverse(),
		});
		expect(permuted.cid).equal(certificate.cid);
		const read = await readCheckpointCertificate({
			...input,
			cid: certificate.cid,
		});
		expect(read.proposal).equal(input.proposal);
		expect(read.genesis).equal(false);
		expect(read.approvals).have.length(3);
		const original = input.values.get(read.cid)!;
		read.approvals[0]!.signature.fill(0);
		read.approvals[0]!.writer.fill(0);
		input.values.delete(read.cid);
		await read.retain();
		expect(input.values.get(read.cid)).deep.equal(original);
		await readCheckpointCertificate({ ...input, cid: certificate.cid });
	});

	it("rejects missing, duplicate, wrong-writer and invalid approvals", async () => {
		const input = await fixture();
		await expect(
			createCheckpointCertificate({
				...input,
				approvals: input.approvals.slice(1),
			}),
		).rejectedWith("every writer");
		await expect(
			createCheckpointCertificate({
				...input,
				approvals: [
					input.approvals[0]!,
					input.approvals[0]!,
					input.approvals[2]!,
				],
			}),
		).rejectedWith("fixed writer roster");
		const outsider = await Ed25519Keypair.create();
		await expect(
			createCheckpointCertificate({
				...input,
				approvals: [
					...input.approvals.slice(1),
					await signCheckpointApproval({ ...input, identity: outsider }),
				],
			}),
		).rejectedWith("fixed writer roster");
		await expect(
			createCheckpointCertificate({
				...input,
				approvals: input.approvals.map((approval, index) =>
					index === 0
						? { ...approval, signature: new Uint8Array(64) }
						: approval,
				),
			}),
		).rejectedWith("signature");
	});

	it("binds approvals to the exact resource and proposal", async () => {
		const input = await fixture();
		const otherProposal = (await calculateRawCid(new Uint8Array([4, 5, 6])))
			.cid;
		for (const changed of [
			{ resource: randomBytes(32) },
			{ proposal: otherProposal },
		]) {
			await expect(
				createCheckpointCertificate({ ...input, ...changed }),
			).rejectedWith("signature");
		}
		const certificate = await createCheckpointCertificate(input);
		await expect(
			readCheckpointCertificate({
				...input,
				cid: certificate.cid,
				resource: randomBytes(32),
			}),
		).rejectedWith("Invalid checkpoint certificate");
		await expect(
			readCheckpointCertificate({
				...input,
				cid: certificate.cid,
				writers: input.writers.slice(1),
			}),
		).rejectedWith("every writer");
	});

	it("requires an explicit genesis exemption with no approvals", async () => {
		const input = await fixture();
		await expect(
			createCheckpointCertificate({ ...input, approvals: [] }),
		).rejectedWith("every writer");
		await expect(
			createCheckpointCertificate({ ...input, genesis: true }),
		).rejectedWith("must not carry writer approvals");
		const certificate = await createCheckpointCertificate({
			...input,
			genesis: true,
			approvals: [],
		});
		const read = await readCheckpointCertificate({
			...input,
			cid: certificate.cid,
		});
		expect(read.genesis).equal(true);
		expect(read.approvals).have.length(0);
	});

	it("rejects noncanonical, tampered and oversized certificate blocks", async () => {
		const input = await fixture();
		const certificate = await createCheckpointCertificate(input);
		const bytes = input.values.get(certificate.cid)!;
		const noncanonical = new TextEncoder().encode(
			`${new TextDecoder().decode(bytes)} `,
		);
		const cid = await input.blocks.put(noncanonical);
		await expect(readCheckpointCertificate({ ...input, cid })).rejectedWith(
			"Noncanonical",
		);
		input.values.set(certificate.cid, new Uint8Array([1, 2, 3]));
		await expect(readCheckpointCertificate({ ...input, cid: certificate.cid }))
			.rejected;
		input.values.set(
			certificate.cid,
			new Uint8Array(CHECKPOINT_MAX_CERTIFICATE_BYTES + 1),
		);
		await expect(
			readCheckpointCertificate({ ...input, cid: certificate.cid }),
		).rejectedWith("capacity");
	});

	it("captures expected resource and roster before an asynchronous fetch", async () => {
		const input = await fixture();
		const certificate = await createCheckpointCertificate(input);
		const entered = pDefer<void>();
		const release = pDefer<void>();
		const get = input.blocks.get.bind(input.blocks);
		input.blocks.get = async (...args) => {
			entered.resolve();
			await release.promise;
			return get(...args);
		};
		const resource = new Uint8Array(input.resource);
		const writers = input.writers.map((writer) => new Uint8Array(writer));
		const reading = readCheckpointCertificate({
			...input,
			cid: certificate.cid,
			resource,
			writers,
		});
		await entered.promise;
		resource.fill(0);
		writers.forEach((writer) => writer.fill(0));
		release.resolve();
		expect((await reading).cid).equal(certificate.cid);
	});

	it("bounds the writer roster and rejects duplicate identities", async () => {
		const input = await fixture();
		for (const writers of [
			[],
			new Array(33).fill(input.writers[0]),
			[input.writers[0]!, input.writers[0]!],
		]) {
			await expect(createCheckpointCertificate({ ...input, writers })).rejected;
		}
	});
});
