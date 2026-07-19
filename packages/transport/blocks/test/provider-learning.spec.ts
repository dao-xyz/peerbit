import { createStore } from "@peerbit/any-store";
import {
	createBlock,
	defaultHasher,
	stringifyCid,
} from "@peerbit/blocks-interface";
import { Ed25519Keypair } from "@peerbit/crypto";
import { expect } from "chai";
import pDefer from "p-defer";
import { AnyBlockStore } from "../src/any-blockstore.js";
import { BlockRequest, BlockResponse, RemoteBlocks } from "../src/remote.js";

const CID = "zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J";
const VALID_BYTES = new Uint8Array([5, 4, 3]);
const INVALID_BYTES = new Uint8Array([3, 4, 5]);
const CUSTOM_VALID_BYTES = new Uint8Array([7, 8, 9]);

type ProviderCache = {
	get(cid: string): string[] | undefined;
};

describe("validated block provider learning", () => {
	let remotes: RemoteBlocks[];

	beforeEach(() => {
		remotes = [];
	});

	afterEach(async () => {
		for (const remote of remotes.reverse()) {
			if (remote.status !== "closed") {
				await remote.stop();
			}
		}
	});

	const createRemote = async (properties?: {
		eagerBlocks?: boolean;
		maxProvidersPerCid?: number;
		requestSeen?: ReturnType<typeof pDefer<void>>;
		requestTargets?: string[][];
		resolveProviders?: (
			cid: string,
			options?: { signal?: AbortSignal },
		) => Promise<string[] | undefined> | string[] | undefined;
	}) => {
		const key = await Ed25519Keypair.create();
		const remote = new RemoteBlocks({
			local: new AnyBlockStore(createStore()),
			publicKey: key.publicKey,
			publish: async (message, options) => {
				if (message instanceof BlockRequest) {
					const to = ((options as any).mode?.to ?? []) as string[];
					properties?.requestTargets?.push([...to]);
					properties?.requestSeen?.resolve();
				}
			},
			waitFor: async (): Promise<string[]> => [],
			eagerBlocks: properties?.eagerBlocks,
			resolveProviders: properties?.resolveProviders,
			providerCache: {
				maxProvidersPerCid: properties?.maxProvidersPerCid,
			},
		});
		remotes.push(remote);
		await remote.start();
		return remote;
	};

	const cachedProviders = (
		remote: RemoteBlocks,
		cid = CID,
	): string[] | undefined =>
		(
			remote as unknown as {
				_providerCache: ProviderCache;
			}
		)._providerCache.get(cid);

	it("does not learn the sender of an invalid active response", async () => {
		const requestSeen = pDefer<void>();
		const remote = await createRemote({ requestSeen });
		const requestedProvider = "requested-provider";
		const invalidResponder = "invalid-responder";

		const read = remote.get(CID, {
			remote: { from: [requestedProvider], timeout: 50 },
		});
		await requestSeen.promise;
		await remote.onMessage(new BlockResponse(CID, INVALID_BYTES), {
			from: invalidResponder,
		});

		expect(await read).to.equal(undefined);
		expect(cachedProviders(remote)).to.deep.equal([requestedProvider]);
	});

	it("learns the sender of a valid active response", async () => {
		const requestSeen = pDefer<void>();
		const remote = await createRemote({ requestSeen });
		const requestedProvider = "requested-provider";
		const validResponder = "valid-responder";

		const read = remote.get(CID, {
			remote: { from: [requestedProvider], timeout: 1_000 },
		});
		await requestSeen.promise;
		await remote.onMessage(new BlockResponse(CID, VALID_BYTES), {
			from: validResponder,
		});

		expect(await read).to.deep.equal(VALID_BYTES);
		expect(cachedProviders(remote)).to.deep.equal([
			validResponder,
			requestedProvider,
		]);
	});

	it("does not learn the sender of an unsolicited invalid response", async () => {
		const remote = await createRemote({ eagerBlocks: true });

		await remote.onMessage(new BlockResponse(CID, INVALID_BYTES), {
			from: "unsolicited-invalid-responder",
		});
		await remote.waitForEagerBlockValidation();

		expect(cachedProviders(remote)).to.equal(undefined);
	});

	it("learns an unsolicited sender only after eager integrity validation", async () => {
		const remote = await createRemote({ eagerBlocks: true });

		await remote.onMessage(new BlockResponse(CID, VALID_BYTES), {
			from: "unsolicited-valid-responder",
		});
		await remote.waitForEagerBlockValidation();

		expect(cachedProviders(remote)).to.deep.equal([
			"unsolicited-valid-responder",
		]);
	});

	it("rejects DAG-CBOR before eager decode but preserves requested reads", async () => {
		const requestSeen = pDefer<void>();
		const remote = await createRemote({ eagerBlocks: true, requestSeen });
		const block = await createBlock(
			{ nested: [[null], [null], [null]] },
			"dag-cbor",
		);
		const cid = stringifyCid(block.cid);
		let eagerValidationCalls = 0;
		(remote as any).validateEagerBlock = async () => {
			eagerValidationCalls += 1;
			throw new Error("DAG-CBOR must not enter eager validation");
		};

		await remote.onMessage(new BlockResponse(cid, block.bytes), {
			from: "unsolicited-dag-cbor-responder",
		});
		await remote.waitForEagerBlockValidation();
		expect(eagerValidationCalls).to.equal(0);
		expect(remote.getEagerBlockCacheTelemetry()).to.include({
			entries: 0,
			bytes: 0,
			pendingEntries: 0,
			pendingBytes: 0,
			rejectedCodec: 1,
		});
		expect(cachedProviders(remote, cid)).to.equal(undefined);

		const read = remote.get(cid, {
			remote: { from: ["requested-provider"], timeout: 1_000 },
		});
		await requestSeen.promise;
		await remote.onMessage(new BlockResponse(cid, block.bytes), {
			from: "requested-dag-cbor-responder",
		});
		expect(await read).to.deep.equal(block.bytes);
		expect(cachedProviders(remote, cid)).to.deep.equal([
			"requested-dag-cbor-responder",
			"requested-provider",
		]);
	});

	it("hands a queued validated response to a resolver installed during hashing", async () => {
		const validationStarted = pDefer<void>();
		const releaseValidation = pDefer<void>();
		const requestSeen = pDefer<void>();
		const remote = await createRemote({ eagerBlocks: true, requestSeen });
		const originalValidator = (remote as any).validateEagerBlock.bind(remote);
		(remote as any).validateEagerBlock = async (...args: any[]) => {
			validationStarted.resolve();
			await releaseValidation.promise;
			return originalValidator(...args);
		};

		await remote.onMessage(new BlockResponse(CID, VALID_BYTES), {
			from: "queued-valid-responder",
		});
		await validationStarted.promise;
		const read = remote.get(CID, {
			remote: { from: ["requested-provider"], timeout: 1_000 },
		});
		await requestSeen.promise;
		releaseValidation.resolve();

		expect(await read).to.deep.equal(VALID_BYTES);
		await remote.waitForEagerBlockValidation();
		expect(remote.getEagerBlockCacheTelemetry()).to.include({
			entries: 0,
			pendingEntries: 0,
			pendingBytes: 0,
		});
		expect(cachedProviders(remote)).to.deep.equal([
			"queued-valid-responder",
			"requested-provider",
		]);
	});

	it("does not bypass an active read's custom hasher or learn its sender", async () => {
		const validationStarted = pDefer<void>();
		const releaseValidation = pDefer<void>();
		const requestSeen = pDefer<void>();
		const remote = await createRemote({ eagerBlocks: true, requestSeen });
		const originalValidator = (remote as any).validateEagerBlock.bind(remote);
		(remote as any).validateEagerBlock = async (...args: any[]) => {
			validationStarted.resolve();
			await releaseValidation.promise;
			return originalValidator(...args);
		};

		await remote.onMessage(new BlockResponse(CID, VALID_BYTES), {
			from: "default-valid-custom-invalid-responder",
		});
		await validationStarted.promise;
		const customHasher = {
			code: defaultHasher.code,
			name: defaultHasher.name,
			digest: async () => defaultHasher.digest(INVALID_BYTES),
		};
		const read = remote.get(CID, {
			remote: {
				from: ["requested-provider"],
				timeout: 50,
				hasher: customHasher,
			} as any,
		});
		await requestSeen.promise;
		releaseValidation.resolve();

		expect(await read).to.equal(undefined);
		await remote.waitForEagerBlockValidation();
		expect(cachedProviders(remote)).to.deep.equal(["requested-provider"]);
		expect(remote.getEagerBlockCacheTelemetry()).to.include({ entries: 0 });
	});

	it("keeps custom-hasher reads on the requested-response path", async () => {
		const requestSeen = pDefer<void>();
		const remote = await createRemote({ eagerBlocks: true, requestSeen });

		// Admission proves the default SHA-256 CID, but that fact is not sufficient
		// for a later caller that explicitly supplies another hasher contract.
		await remote.onMessage(new BlockResponse(CID, VALID_BYTES));
		await remote.waitForEagerBlockValidation();
		expect(remote.getEagerBlockCacheTelemetry()).to.include({
			entries: 1,
			hits: 0,
		});

		const digestInputs: number[][] = [];
		const customHasher = {
			code: defaultHasher.code,
			name: "test-custom-sha256",
			digest: async (bytes: Uint8Array) => {
				digestInputs.push(Array.from(bytes));
				return defaultHasher.digest(
					bytes.byteLength === CUSTOM_VALID_BYTES.byteLength &&
						bytes.every((byte, index) => byte === CUSTOM_VALID_BYTES[index])
						? VALID_BYTES
						: INVALID_BYTES,
				);
			},
		};
		const read = remote.get(CID, {
			remote: {
				from: ["requested-provider"],
				timeout: 1_000,
				hasher: customHasher,
			} as any,
		});
		await requestSeen.promise;

		// The eager entry was neither returned nor asynchronously hashed before
		// resolver installation, so there is no recursive cache/await race.
		expect(digestInputs).to.deep.equal([]);
		expect(remote.getEagerBlockCacheTelemetry()).to.include({
			entries: 1,
			hits: 0,
		});

		await remote.onMessage(new BlockResponse(CID, CUSTOM_VALID_BYTES), {
			from: "custom-valid-responder",
		});
		expect(await read).to.deep.equal(CUSTOM_VALID_BYTES);
		expect(digestInputs).to.deep.equal([Array.from(CUSTOM_VALID_BYTES)]);
		expect(cachedProviders(remote)).to.deep.equal([
			"custom-valid-responder",
			"requested-provider",
		]);
	});

	it("rechecks eager cache after asynchronous provider resolution", async () => {
		const resolverStarted = pDefer<void>();
		const resolverResult = pDefer<string[] | undefined>();
		const requestTargets: string[][] = [];
		const remote = await createRemote({
			eagerBlocks: true,
			requestTargets,
			resolveProviders: async () => {
				resolverStarted.resolve();
				return resolverResult.promise;
			},
		});

		const read = remote.get(CID, { remote: { timeout: 1_000 } });
		await resolverStarted.promise;
		await remote.onMessage(new BlockResponse(CID, VALID_BYTES));
		await remote.waitForEagerBlockValidation();
		resolverResult.resolve(undefined);

		expect(await read).to.deep.equal(VALID_BYTES);
		expect(requestTargets).to.deep.equal([]);
		expect(remote.getEagerBlockCacheTelemetry()).to.include({
			entries: 0,
			hits: 1,
		});
	});

	it("drains queued validation reservations before stop completes", async () => {
		const validationStarted = pDefer<void>();
		const releaseValidation = pDefer<void>();
		const remote = await createRemote({ eagerBlocks: true });
		const originalValidator = (remote as any).validateEagerBlock.bind(remote);
		(remote as any).validateEagerBlock = async (...args: any[]) => {
			validationStarted.resolve();
			await releaseValidation.promise;
			return originalValidator(...args);
		};

		await remote.onMessage(new BlockResponse(CID, VALID_BYTES));
		await validationStarted.promise;
		const stopping = remote.stop();
		releaseValidation.resolve();
		await stopping;
		expect(remote.getEagerBlockCacheTelemetry()).to.include({
			entries: 0,
			pendingEntries: 0,
			pendingBytes: 0,
			admitted: 0,
			rejectedLifecycle: 1,
		});

		await remote.start();
		await remote.onMessage(new BlockResponse(CID, VALID_BYTES));
		await remote.waitForEagerBlockValidation();
		expect(remote.getEagerBlockCacheTelemetry()).to.include({
			entries: 1,
			pendingEntries: 0,
			pendingBytes: 0,
			admitted: 1,
		});
	});

	it("does not let an invalid response evict a bounded provider hint", async () => {
		const requestSeen = pDefer<void>();
		const requestTargets: string[][] = [];
		const remote = await createRemote({
			maxProvidersPerCid: 1,
			requestSeen,
			requestTargets,
		});
		const trustedProvider = "trusted-provider";
		remote.hintProviders(CID, [trustedProvider]);

		const read = remote.get(CID, { remote: { timeout: 50 } });
		await requestSeen.promise;
		await remote.onMessage(new BlockResponse(CID, INVALID_BYTES), {
			from: "invalid-responder",
		});

		expect(await read).to.equal(undefined);
		expect(cachedProviders(remote)).to.deep.equal([trustedProvider]);

		// A subsequent read without explicit providers must still target the trusted
		// cached hint rather than the invalid response sender.
		expect(await remote.get(CID, { remote: { timeout: 50 } })).to.equal(
			undefined,
		);
		expect(requestTargets).to.deep.equal([
			[trustedProvider],
			[trustedProvider],
		]);
	});
});
