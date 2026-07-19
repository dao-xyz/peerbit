import { createStore } from "@peerbit/any-store";
import { Ed25519Keypair } from "@peerbit/crypto";
import { expect } from "chai";
import pDefer from "p-defer";
import { AnyBlockStore } from "../src/any-blockstore.js";
import { BlockRequest, BlockResponse, RemoteBlocks } from "../src/remote.js";

const CID = "zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J";
const VALID_BYTES = new Uint8Array([5, 4, 3]);
const INVALID_BYTES = new Uint8Array([3, 4, 5]);

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
			providerCache: {
				maxProvidersPerCid: properties?.maxProvidersPerCid,
			},
		});
		remotes.push(remote);
		await remote.start();
		return remote;
	};

	const cachedProviders = (remote: RemoteBlocks): string[] | undefined =>
		(
			remote as unknown as {
				_providerCache: ProviderCache;
			}
		)._providerCache.get(CID);

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

		expect(cachedProviders(remote)).to.equal(undefined);
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
