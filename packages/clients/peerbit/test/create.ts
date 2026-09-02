/* eslint-disable @typescript-eslint/no-unused-expressions */
import { keys } from "@libp2p/crypto";
import { createStore } from "@peerbit/any-store";
import { RustAnyStore } from "@peerbit/any-store-rust";
import {
	BLOCK_SERVICE_BLOCK_STORE_SAFETY,
	type BlockStoreSafety,
	UNKNOWN_BLOCK_STORE_SAFETY,
} from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { RustIndices } from "@peerbit/indexer-rust";
import { expect } from "chai";
import path from "path";
import { v4 as uuid } from "uuid";
import { Peerbit } from "../src/peer.js";
import { createRustPeerbitOptions } from "../src/rust.js";

describe("Create", function () {
	describe("with db", function () {
		let client: Peerbit;
		let clientDirectory: string;
		before(async () => {
			const dbPath = path.join("tmp", "peerbit", "tests", "create-open");
			clientDirectory = dbPath + uuid();
			client = (await Peerbit.create({
				directory: clientDirectory,
			})) as Peerbit;
		});
		after(async () => {
			await client.stop();
		});

		it("directory exist", async () => {
			expect(client.directory).equal(clientDirectory);
		});

		it("creates directory", async () => {
			const fs = await import("fs");
			expect(fs.existsSync(clientDirectory)).equal(true);
		});

		it("block storage exist at path", async () => {
			expect(await client.libp2p.services.blocks.persisted()).to.be.true;
			expect(client.services.blocks.localStoreSafety).to.equal(
				BLOCK_SERVICE_BLOCK_STORE_SAFETY,
			);
		});
	});

	it("reports the built-in memory store's block-service reference domain", async () => {
		const client = await Peerbit.create();
		try {
			expect(await client.services.blocks.persisted()).to.equal(false);
			expect(client.services.blocks.localStoreSafety).to.equal(
				BLOCK_SERVICE_BLOCK_STORE_SAFETY,
			);
			expect(Object.isFrozen(client.services.blocks.localStoreSafety)).to.equal(
				true,
			);
		} finally {
			await client.stop();
		}
	});

	it("can create with a local store factory", async () => {
		const clientDirectory = path.join(
			"tmp",
			"peerbit",
			"tests",
			"create-store-factory-" + uuid(),
		);
		const directories: string[] = [];
		const client = await Peerbit.create({
			directory: clientDirectory,
			storage: {
				storeFactory: (directory) => {
					directories.push(directory ?? "");
					return createStore(directory);
				},
			},
		});

		expect(directories).to.include(path.join(clientDirectory, "/cache"));
		expect(directories).to.include(path.join(clientDirectory, "/keychain"));
		expect(directories).to.include(path.join(clientDirectory, "/blocks"));
		expect(client.services.blocks.localStoreSafety).to.equal(
			UNKNOWN_BLOCK_STORE_SAFETY,
		);
		await client.stop();
	});

	it("defaults a custom blocks factory to unknown", async () => {
		const client = await Peerbit.create({
			storage: { blocksStoreFactory: () => createStore() },
		});
		try {
			expect(client.services.blocks.localStoreSafety).to.equal(
				UNKNOWN_BLOCK_STORE_SAFETY,
			);
		} finally {
			await client.stop();
		}
	});

	it("copies and freezes an explicit custom blocks-store declaration", async () => {
		const declaration: BlockStoreSafety = {
			referenceDomain: "caller-exclusive",
			enforcedReclamation: "none",
		};
		const client = await Peerbit.create({
			storage: {
				blocksStoreFactory: () => createStore(),
				blocksStoreSafety: declaration,
			},
		});
		try {
			const observed = client.services.blocks.localStoreSafety;
			expect(observed).to.deep.equal(declaration);
			expect(observed).not.to.equal(declaration);
			expect(Object.isFrozen(observed)).to.equal(true);
			(declaration as { referenceDomain: string }).referenceDomain = "shared";
			expect(observed?.referenceDomain).to.equal("caller-exclusive");
		} finally {
			await client.stop();
		}
	});

	it("rejects unsafe declarations before acquiring resources", async () => {
		let indexerCalls = 0;
		await expect(
			Peerbit.create({
				storage: {
					blocksStoreSafety: {
						referenceDomain: "caller-exclusive",
						enforcedReclamation: "none",
					},
				},
				indexer: async () => {
					indexerCalls += 1;
					throw new Error("indexer must not be acquired");
				},
			}),
		).to.be.rejectedWith(
			Error,
			"'storage.blocksStoreSafety' requires a custom blocks store factory",
		);
		expect(indexerCalls).to.equal(0);

		for (const blocksStoreSafety of [
			null,
			{},
			{ referenceDomain: "invalid", enforcedReclamation: "none" },
			{ referenceDomain: "unknown", enforcedReclamation: "unsupported" },
		]) {
			let blocksStoreFactoryCalls = 0;
			let genericStoreFactoryCalls = 0;
			indexerCalls = 0;
			await expect(
				Peerbit.create({
					storage: {
						blocksStoreFactory: () => {
							blocksStoreFactoryCalls += 1;
							return createStore();
						},
						blocksStoreSafety: blocksStoreSafety as any,
						storeFactory: (directory) => {
							genericStoreFactoryCalls += 1;
							return createStore(directory);
						},
					},
					indexer: async () => {
						indexerCalls += 1;
						throw new Error("indexer must not be acquired");
					},
				}),
			).to.be.rejectedWith(Error);
			expect(blocksStoreFactoryCalls).to.equal(0);
			expect(genericStoreFactoryCalls).to.equal(0);
			expect(indexerCalls).to.equal(0);
		}
	});

	it("rejects a nullish custom blocks-store result", async () => {
		const nullishResults: readonly (null | undefined)[] = [null, undefined];
		for (const result of nullishResults) {
			await expect(
				Peerbit.create({
					storage: {
						blocksStoreFactory: (() => result) as any,
					},
				}),
			).to.be.rejectedWith(
				TypeError,
				"The custom blocks store factory must return a block store",
			);
		}
	});

	it("can create with the rust storage preset", async () => {
		const clientDirectory = path.join(
			"tmp",
			"peerbit",
			"tests",
			"create-rust-preset-" + uuid(),
		);
		const client = await Peerbit.create({
			directory: clientDirectory,
			...createRustPeerbitOptions(),
		});

		expect(client.storage).to.be.instanceOf(RustAnyStore);
		expect(client.indexer).to.be.instanceOf(RustIndices);
		expect(await client.storage.persisted()).to.be.true;
		expect(await client.indexer.persisted()).to.be.true;
		expect(await client.libp2p.services.blocks.persisted()).to.be.true;
		expect(client.services.blocks.localStoreSafety).to.deep.equal(
			BLOCK_SERVICE_BLOCK_STORE_SAFETY,
		);
		await client.stop();
	});

	it("rejects safety declarations a custom blocks service would ignore", async () => {
		const declaration: BlockStoreSafety = {
			referenceDomain: "caller-exclusive",
			enforcedReclamation: "none",
		};
		for (const blocks of [undefined, (): undefined => undefined]) {
			await expect(
				Peerbit.create({
					libp2p: { services: { blocks } as any },
					storage: {
						blocksStoreFactory: () => createStore(),
						blocksStoreSafety: declaration,
					},
				}),
			).to.be.rejectedWith(
				Error,
				"'storage.blocksStoreSafety' applies only to Peerbit's default blocks service",
			);
		}
	});

	it("rejects safety declarations an external libp2p would ignore", async () => {
		const external = await Peerbit.create();
		try {
			await expect(
				Peerbit.create({
					libp2p: external.libp2p,
					storage: {
						blocksStoreFactory: () => createStore(),
						blocksStoreSafety: {
							referenceDomain: "caller-exclusive",
							enforcedReclamation: "none",
						},
					},
				}),
			).to.be.rejectedWith(
				Error,
				"'storage.blocksStoreSafety' applies only to Peerbit's default blocks service",
			);
		} finally {
			await external.stop();
		}
	});

	it("ignores a Rust preset factory declaration when libp2p is external", async () => {
		const external = await Peerbit.create();
		let wrapper: Peerbit | undefined;
		try {
			wrapper = await Peerbit.create({
				...createRustPeerbitOptions({ network: false }),
				libp2p: external.libp2p,
			});
			expect(wrapper.services.blocks).to.equal(external.services.blocks);
			expect(wrapper.services.blocks.localStoreSafety).to.equal(
				BLOCK_SERVICE_BLOCK_STORE_SAFETY,
			);
		} finally {
			await wrapper?.stop();
			await external.stop();
		}
	});

	it("rust preset wires the native network chain", async () => {
		const client = await Peerbit.create({
			...createRustPeerbitOptions(),
		});
		try {
			const runtime = client.nativeNetwork;
			expect(runtime?.rustCore).to.exist;
			expect(runtime?.wireSync).to.exist;
			// one native core shared by all three DirectStream services
			expect((client.services.pubsub as any).rustCore).to.equal(
				runtime!.rustCore,
			);
			expect((client.services.blocks as any).rustCore).to.equal(
				runtime!.rustCore,
			);
			expect((client.services.fanout as any).rustCore).to.equal(
				runtime!.rustCore,
			);
			// the pubsub inbound decoder is the wire-sync session (receive fusion)
			expect((client.services.pubsub as any).nativeWire).to.equal(
				runtime!.wireSync,
			);
			// programs opened on this client inherit the native shared-log defaults
			expect(client.sharedLogNativeDefaults?.nativeBackbone).to.exist;
			// nativeGraph is advertised as an optional default so a missing
			// @peerbit/log-rust degrades gracefully instead of throwing on open.
			expect(client.sharedLogNativeDefaults?.nativeGraph).to.deep.equal({
				optional: true,
			});
			expect(client.sharedLogNativeDefaults?.sync?.rawExchangeHeads).to.equal(
				true,
			);
			expect(client.sharedLogNativeDefaults?.sync?.nativeWireSync).to.equal(
				runtime!.wireSync,
			);
		} finally {
			await client.stop();
		}
	});

	it("rust preset network can be disabled", async () => {
		const client = await Peerbit.create({
			...createRustPeerbitOptions({ network: false }),
		});
		try {
			expect(client.nativeNetwork).to.equal(undefined);
			expect(client.sharedLogNativeDefaults).to.equal(undefined);
			expect((client.services.pubsub as any).rustCore).to.equal(undefined);
			expect((client.services.pubsub as any).nativeWire).to.equal(undefined);
		} finally {
			await client.stop();
		}
	});

	it("uses the default pubsub upload limit for root and node fanout channels", async () => {
		const client = await Peerbit.create();
		try {
			const snapshot = client.services.pubsub.getRuntimeSnapshot();
			expect(snapshot).to.deep.equal({
				fanout: {
					root: { uploadLimitBps: 5_000_000 },
					node: { uploadLimitBps: 5_000_000 },
				},
			});
			expect(Object.isFrozen(snapshot)).to.equal(true);
			expect(Object.isFrozen(snapshot.fanout)).to.equal(true);
			expect(Object.isFrozen(snapshot.fanout.root)).to.equal(true);
			expect(Object.isFrozen(snapshot.fanout.node)).to.equal(true);
			expect(client.sharedLogNativeDefaults).to.equal(undefined);
		} finally {
			await client.stop();
		}
	});

	it("propagates an explicit pubsub upload limit to all local fanout defaults", async () => {
		const client = await Peerbit.create({
			pubsubUploadLimitBps: 20_000_000,
		});
		try {
			expect(client.services.pubsub.getRuntimeSnapshot()).to.deep.equal({
				fanout: {
					root: { uploadLimitBps: 20_000_000 },
					node: { uploadLimitBps: 20_000_000 },
				},
			});
			expect(
				client.sharedLogNativeDefaults?.fanout?.channel?.uploadLimitBps,
			).to.equal(20_000_000);
		} finally {
			await client.stop();
		}
	});

	it("rejects invalid pubsub upload limits before creating a client", async () => {
		for (const value of [
			0,
			-1,
			null as unknown as number,
			Number.NaN,
			Number.POSITIVE_INFINITY,
			Number.NEGATIVE_INFINITY,
			1.5,
			Number.MAX_SAFE_INTEGER + 1,
		]) {
			await expect(
				Peerbit.create({ pubsubUploadLimitBps: value }),
			).to.be.rejectedWith(
				RangeError,
				"pubsubUploadLimitBps must be a positive safe integer",
			);
		}
	});

	it("normalizes an own undefined pubsub service as absent", async () => {
		const client = await Peerbit.create({
			pubsubUploadLimitBps: 20_000_000,
			libp2p: {
				services: {
					pubsub: undefined,
				},
			},
		});
		try {
			expect(client.services.pubsub).to.exist;
			expect(client.services.pubsub.getRuntimeSnapshot()).to.deep.equal({
				fanout: {
					root: { uploadLimitBps: 20_000_000 },
					node: { uploadLimitBps: 20_000_000 },
				},
			});
		} finally {
			await client.stop();
		}
	});

	it("accepts the positive safe-integer pubsub upload limit boundaries", async () => {
		for (const value of [1, Number.MAX_SAFE_INTEGER]) {
			const client = await Peerbit.create({ pubsubUploadLimitBps: value });
			try {
				const snapshot = client.services.pubsub.getRuntimeSnapshot();
				expect(snapshot.fanout.root.uploadLimitBps).to.equal(value);
				expect(snapshot.fanout.node.uploadLimitBps).to.equal(value);
			} finally {
				await client.stop();
			}
		}
	});

	it("rejects a pubsub upload limit that an external libp2p would ignore", async () => {
		const external = await Peerbit.create();
		try {
			await expect(
				Peerbit.create({
					libp2p: external.libp2p,
					pubsubUploadLimitBps: 20_000_000,
				}),
			).to.be.rejectedWith(
				Error,
				"The 'pubsubUploadLimitBps' option requires Peerbit.create to build the pubsub service",
			);
		} finally {
			await external.stop();
		}
	});

	it("rejects defined pubsub service overrides before opening resources", async () => {
		for (const [label, pubsub] of [
			["custom", (): undefined => undefined],
			["null", null],
			["false", false],
		] as const) {
			const clientDirectory = path.join(
				"tmp",
				"peerbit",
				"tests",
				`create-reject-pubsub-${label}-${uuid()}`,
			);
			let storeFactoryCalls = 0;
			await expect(
				Peerbit.create({
					directory: clientDirectory,
					pubsubUploadLimitBps: 20_000_000,
					libp2p: {
						services: { pubsub } as any,
					},
					storage: {
						storeFactory: (directory) => {
							storeFactoryCalls++;
							return createStore(directory);
						},
					},
				}),
			).to.be.rejectedWith(
				Error,
				"The 'pubsubUploadLimitBps' option requires 'libp2p.services.pubsub' to be omitted or undefined",
			);
			expect(storeFactoryCalls, label).to.equal(0);

			// A retry proves the rejected call did not leave any directory store
			// open or locked.
			const retry = await Peerbit.create({ directory: clientDirectory });
			await retry.stop();
		}
	});

	it("throws when network options are combined with an external libp2p", async () => {
		const external = await Peerbit.create();
		try {
			await expect(
				Peerbit.create({
					libp2p: external.libp2p,
					...createRustPeerbitOptions(),
				}),
			).to.be.rejectedWith(
				Error,
				"The 'network' option requires Peerbit.create to build the libp2p services",
			);
		} finally {
			await external.stop();
		}
	});

	it("does not lock the directory when rejecting network + external libp2p", async () => {
		// The incompatibility must be detected before any store/indexer/
		// datastore is opened, otherwise the rejected call leaves the
		// directory's level stores locked and a retry in the same process
		// fails with LEVEL_LOCKED.
		const clientDirectory = path.join(
			"tmp",
			"peerbit",
			"tests",
			"create-reject-no-lock-" + uuid(),
		);
		const external = await Peerbit.create();
		try {
			await expect(
				Peerbit.create({
					directory: clientDirectory,
					libp2p: external.libp2p,
					...createRustPeerbitOptions(),
				}),
			).to.be.rejectedWith(
				Error,
				"The 'network' option requires Peerbit.create to build the libp2p services",
			);

			// The corrected retry in the same process must succeed — the
			// rejected call must not have left cache/index/libp2p stores open.
			const retry = await Peerbit.create({ directory: clientDirectory });
			await retry.stop();
		} finally {
			await external.stop();
		}
	});

	it("can create with privateKey", async () => {
		const privateKey = await keys.generateKeyPair("Ed25519");
		const client = await Peerbit.create({
			libp2p: { privateKey },
		});
		expect(client.peerId.publicKey!.equals(privateKey.publicKey)).to.be.true;
		await client.stop();
	});

	it("throws when peerId is provided in libp2p options", async () => {
		const peerId = (await Ed25519Keypair.create()).toPeerId();
		await expect(
			Peerbit.create({
				libp2p: { peerId } as any,
			}),
		).to.be.rejectedWith(
			Error,
			"Invalid libp2p option 'peerId'. libp2p derives the peer id from 'privateKey', so pass 'privateKey' to control identity.",
		);
	});

	it("relays by default", async () => {
		const client = await Peerbit.create();
		expect(client.services.blocks.canRelayMessage).equal(true);
		expect(client.services.pubsub.canRelayMessage).equal(true);
		expect(client.services.relay).to.exist;
		await client.stop();
	});
});
