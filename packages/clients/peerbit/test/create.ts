/* eslint-disable @typescript-eslint/no-unused-expressions */
import { keys } from "@libp2p/crypto";
import { createStore } from "@peerbit/any-store";
import { RustAnyStore } from "@peerbit/any-store-rust";
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
		});
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
		await client.stop();
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
		await client.stop();
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
