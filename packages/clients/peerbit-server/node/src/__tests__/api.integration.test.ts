import { TestSession } from "@peerbit/test-utils";
import http from "http";
import { startApiServer, startServerWithNode } from "../server.js";
import { jest } from "@jest/globals";
import { PermissionedString } from "@peerbit/test-lib";
import { Address, Program, ProgramClient } from "@peerbit/program";
import { getSchema, serialize } from "@dao-xyz/borsh";
import {
	Ed25519Keypair,
	Ed25519PublicKey,
	Identity,
	toBase64
} from "@peerbit/crypto";
import { Peerbit } from "peerbit";
import { tcp } from "@libp2p/tcp";
import { webSockets } from "@libp2p/websockets";
import { createClient as createClient } from "../client.js";
import { v4 as uuid } from "uuid";
import path, { dirname } from "path";
import { Trust } from "../trust.js";
import { getTrustPath } from "../config.js";
import fs from "fs";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

const client = (keypair: Identity<Ed25519PublicKey>, address?: string) => {
	return createClient(keypair, address ? { address } : undefined);
};

describe("libp2p only", () => {
	let session: TestSession, server: http.Server;
	jest.setTimeout(60 * 1000);
	let configDirectory: string;

	beforeAll(async () => {
		session = await TestSession.connected(1);
	});

	beforeEach(async () => {
		session.peers[0].services.pubsub.subscribe("1");
		session.peers[0].services.pubsub.subscribe("2");
		session.peers[0].services.pubsub.subscribe("3");
		configDirectory = path.join(
			__dirname,
			"tmp",
			"api-test",
			"libp2ponly",
			uuid()
		);
		fs.mkdirSync(configDirectory, { recursive: true });
		server = await startApiServer(session.peers[0], {
			trust: new Trust(getTrustPath(configDirectory)),
			port: 7676
		});
	});
	afterEach(() => {
		server.close();
	});

	afterAll(async () => {
		await session.stop();
	});

	it("use cli as libp2p cli", async () => {
		const c = await createClient(await Ed25519Keypair.create(), {
			address: "http://localhost:" + 7676
		});
		expect(await c.peer.id.get()).toBeDefined();
	});
});

describe("server", () => {
	describe("with node", () => {
		let server: http.Server;
		let node: Peerbit;

		afterEach(async () => {
			await node?.stop();
			server?.close();
		});
		it("bootstrap on start", async () => {
			// TMP disable until bootstrap nodes have migrated
			/* 	let result = await startServerWithNode({
					bootstrap: true,
					directory: path.join(__dirname, "tmp", "api-test", "server", uuid())
				});
				node = result.node;
				server = result.server;
				expect(node.libp2p.services.pubsub.peers.size).toBeGreaterThan(0); */
		});
	});
	describe("api", () => {
		let session: TestSession,
			peer: ProgramClient,
			address: Address,
			server: http.Server;
		let db: PermissionedString;

		beforeAll(async () => {});

		beforeEach(async () => {
			let directory = path.join(__dirname, "tmp", "api-test", "api", uuid());
			session = await TestSession.connected(1, {
				libp2p: { transports: [tcp(), webSockets()] }
			});
			peer = session.peers[0];
			db = await peer.open(new PermissionedString({ trusted: [] }));
			address = db.address;
			fs.mkdirSync(directory, { recursive: true });
			server = await startApiServer(peer, {
				trust: new Trust(getTrustPath(directory))
			});
		});
		afterEach(async () => {
			server.close();
			await db.close();
			await session.stop();
		});

		describe("client", () => {
			it("id", async () => {
				const c = await client(session.peers[0].identity);
				expect(await c.peer.id.get()).toEqual(peer.peerId.toString());
			});

			it("addresses", async () => {
				const c = await client(session.peers[0].identity);
				expect((await c.peer.addresses.get()).map((x) => x.toString())).toEqual(
					(await peer.getMultiaddrs()).map((x) => x.toString())
				);
			});
		});

		describe("program", () => {
			describe("open", () => {
				it("variant", async () => {
					const c = await client(session.peers[0].identity);
					const address = await c.program.open({
						variant: getSchema(PermissionedString).variant! as string
					});
					expect(await c.program.has(address)).toBeTrue();
				});

				it("base64", async () => {
					const c = await client(session.peers[0].identity);
					const program = new PermissionedString({
						trusted: []
					});
					const address = await c.program.open({
						base64: toBase64(serialize(program))
					});
					expect(await c.program.has(address)).toBeTrue();
				});
			});

			describe("trust", () => {
				it("add", async () => {
					const c = await client(session.peers[0].identity);
					const kp2 = await Ed25519Keypair.create();
					const kp3 = await Ed25519Keypair.create();

					const c2 = await client(kp2);
					await expect(() =>
						c2.access.allow(kp3.publicKey)
					).rejects.toThrowError("Request failed with status code 401");
					await c.access.allow(kp2.publicKey);
					await c2.access.allow(kp3.publicKey); // now c2 can add since it is trusted by c
				});
			});
			describe("close/drop", () => {
				let program: Program;
				let address: Address;
				let dropped = false;
				let closed = false;

				beforeEach(async () => {
					dropped = false;
					closed = false;

					const c = await client(session.peers[0].identity);
					address = await c.program.open({
						variant: getSchema(PermissionedString).variant! as string
					});
					program = (session.peers[0] as Peerbit).handler.items.get(address)!;

					const dropFn = program.drop.bind(program);
					program.drop = (from) => {
						dropped = true;
						return dropFn(from);
					};

					const closeFn = program.close.bind(program);
					program.close = (from) => {
						closed = true;
						return closeFn(from);
					};
				});

				it("close", async () => {
					const c = await client(session.peers[0].identity);
					await c.program.close(address);
					expect(dropped).toBeFalse();
					expect(closed).toBeTrue();
				});

				it("drop", async () => {
					const c = await client(session.peers[0].identity);
					await c.program.drop(address);
					expect(dropped).toBeTrue();
					expect(closed).toBeFalse();
				});
			});

			it("list", async () => {
				const c = await client(session.peers[0].identity);
				const address = await c.program.open({
					variant: getSchema(PermissionedString).variant! as string
				});
				expect(await c.program.list()).toContain(address);
			});
		});

		it("bootstrap", async () => {
			// TMP disable until bootstrap nodes have migrated
			/* expect((session.peers[0] as Peerbit).services.pubsub.peers.size).toEqual(
				0
			);
			const c = await client(session.peers[0].identity);
			await c.network.bootstrap();
			expect(
				(session.peers[0] as Peerbit).services.pubsub.peers.size
			).toBeGreaterThan(0); */
		});

		/* TODO how to test this properly? Seems to hang once we added 'sudo --prefix __dirname' to the npm install in the child_process
		it("dependency", async () => {
			const c = await client();
			const result = await c.dependency.install("@peerbit/test-lib");
			expect(result).toEqual([]); // will already be imported in this test env. TODO make test better here, so that new programs are discvovered on import
		}); */
	});

	/*  TODO feat
	
	it("topics", async () => {
		const c = await client();
		expect(await c.topics.get(true)).toEqual([address.toString()]);
	});
	
	
	 */

	/*  TODO add network functionality
	
	it("network", async () => {
		const c = await client();
		const program = new PermissionedString({
			store: new DString({}),
			trusted: [peer.identity.publicKey],
		});
		program.setupIndices();
		const address = await c.program.put(program, "topic");
		expect(await c.program.get(address)).toBeInstanceOf(PermissionedString);
		expect(await c.network.peers.get(address)).toHaveLength(0);
		const pk = (await Ed25519Keypair.create()).publicKey;
		await c.network.peer.put(address, pk);
		const peers = await c.network.peers.get(address);
		expect(peers).toHaveLength(1);
		expect(
			(peers?.[0] as IdentityRelation).from.equals(
				peer.identity.publicKey
			)
		);
		expect((peers?.[0] as IdentityRelation).to.equals(pk));
	}); */
});
