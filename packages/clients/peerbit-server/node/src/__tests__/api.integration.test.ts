import { LSession } from "@peerbit/test-utils";
import http from "http";
import { startServer, startServerWithNode } from "../server.js";
import { jest } from "@jest/globals";
import { PermissionedString } from "@peerbit/test-lib";
import { Address, Program, ProgramClient } from "@peerbit/program";
import { getSchema, serialize } from "@dao-xyz/borsh";
import { toBase64 } from "@peerbit/crypto";
import { Peerbit } from "peerbit";
import { tcp } from "@libp2p/tcp";
import { webSockets } from "@libp2p/websockets";
import { client } from "../client.js";

describe("libp2p only", () => {
	let session: LSession, server: http.Server;
	jest.setTimeout(60 * 1000);

	beforeAll(async () => {
		session = await LSession.connected(1);
	});

	beforeEach(async () => {
		session.peers[0].services.pubsub.subscribe("1", {
			data: new Uint8Array([1]),
		});
		session.peers[0].services.pubsub.subscribe("2", {
			data: new Uint8Array([2]),
		});
		session.peers[0].services.pubsub.subscribe("3", {
			data: new Uint8Array([3]),
		});
		server = await startServer(session.peers[0], 7676);
	});
	afterEach(() => {
		server.close();
	});

	afterAll(async () => {
		await session.stop();
	});

	it("use cli as libp2p cli", async () => {
		const c = await client("http://localhost:" + 7676);
		expect(await c.peer.id.get()).toBeDefined();
	});
});

describe("server", () => {
	describe("with node", () => {
		let server: http.Server;
		let node: Peerbit;

		afterEach(async () => {
			await node.stop();
			server.close();
		});
		it("bootstrap on start", async () => {
			let result = await startServerWithNode({ bootstrap: true });
			node = result.node;
			server = result.server;
			expect(node.libp2p.services.pubsub.peers.size).toBeGreaterThan(0);
		});
	});
	describe("api", () => {
		let session: LSession,
			peer: ProgramClient,
			address: Address,
			server: http.Server;
		let db: PermissionedString;

		beforeAll(async () => {});

		beforeEach(async () => {
			session = await LSession.connected(1, {
				directory: "./peerbit/" + +new Date(),
				libp2p: { transports: [tcp(), webSockets()] },
			});
			peer = session.peers[0];
			db = await peer.open(new PermissionedString({ trusted: [] }));
			address = db.address;
			server = await startServer(peer);
		});
		afterEach(async () => {
			server.close();
			await db.close();
			await session.stop();
		});

		describe("client", () => {
			it("id", async () => {
				const c = await client();
				expect(await c.peer.id.get()).toEqual(peer.peerId.toString());
			});
			it("addresses", async () => {
				const c = await client();
				expect((await c.peer.addresses.get()).map((x) => x.toString())).toEqual(
					(await peer.getMultiaddrs()).map((x) => x.toString())
				);
			});
		});

		describe("program", () => {
			describe("open", () => {
				it("variant", async () => {
					const c = await client();
					const address = await c.program.open({
						variant: getSchema(PermissionedString).variant! as string,
					});
					expect(await c.program.has(address)).toBeTrue();
				});

				it("base64", async () => {
					const c = await client();
					const program = new PermissionedString({
						trusted: [],
					});
					const address = await c.program.open({
						base64: toBase64(serialize(program)),
					});
					expect(await c.program.has(address)).toBeTrue();
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

					const c = await client();
					address = await c.program.open({
						variant: getSchema(PermissionedString).variant! as string,
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
					const c = await client();
					await c.program.close(address);
					expect(dropped).toBeFalse();
					expect(closed).toBeTrue();
				});

				it("drop", async () => {
					const c = await client();
					await c.program.drop(address);
					expect(dropped).toBeTrue();
					expect(closed).toBeFalse();
				});
			});

			it("list", async () => {
				const c = await client();
				const address = await c.program.open({
					variant: getSchema(PermissionedString).variant! as string,
				});
				expect(await c.program.list()).toContain(address);
			});
		});

		it("bootstrap", async () => {
			expect((session.peers[0] as Peerbit).services.pubsub.peers.size).toEqual(
				0
			);
			const c = await client();
			await c.network.bootstrap();
			expect(
				(session.peers[0] as Peerbit).services.pubsub.peers.size
			).toBeGreaterThan(0);
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
