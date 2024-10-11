import { getSchema, serialize } from "@dao-xyz/borsh";
import { tcp } from "@libp2p/tcp";
import { webSockets } from "@libp2p/websockets";
import {
	Ed25519Keypair,
	type Ed25519PublicKey,
	type Identity,
	toBase64,
} from "@peerbit/crypto";
import type { Address, Program, ProgramClient } from "@peerbit/program";
import { PermissionedString } from "@peerbit/test-lib";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import fs from "fs";
import type http from "http";
import path, { dirname } from "path";
import type { Peerbit } from "peerbit";
import { fileURLToPath } from "url";
import { v4 as uuid } from "uuid";
import { createClient } from "../src/client.js";
import { getTrustPath } from "../src/config.js";
import { startApiServer } from "../src/server.js";
import { Trust } from "../src/trust.js";

const client = (keypair: Identity<Ed25519PublicKey>, address?: string) => {
	return createClient(keypair, address ? { address } : undefined);
};
describe("libp2p only", () => {
	// eslint-disable-next-line @typescript-eslint/naming-convention

	let session: TestSession, server: http.Server;
	let configDirectory: string;

	before(async () => {
		session = await TestSession.connected(1);
	});

	beforeEach(async () => {
		session.peers[0].services.pubsub.subscribe("1");
		session.peers[0].services.pubsub.subscribe("2");
		session.peers[0].services.pubsub.subscribe("3");
		const dirnameResolved = dirname(fileURLToPath(import.meta.url));
		configDirectory = path.join(
			dirnameResolved,
			"tmp",
			"api-test",
			"libp2ponly",
			uuid(),
		);
		fs.mkdirSync(configDirectory, { recursive: true });
		server = await startApiServer(session.peers[0], {
			trust: new Trust(getTrustPath(configDirectory)),
			port: 7676,
		});
	});
	afterEach(() => {
		server.close();
	});

	after(async () => {
		await session.stop();
	});

	it("use cli as libp2p cli", async () => {
		const c = await createClient(await Ed25519Keypair.create(), {
			address: "http://localhost:" + 7676,
		});
		expect(await c.peer.id.get()).to.exist;
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
				expect(node.libp2p.services.pubsub.peers.size).greaterThan(0); */
		});
	});
	describe("api", () => {
		let session: TestSession, peer: ProgramClient, server: http.Server;
		let db: PermissionedString;

		before(async () => {});

		beforeEach(async () => {
			const dirnameResolved = dirname(fileURLToPath(import.meta.url));
			let directory = path.join(
				dirnameResolved,
				"tmp",
				"api-test",
				"api",
				uuid(),
			);
			session = await TestSession.connected(1, {
				libp2p: { transports: [tcp(), webSockets()] },
			});
			peer = session.peers[0];
			db = await peer.open(new PermissionedString({ trusted: [] }));
			fs.mkdirSync(directory, { recursive: true });
			server = await startApiServer(peer, {
				trust: new Trust(getTrustPath(directory)),
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
				expect(await c.peer.id.get()).equal(peer.peerId.toString());
			});

			it("addresses", async () => {
				const c = await client(session.peers[0].identity);
				expect(
					(await c.peer.addresses.get()).map((x) => x.toString()),
				).to.deep.equal((await peer.getMultiaddrs()).map((x) => x.toString()));
			});
		});

		describe("program", () => {
			describe("open", () => {
				it("variant", async () => {
					const c = await client(session.peers[0].identity);
					const address = await c.program.open({
						variant: getSchema(PermissionedString).variant! as string,
					});
					expect(await c.program.has(address)).to.be.true;
				});

				it("base64", async () => {
					const c = await client(session.peers[0].identity);
					const program = new PermissionedString({
						trusted: [],
					});
					const address = await c.program.open({
						base64: toBase64(serialize(program)),
					});
					expect(await c.program.has(address)).to.be.true;
				});
			});

			describe("trust", () => {
				it("add", async () => {
					const c = await client(session.peers[0].identity);
					const kp2 = await Ed25519Keypair.create();
					const kp3 = await Ed25519Keypair.create();

					const c2 = await client(kp2);
					await expect(c2.access.allow(kp3.publicKey)).rejectedWith(
						"Request failed with status code 401",
					);
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
					const c = await client(session.peers[0].identity);
					await c.program.close(address);
					expect(dropped).to.be.false;
					expect(closed).to.be.true;
				});

				it("drop", async () => {
					const c = await client(session.peers[0].identity);
					await c.program.drop(address);
					expect(dropped).to.be.true;
					expect(closed).to.be.false;
				});
			});

			it("list", async () => {
				const c = await client(session.peers[0].identity);
				const address = await c.program.open({
					variant: getSchema(PermissionedString).variant! as string,
				});
				expect(await c.program.list()).include(address);
			});
		});

		it("bootstrap", async () => {
			expect((session.peers[0] as Peerbit).services.pubsub.peers.size).equal(0);
			const c = await client(session.peers[0].identity);
			await c.network.bootstrap();
			expect(
				(session.peers[0] as Peerbit).services.pubsub.peers.size,
			).greaterThan(0);
		});

		/* TODO how to test this properly? Seems to hang once we added 'sudo --prefix __dirname' to the npm install in the child_process
		it("dependency", async () => {
			const c = await client();
			const result = await c.dependency.install("@peerbit/test-lib");
			expect(result).to.be.empty; // will already be imported in this test env. TODO make test better here, so that new programs are discvovered on import
		}); */
	});

	/*  TODO feat
	
	it("topics", async () => {
		const c = await client();
		expect(await c.topics.get(true)).to.deep.equal([address.toString()]);
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
		expect(await c.program.get(address)).to.be.instanceOf(PermissionedString);
		expect(await c.network.peers.get(address)).to.be.empty;
		const pk = (await Ed25519Keypair.create()).publicKey;
		await c.network.peer.put(address, pk);
		const peers = await c.network.peers.get(address);
		expect(peers).to.have.length(1);
		expect(
			(peers?.[0] as IdentityRelation).from.equals(
				peer.identity.publicKey
			)
		);
		expect((peers?.[0] as IdentityRelation).to.equals(pk));
	}); */
});
