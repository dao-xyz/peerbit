import { getSchema, serialize } from "@dao-xyz/borsh";
import { tcp } from "@libp2p/tcp";
import { webSockets } from "@libp2p/websockets";
import {
	Ed25519Keypair,
	type Ed25519PublicKey,
	type Identity,
	toBase64,
} from "@peerbit/crypto";
import type { Address, Program } from "@peerbit/program";
import { PermissionedString } from "@peerbit/test-lib";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import type { AbstractLevel } from "abstract-level";
import { expect } from "chai";
import fs from "fs";
import http from "http";
import { Level } from "level";
import { MemoryLevel } from "memory-level";
import path, { dirname } from "path";
import type { Peerbit } from "peerbit";
import sinon from "sinon";
import { fileURLToPath } from "url";
import { v4 as uuid } from "uuid";
import { createClient } from "../src/client.js";
import { getTrustPath } from "../src/config.js";
import {
	AUTH_PATH,
	PEER_ID_PATH,
	PROGRAMS_PATH,
	PROGRAM_PATH,
} from "../src/routes.js";
import { startApiServer, startServerWithNode } from "../src/server.js";
import { Session } from "../src/session.js";
import {
	type AuthDescriptor,
	RequestAuthenticator,
	createAuthDescriptor,
	getBody,
	signRequest,
	verifyAuthDescriptor,
	verifyRequestBody,
} from "../src/signed-request.js";
import { Trust } from "../src/trust.js";

const client = (keypair: Identity<Ed25519PublicKey>, address?: string) => {
	return createClient(
		keypair,
		address
			? { address, peerId: keypair.publicKey.toPeerId().toString() }
			: undefined,
	);
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
		server = await startApiServer(session.peers[0] as Peerbit, {
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
			// @ts-ignore
			await node?.stop();
			// @ts-ignore
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

		it("continues startup when a saved dependency is missing", async () => {
			const dirnameResolved = dirname(fileURLToPath(import.meta.url));
			const directory = path.join(
				dirnameResolved,
				"tmp",
				"api-test",
				"server",
				"missing-import",
				uuid(),
			);
			fs.mkdirSync(directory, { recursive: true });

			// Seed a persisted session import that can't be resolved.
			const sessionDirectory = path.join(directory, "session");
			const level = new Level<string, Uint8Array>(sessionDirectory, {
				valueEncoding: "view",
				keyEncoding: "utf-8",
			});
			const imports = level.sublevel<string, Uint8Array>("imports", {
				keyEncoding: "utf8",
				valueEncoding: "view",
			});
			await imports.put(
				"definitely-not-a-real-peerbit-package-123",
				new Uint8Array(),
			);
			await level.close();

			const errorStub = sinon.stub(console, "error");
			try {
				const result = await startServerWithNode({
					directory,
					ports: { api: 0, node: 0 },
				});
				node = result.node;
				server = result.server;
			} finally {
				errorStub.restore();
			}

			expect(errorStub.calledWithMatch(/Failed to import dependency/)).to.equal(
				true,
			);
		});
	});
	describe("api", () => {
		let session: TestSession, serverPeer: Peerbit, server: http.Server;
		let db: PermissionedString;
		let apiAddress: string;
		let directory: string;
		before(async () => {});

		beforeEach(async () => {
			const dirnameResolved = dirname(fileURLToPath(import.meta.url));
			directory = path.join(dirnameResolved, "tmp", "api-test", "api", uuid());
			session = await TestSession.disconnected(2, {
				libp2p: { transports: [tcp(), webSockets()] },
			});
			serverPeer = session.peers[0] as Peerbit;
			db = await serverPeer.open(new PermissionedString({ trusted: [] }));
			fs.mkdirSync(directory, { recursive: true });
			server = await startApiServer(serverPeer, {
				trust: new Trust(getTrustPath(directory)),
				session: new Session(
					new MemoryLevel<string, Uint8Array>({
						valueEncoding: "view",
						keyEncoding: "utf-8",
					}) as unknown as AbstractLevel<
						string | Buffer | Uint8Array,
						string,
						Uint8Array
					>,
				),
				port: 0,
			});
			const addr = server.address();
			if (!addr || typeof addr === "string") {
				throw new Error("Failed to resolve API server address");
			}
			apiAddress = `http://localhost:${addr.port}`;
		});
		afterEach(async () => {
			await new Promise<void>((resolve) => server.close(() => resolve()));
			await db.close();
			await session.stop();
		});

		describe("client", () => {
			it("id", async () => {
				const c = await client(session.peers[0].identity, apiAddress);
				expect(await c.peer.id.get()).equal(serverPeer.peerId.toString());
			});

			it("addresses", async () => {
				const c = await client(session.peers[0].identity, apiAddress);
				expect(
					(await c.peer.addresses.get()).map((x) => x.toString()),
				).to.deep.equal(
					(await serverPeer.getMultiaddrs()).map((x) => x.toString()),
				);
			});

			it("requires an exact public route", async () => {
				expect((await fetch(apiAddress + PEER_ID_PATH)).status).to.equal(200);
				expect(
					(await fetch(apiAddress + PEER_ID_PATH + "?extra=1")).status,
				).to.equal(401);
				expect(
					(await fetch(apiAddress + PEER_ID_PATH + "-extra")).status,
				).to.equal(401);
			});

			it("closes unauthorized connections and rejects bodies on public GETs", async () => {
				const unauthorized = await fetch(apiAddress + PROGRAMS_PATH);
				expect(unauthorized.status).to.equal(401);
				expect(unauthorized.headers.get("connection")).to.equal("close");

				const publicWithBody = await new Promise<{
					status: number | undefined;
					connection: string | undefined;
					body: string;
				}>((resolve, reject) => {
					const request = http.request(
						apiAddress + PEER_ID_PATH,
						{
							method: "GET",
							headers: { "Content-Length": "1" },
						},
						(response) => {
							let body = "";
							response.setEncoding("utf8");
							response.on("data", (chunk) => (body += chunk));
							response.on("end", () =>
								resolve({
									status: response.statusCode,
									connection: response.headers.connection,
									body,
								}),
							);
						},
					);
					request.once("error", reject);
					request.end("x");
				});
				expect(publicWithBody).to.deep.equal({
					status: 400,
					connection: "close",
					body: "Public GET endpoints do not accept a request body",
				});
			});

			it("requires the client to pin the server identity", async () => {
				const unpinned = await createClient(session.peers[0].identity, {
					address: apiAddress,
				});
				await expect(unpinned.program.list()).rejectedWith(
					"pinned server peer ID",
				);

				const other = await Ed25519Keypair.create();
				const wronglyPinned = await createClient(session.peers[0].identity, {
					address: apiAddress,
					peerId: other.publicKey.toPeerId().toString(),
				});
				await expect(wronglyPinned.program.list()).rejectedWith(
					"identity does not match",
				);
			});

			it("accepts exactly one concurrent copy of a signed request", async () => {
				const descriptorResponse = await fetch(apiAddress + AUTH_PATH);
				expect(descriptorResponse.headers.get("cache-control")).to.equal(
					"no-store",
				);
				const descriptor = (await descriptorResponse.json()) as AuthDescriptor;
				const headers: Record<string, string> = {};
				await signRequest(
					headers,
					"GET",
					PROGRAMS_PATH,
					undefined,
					session.peers[0].identity,
					descriptor,
				);
				const responses = await Promise.all([
					fetch(apiAddress + PROGRAMS_PATH, { headers }),
					fetch(apiAddress + PROGRAMS_PATH, { headers }),
				]);
				expect(
					responses.map((response) => response.status).sort(),
				).to.deep.equal([200, 401]);
			});

			it("does not let a mismatched body burn a valid nonce", async () => {
				const descriptor = (await (
					await fetch(apiAddress + AUTH_PATH)
				).json()) as AuthDescriptor;
				const target = PROGRAM_PATH + "/missing";
				const headers: Record<string, string> = {};
				await signRequest(
					headers,
					"DELETE",
					target,
					"right",
					session.peers[0].identity,
					descriptor,
				);
				const raced = await fetch(apiAddress + target, {
					method: "DELETE",
					headers,
					body: "wrong",
				});
				expect(raced.status).to.equal(401);
				const legitimate = await fetch(apiAddress + target, {
					method: "DELETE",
					headers,
					body: "right",
				});
				expect(legitimate.status).to.equal(404);
			});

			it("single-flights descriptor discovery for concurrent requests", async () => {
				let descriptorRequests = 0;
				const observeDescriptor: http.RequestListener = (request) => {
					if (request.method === "GET" && request.url === AUTH_PATH) {
						descriptorRequests += 1;
					}
				};
				server.prependListener("request", observeDescriptor);
				try {
					const c = await client(session.peers[0].identity, apiAddress);
					await Promise.all([c.program.list(), c.peer.stats.get()]);
					expect(descriptorRequests).to.equal(1);
				} finally {
					server.removeListener("request", observeDescriptor);
				}
			});

			it("briefly caches and then refreshes the signed descriptor", async () => {
				let wallClockMs = 1_750_000_000_000;
				const timedDirectory = path.join(
					process.cwd(),
					"tmp",
					"api-test",
					"timed-auth",
					uuid(),
				);
				fs.mkdirSync(timedDirectory, { recursive: true });
				const timed = await startApiServer(serverPeer, {
					trust: new Trust(getTrustPath(timedDirectory)),
					port: 0,
					signedRequests: { wallClockMs: () => wallClockMs },
				});
				try {
					const address = timed.address();
					if (!address || typeof address === "string") {
						throw new Error("Failed to resolve timed API server address");
					}
					const endpoint = `http://127.0.0.1:${address.port}${AUTH_PATH}`;
					const first = (await (
						await fetch(endpoint)
					).json()) as AuthDescriptor;
					wallClockMs += 1_000;
					const second = (await (
						await fetch(endpoint)
					).json()) as AuthDescriptor;
					wallClockMs += 30_000;
					const third = (await (
						await fetch(endpoint)
					).json()) as AuthDescriptor;
					expect(first.serverTime).to.equal("1750000000");
					expect(second).to.deep.equal(first);
					expect(third.serverTime).to.equal("1750000031");
					expect(third.bootId).to.equal(first.bootId);
					await verifyAuthDescriptor(third, serverPeer.peerId.toString(), {
						nowMs: wallClockMs,
					});
				} finally {
					await new Promise<void>((resolve) => timed.close(() => resolve()));
				}
			});

			it("does not forward signed requests across redirects", async () => {
				const descriptor = await (await fetch(apiAddress + AUTH_PATH)).text();
				let sinkRequests = 0;
				const sink = http.createServer((request, response) => {
					sinkRequests += 1;
					request.resume();
					response.writeHead(200);
					response.end("captured");
				});
				const listen = (target: http.Server) =>
					new Promise<number>((resolve, reject) => {
						target.once("error", reject);
						target.listen(0, "127.0.0.1", () => {
							const address = target.address();
							if (!address || typeof address === "string") {
								reject(new Error("Failed to resolve test server address"));
								return;
							}
							resolve(address.port);
						});
					});
				const close = (target: http.Server) =>
					new Promise<void>((resolve) => target.close(() => resolve()));
				let proxy: http.Server | undefined;
				try {
					const sinkPort = await listen(sink);
					proxy = http.createServer((request, response) => {
						request.resume();
						if (request.method === "GET" && request.url === AUTH_PATH) {
							response.setHeader("Content-Type", "application/json");
							response.end(descriptor);
							return;
						}
						response.writeHead(307, {
							Location: `http://127.0.0.1:${sinkPort}/captured`,
						});
						response.end();
					});
					const proxyPort = await listen(proxy);
					const c = await createClient(session.peers[0].identity, {
						address: `http://127.0.0.1:${proxyPort}`,
						peerId: serverPeer.peerId.toString(),
					});
					await expect(c.program.list()).rejectedWith("status code 307");
					expect(sinkRequests).to.equal(0);
				} finally {
					if (proxy?.listening) await close(proxy);
					if (sink.listening) await close(sink);
				}
			});

			it("sends exactly an offset Uint8Array view over Axios HTTP", async () => {
				const descriptor = await createAuthDescriptor(serverPeer.identity, {
					serverPeerId: serverPeer.peerId.toString(),
					bootId: toBase64(new Uint8Array(32).fill(9)),
				});
				const authenticator = new RequestAuthenticator({
					...descriptor,
					isTrusted: (key) => key.equals(serverPeer.identity.publicKey),
				});
				let received: Uint8Array | undefined;
				const wireServer = http.createServer(async (request, response) => {
					if (request.method === "GET" && request.url === AUTH_PATH) {
						response.setHeader("Content-Type", "application/json");
						response.end(JSON.stringify(descriptor));
						return;
					}
					try {
						const verified = await authenticator.verify(
							request.headers,
							request.method!,
							request.url!,
						);
						received = await getBody(request, verified.bodyLength);
						verifyRequestBody(verified, received);
						authenticator.consume(verified);
						response.writeHead(200);
						response.end();
					} catch (error: any) {
						response.writeHead(500);
						response.end(error?.toString());
					}
				});
				await new Promise<void>((resolve, reject) => {
					wireServer.once("error", reject);
					wireServer.listen(0, "127.0.0.1", () => resolve());
				});
				try {
					const address = wireServer.address();
					if (!address || typeof address === "string") {
						throw new Error("Failed to resolve wire server address");
					}
					const { default: axios } = await import("axios");
					const originalCreate = axios.create.bind(axios);
					const instances: ReturnType<typeof axios.create>[] = [];
					const createStub = sinon.stub(axios, "create");
					createStub.callsFake(((config: any) => {
						const instance = originalCreate(config);
						instances.push(instance);
						return instance;
					}) as typeof axios.create);
					try {
						await createClient(serverPeer.identity, {
							address: `http://127.0.0.1:${address.port}`,
							peerId: serverPeer.peerId.toString(),
						});
					} finally {
						createStub.restore();
					}
					expect(instances).to.have.length(2);

					const backing = new TextEncoder().encode("xxwire 🌍 bytesyy");
					const view = backing.subarray(2, backing.length - 2);
					await instances[0].put(`http://127.0.0.1:${address.port}/wire`, view);
					expect(received).to.deep.equal(view);
				} finally {
					await new Promise<void>((resolve) =>
						wireServer.close(() => resolve()),
					);
				}
			});

			it("does not verify an endpoint that only echoes the pinned ID", async () => {
				const fake = http.createServer((request, response) => {
					if (request.url === PEER_ID_PATH) {
						response.end(serverPeer.peerId.toString());
						return;
					}
					response.setHeader("Content-Type", "application/json");
					response.end(
						JSON.stringify({
							version: "2",
							serverPeerId: serverPeer.peerId.toString(),
							bootId: toBase64(new Uint8Array(32).fill(1)),
							serverTime: "1750000000",
							signature: "invalid",
						}),
					);
				});
				await new Promise<void>((resolve, reject) => {
					fake.once("error", reject);
					fake.listen(0, "127.0.0.1", () => resolve());
				});
				try {
					const address = fake.address();
					if (!address || typeof address === "string") {
						throw new Error("Failed to resolve fake server address");
					}
					const c = await createClient(serverPeer.identity, {
						address: `http://127.0.0.1:${address.port}`,
						peerId: serverPeer.peerId.toString(),
					});
					expect(await c.peer.id.get()).to.equal(serverPeer.peerId.toString());
					await expect(c.peer.id.verify()).rejectedWith(
						"identity does not match",
					);
				} finally {
					await new Promise<void>((resolve) => fake.close(() => resolve()));
				}
			});

			it("refreshes a restarted server on the next call without retrying", async () => {
				const c = await client(session.peers[0].identity, apiAddress);
				await c.program.list();
				const firstAddress = server.address();
				if (!firstAddress || typeof firstAddress === "string") {
					throw new Error("Failed to resolve API server address");
				}
				await new Promise<void>((resolve) => server.close(() => resolve()));
				const restartDirectory = path.join(
					process.cwd(),
					"tmp",
					"api-test",
					"restart",
					uuid(),
				);
				fs.mkdirSync(restartDirectory, { recursive: true });
				server = await startApiServer(serverPeer, {
					trust: new Trust(getTrustPath(restartDirectory)),
					port: firstAddress.port,
				});

				let descriptorRequests = 0;
				let protectedRequests = 0;
				const observeRequests: http.RequestListener = (request) => {
					if (request.url === AUTH_PATH) descriptorRequests += 1;
					if (request.url === PROGRAMS_PATH) protectedRequests += 1;
				};
				server.prependListener("request", observeRequests);
				try {
					await expect(c.program.list()).to.be.rejected;
					const failedProtectedRequests = protectedRequests;
					expect(failedProtectedRequests).to.be.at.most(1);
					expect(descriptorRequests).to.equal(0);

					await c.program.list();
					expect(protectedRequests).to.equal(failedProtectedRequests + 1);
					expect(descriptorRequests).to.equal(1);
				} finally {
					server.removeListener("request", observeRequests);
				}
			});

			it("uses a distinct boot audience for each API server", async () => {
				const secondDirectory = path.join(
					process.cwd(),
					"tmp",
					"api-test",
					"second-api",
					uuid(),
				);
				fs.mkdirSync(secondDirectory, { recursive: true });
				const second = await startApiServer(serverPeer, {
					trust: new Trust(getTrustPath(secondDirectory)),
					port: 0,
				});
				try {
					const secondAddress = second.address();
					if (!secondAddress || typeof secondAddress === "string") {
						throw new Error("Failed to resolve second API address");
					}
					const firstDescriptor = (await (
						await fetch(apiAddress + AUTH_PATH)
					).json()) as AuthDescriptor;
					const secondBase = `http://localhost:${secondAddress.port}`;
					const secondDescriptor = (await (
						await fetch(secondBase + AUTH_PATH)
					).json()) as AuthDescriptor;
					expect(secondDescriptor.serverPeerId).to.equal(
						firstDescriptor.serverPeerId,
					);
					expect(secondDescriptor.bootId).not.to.equal(firstDescriptor.bootId);

					const headers: Record<string, string> = {};
					await signRequest(
						headers,
						"GET",
						PROGRAMS_PATH,
						undefined,
						session.peers[0].identity,
						firstDescriptor,
					);
					expect(
						(await fetch(secondBase + PROGRAMS_PATH, { headers })).status,
					).to.equal(401);
				} finally {
					await new Promise<void>((resolve) => second.close(() => resolve()));
				}
			});
		});

		describe("program", () => {
			describe("open", () => {
				it("variant", async () => {
					const c = await client(session.peers[0].identity, apiAddress);
					const address = await c.program.open({
						variant: getSchema(PermissionedString).variant! as string,
					});
					expect(await c.program.has(address)).to.be.true;
				});

				it("base64", async () => {
					const c = await client(session.peers[0].identity, apiAddress);
					const program = new PermissionedString({
						trusted: [],
					});
					const address = await c.program.open({
						base64: toBase64(serialize(program)),
					});
					expect(await c.program.has(address)).to.be.true;
				});

				it("with args", async () => {
					const c = await client(session.peers[0].identity, apiAddress);
					const serverOpen = sinon.spy(serverPeer, "open");
					serverPeer.open = serverOpen as any;

					const address = await c.program.open({
						variant: getSchema(PermissionedString).variant! as string,
						log: true,
					});
					expect(await c.program.has(address)).to.be.true;
					expect(serverOpen.args[0][1]).to.deep.equal({ args: { log: true } });
				});
			});

			describe("trust", () => {
				it("add", async () => {
					const c = await client(session.peers[0].identity, apiAddress);
					const kp2 = await Ed25519Keypair.create();
					const kp3 = await Ed25519Keypair.create();

					const c2 = await createClient(kp2, {
						address: apiAddress,
						peerId: serverPeer.peerId.toString(),
					});
					await expect(c2.access.allow(kp3.publicKey)).rejectedWith(
						"Request failed with status code 401",
					);
					await c.access.allow(kp2.publicKey);
					await c2.access.allow(kp3.publicKey); // now c2 can add since it is trusted by c
				});

				it("reports and persists access revocation", async () => {
					const c = await client(session.peers[0].identity, apiAddress);
					const revoked = (await Ed25519Keypair.create()).publicKey;
					const revokedHash = revoked.hashcode();

					expect(await c.access.allow(revoked)).to.equal(true);
					expect(
						new Trust(getTrustPath(directory)).isTrusted(revokedHash),
					).to.equal(true);

					expect(await c.access.deny(revoked)).to.equal(true);
					expect(await c.access.deny(revoked)).to.equal(false);

					const reloaded = new Trust(getTrustPath(directory));
					expect(reloaded.isTrusted(revokedHash)).to.equal(false);
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

					const c = await client(session.peers[0].identity, apiAddress);
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
					const c = await client(session.peers[0].identity, apiAddress);
					await c.program.close(address);
					expect(dropped).to.be.false;
					expect(closed).to.be.true;
				});

				it("drop", async () => {
					const c = await client(session.peers[0].identity, apiAddress);
					await c.program.drop(address);
					expect(dropped).to.be.true;
					expect(closed).to.be.false;
				});
			});

			it("list arg value", async () => {
				const c = await client(session.peers[0].identity, apiAddress);
				const address = await c.program.open({
					variant: getSchema(PermissionedString).variant! as string,
					log: true,
				});
				const map = await c.program.list();
				expect(map[address]).to.deep.eq({ log: true });
			});

			it("list arg undefined", async () => {
				const c = await client(session.peers[0].identity, apiAddress);
				const address = await c.program.open({
					variant: getSchema(PermissionedString).variant! as string,
				});
				const map = await c.program.list();
				expect(map[address]).to.deep.eq(null);
			});

			/* TODO tet correctly 
			it("list args after restart", async () => {
				const c = await client(session.peers[0].identity);
				const address = await c.program.open({
					variant: getSchema(PermissionedString).variant! as string,
					log: true,
				});
				await c.restart();
				await waitForResolved(async () => {
					const map = await c.program.list();
					expect(map[address]).to.deep.eq({ log: true });
				}, {
					delayInterval: 5e2,
				})
			}) */
		});

		it("bootstrap", async () => {
			expect((session.peers[0] as Peerbit).services.pubsub.peers.size).equal(0);
			const c = await client(session.peers[0].identity, apiAddress);
			const bootstrapAddresses = (session.peers[1] as Peerbit)
				.getMultiaddrs()
				.map((x) => x.toString());
			await c.network.bootstrap({ addresses: bootstrapAddresses });
			await waitForResolved(() =>
				expect(
					(session.peers[0] as Peerbit).services.pubsub.peers.size,
				).greaterThan(0),
			);
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
