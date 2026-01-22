import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import {
	CanonicalControlRequest,
	CanonicalControlResponse,
	CanonicalFrame,
	CanonicalLoadProgramRequest,
	CanonicalSignRequest,
} from "@peerbit/canonical-transport";
import { Ed25519PublicKey, PreHash, SignatureWithKey } from "@peerbit/crypto";
import { Program } from "@peerbit/program";
import { expect } from "chai";
import {
	CanonicalClient,
	PeerbitCanonicalClient,
	createVariantAdapter,
} from "../src/index.js";

describe("@peerbit/canonical-client", () => {
	it("requests peerId and opens ports", async () => {
		const control = new MessageChannel();
		control.port2.start();
		control.port2.addEventListener("message", (ev) => {
			const bytes = ev.data as Uint8Array;
			const msg = deserialize(bytes, CanonicalFrame) as CanonicalFrame;
			if (!(msg instanceof CanonicalControlRequest)) return;
			if (msg.op === "peerId") {
				control.port2.postMessage(
					serialize(
						new CanonicalControlResponse({
							id: msg.id,
							ok: true,
							peerId: "peer-id",
						}),
					),
				);
			} else if (msg.op === "open") {
				control.port2.postMessage(
					serialize(
						new CanonicalControlResponse({
							id: msg.id,
							ok: true,
							channelId: 1,
						}),
					),
				);
			} else {
				control.port2.postMessage(
					serialize(
						new CanonicalControlResponse({
							id: msg.id,
							ok: false,
							error: "Unknown op",
						}),
					),
				);
			}
		});

		const client = new CanonicalClient(control.port1);

		expect(await client.peerId()).to.equal("peer-id");

		const channel = await client.openPort("any", new Uint8Array([1, 2, 3]));
		expect(channel).to.have.property("send");
		expect(channel).to.have.property("onMessage");
	});

	it("fetches publicKey from the host", async () => {
		const expected = new Ed25519PublicKey({
			publicKey: new Uint8Array(32).fill(7),
		});

		const control = new MessageChannel();
		control.port2.start();
		control.port2.addEventListener("message", (ev) => {
			const bytes = ev.data as Uint8Array;
			const msg = deserialize(bytes, CanonicalFrame) as CanonicalFrame;
			if (!(msg instanceof CanonicalControlRequest)) return;
			if (msg.op !== "peerInfo") return;
			control.port2.postMessage(
				serialize(
					new CanonicalControlResponse({
						id: msg.id,
						ok: true,
						peerId: "peer-id",
						payload: expected.bytes,
						strings: [],
					}),
				),
			);
		});

		const client = new CanonicalClient(control.port1);
		const info = await client.peerInfo();
		const pk = info.publicKey;
		expect(pk).to.be.instanceOf(Ed25519PublicKey);
		expect(Buffer.from(pk.bytes)).to.deep.equal(Buffer.from(expected.bytes));
	});

	it("exposes identity with publicKey + sign()", async () => {
		const expectedPublicKey = new Ed25519PublicKey({
			publicKey: new Uint8Array(32).fill(9),
		});

		const control = new MessageChannel();
		control.port2.start();
		control.port2.addEventListener("message", (ev) => {
			const bytes = ev.data as Uint8Array;
			const msg = deserialize(bytes, CanonicalFrame) as CanonicalFrame;
			if (!(msg instanceof CanonicalControlRequest)) return;

			if (msg.op === "peerInfo") {
				control.port2.postMessage(
					serialize(
						new CanonicalControlResponse({
							id: msg.id,
							ok: true,
							peerId: "peer-id",
							payload: expectedPublicKey.bytes,
							strings: [],
						}),
					),
				);
				return;
			}

			if (msg.op === "sign") {
				const req = deserialize(
					msg.payload as Uint8Array,
					CanonicalSignRequest,
				) as CanonicalSignRequest;

				const signature = new SignatureWithKey({
					signature: new Uint8Array([1, 2, 3]),
					publicKey: expectedPublicKey,
					prehash: (req.prehash ?? PreHash.NONE) as PreHash,
				});

				control.port2.postMessage(
					serialize(
						new CanonicalControlResponse({
							id: msg.id,
							ok: true,
							payload: serialize(signature),
						}),
					),
				);
				return;
			}

			control.port2.postMessage(
				serialize(
					new CanonicalControlResponse({
						id: msg.id,
						ok: false,
						error: "Unknown op",
					}),
				),
			);
		});

		const client = await CanonicalClient.create(control.port1);
		const identity = client.identity;
		expect(identity.publicKey).to.be.instanceOf(Ed25519PublicKey);
		expect(Buffer.from(identity.publicKey.bytes)).to.deep.equal(
			Buffer.from(expectedPublicKey.bytes),
		);

		const sig = await identity.sign(new Uint8Array([4, 5, 6]), PreHash.SHA_256);
		expect(sig).to.be.instanceOf(SignatureWithKey);
		expect(Buffer.from(sig.publicKey.bytes)).to.deep.equal(
			Buffer.from(expectedPublicKey.bytes),
		);
		expect(sig.prehash).to.equal(PreHash.SHA_256);
	});

	it("exposes a Peerbit-like client wrapper", async () => {
		const expectedPublicKey = new Ed25519PublicKey({
			publicKey: new Uint8Array(32).fill(11),
		});
		const expectedPeerId = expectedPublicKey.toPeerId().toString();

		const control = new MessageChannel();
		control.port2.start();
		control.port2.addEventListener("message", (ev) => {
			const bytes = ev.data as Uint8Array;
			const msg = deserialize(bytes, CanonicalFrame) as CanonicalFrame;
			if (!(msg instanceof CanonicalControlRequest)) return;

			if (msg.op === "peerInfo") {
				control.port2.postMessage(
					serialize(
						new CanonicalControlResponse({
							id: msg.id,
							ok: true,
							peerId: expectedPeerId,
							payload: expectedPublicKey.bytes,
							strings: ["/ip4/127.0.0.1/tcp/4001"],
						}),
					),
				);
				return;
			}

			if (msg.op === "sign") {
				const req = deserialize(
					msg.payload as Uint8Array,
					CanonicalSignRequest,
				) as CanonicalSignRequest;
				const signature = new SignatureWithKey({
					signature: new Uint8Array([7, 8, 9]),
					publicKey: expectedPublicKey,
					prehash: (req.prehash ?? PreHash.NONE) as PreHash,
				});
				control.port2.postMessage(
					serialize(
						new CanonicalControlResponse({
							id: msg.id,
							ok: true,
							payload: serialize(signature),
						}),
					),
				);
				return;
			}

			control.port2.postMessage(
				serialize(
					new CanonicalControlResponse({
						id: msg.id,
						ok: false,
						error: "Unknown op",
					}),
				),
			);
		});

		const canonical = new CanonicalClient(control.port1);
		const peer = await PeerbitCanonicalClient.create(canonical);
		expect(peer.peerId.toString()).to.equal(expectedPeerId);
		expect(Buffer.from(peer.identity.publicKey.bytes)).to.deep.equal(
			Buffer.from(expectedPublicKey.bytes),
		);

		const sig = await peer.identity.sign(
			new Uint8Array([1, 2, 3]),
			PreHash.SHA_256,
		);
		expect(sig).to.be.instanceOf(SignatureWithKey);
		expect(Buffer.from(sig.publicKey.bytes)).to.deep.equal(
			Buffer.from(expectedPublicKey.bytes),
		);
		expect(sig.prehash).to.equal(PreHash.SHA_256);
	});

	it("times out requests when the host is unresponsive", async () => {
		const control = new MessageChannel();
		control.port2.start();

		const client = new CanonicalClient(control.port1, { requestTimeoutMs: 20 });
		let error: any;
		try {
			await client.peerId();
		} catch (e) {
			error = e;
		} finally {
			client.close();
			try {
				control.port2.close();
			} catch {}
		}
		expect(error).to.be.instanceOf(Error);
		expect(String(error?.message ?? error)).to.match(/timeout/i);
	});

	it("opens programs by address via loadProgram()", async () => {
		@variant("canonical-test-program")
		class TestProgram extends Program<any> {
			@field({ type: "string" })
			name: string;

			constructor(properties?: { name?: string }) {
				super();
				this.name = properties?.name ?? "";
			}

			async open(): Promise<void> {}
		}

		const expectedPublicKey = new Ed25519PublicKey({
			publicKey: new Uint8Array(32).fill(3),
		});
		const expectedPeerId = expectedPublicKey.toPeerId().toString();

		const address = "bafycanonicaltestaddress";
		const storedProgram = new TestProgram({ name: "hello" });
		const storedBytes = serialize(storedProgram);
		const seenLoadProgram: Array<{ name?: string; payload?: Uint8Array }> = [];

		const adapter = {
			name: "test-adapter",
			canOpen: (program: Program<any>): program is TestProgram =>
				program instanceof TestProgram,
			open: async ({ program }: { program: TestProgram }) => {
				return {
					proxy: {
						name: program.name,
						close: async () => {},
					},
					address: program.address,
				};
			},
		};

		const control = new MessageChannel();
		control.port2.start();
		control.port2.addEventListener("message", (ev) => {
			const bytes = ev.data as Uint8Array;
			const msg = deserialize(bytes, CanonicalFrame) as CanonicalFrame;
			if (!(msg instanceof CanonicalControlRequest)) return;

			if (msg.op === "peerInfo") {
				control.port2.postMessage(
					serialize(
						new CanonicalControlResponse({
							id: msg.id,
							ok: true,
							peerId: expectedPeerId,
							payload: expectedPublicKey.bytes,
							strings: [],
						}),
					),
				);
				return;
			}

			if (msg.op === "loadProgram") {
				seenLoadProgram.push({ name: msg.name, payload: msg.payload });
				control.port2.postMessage(
					serialize(
						new CanonicalControlResponse({
							id: msg.id,
							ok: true,
							payload: storedBytes,
						}),
					),
				);
				return;
			}

			control.port2.postMessage(
				serialize(
					new CanonicalControlResponse({
						id: msg.id,
						ok: false,
						error: "Unknown op",
					}),
				),
			);
		});

		const canonical = new CanonicalClient(control.port1, {
			requestTimeoutMs: 500,
		});
		const peer = await PeerbitCanonicalClient.create(canonical, {
			adapters: [adapter as any],
		});

		const proxy = (await peer.open<TestProgram>(address, {
			timeout: 123,
		})) as any;
		expect(proxy).to.have.property("name", "hello");
		expect(proxy).to.have.property("address", address);

		expect(seenLoadProgram).to.have.length(1);
		expect(seenLoadProgram[0].name).to.equal(address);
		expect(seenLoadProgram[0].payload).to.be.instanceOf(Uint8Array);
		const parsed = deserialize(
			seenLoadProgram[0].payload as Uint8Array,
			CanonicalLoadProgramRequest,
		) as CanonicalLoadProgramRequest;
		expect(parsed.timeoutMs).to.equal(123);

		await proxy.close();
		peer.close();
		try {
			control.port2.close();
		} catch {}
	});

	it("opens programs by adapter.variant without canOpen()", async () => {
		@variant("canonical-test-program-2")
		class TestProgram extends Program<any> {
			@field({ type: "string" })
			name: string;

			constructor(properties?: { name?: string }) {
				super();
				this.name = properties?.name ?? "";
			}

			async open(): Promise<void> {}
		}

		const expectedPublicKey = new Ed25519PublicKey({
			publicKey: new Uint8Array(32).fill(4),
		});
		const expectedPeerId = expectedPublicKey.toPeerId().toString();

		const address = "bafycanonicaltestaddress2";
		const storedProgram = new TestProgram({ name: "hello-2" });
		const storedBytes = serialize(storedProgram);

		const adapter = {
			name: "test-adapter-2",
			variant: "canonical-test-program-2",
			open: async ({ program }: { program: TestProgram }) => {
				return {
					proxy: {
						name: program.name,
						close: async () => {},
					},
					address: program.address,
				};
			},
		};

		const control = new MessageChannel();
		control.port2.start();
		control.port2.addEventListener("message", (ev) => {
			const bytes = ev.data as Uint8Array;
			const msg = deserialize(bytes, CanonicalFrame) as CanonicalFrame;
			if (!(msg instanceof CanonicalControlRequest)) return;

			if (msg.op === "peerInfo") {
				control.port2.postMessage(
					serialize(
						new CanonicalControlResponse({
							id: msg.id,
							ok: true,
							peerId: expectedPeerId,
							payload: expectedPublicKey.bytes,
							strings: [],
						}),
					),
				);
				return;
			}

			if (msg.op === "loadProgram") {
				control.port2.postMessage(
					serialize(
						new CanonicalControlResponse({
							id: msg.id,
							ok: true,
							payload: storedBytes,
						}),
					),
				);
				return;
			}

			control.port2.postMessage(
				serialize(
					new CanonicalControlResponse({
						id: msg.id,
						ok: false,
						error: "Unknown op",
					}),
				),
			);
		});

		const canonical = new CanonicalClient(control.port1, {
			requestTimeoutMs: 500,
		});
		const peer = await PeerbitCanonicalClient.create(canonical, {
			adapters: [adapter as any],
		});

		const proxy = (await peer.open<TestProgram>(address)) as any;
		expect(proxy).to.have.property("name", "hello-2");
		expect(proxy).to.have.property("address", address);

		await proxy.close();
		peer.close();
		try {
			control.port2.close();
		} catch {}
	});

	it("createVariantAdapter() creates a robust variant matcher", async () => {
		@variant("canonical-test-program-3")
		class TestProgram extends Program<any> {
			@field({ type: "string" })
			name: string;

			constructor(properties?: { name?: string }) {
				super();
				this.name = properties?.name ?? "";
			}

			async open(): Promise<void> {}
		}

		const expectedPublicKey = new Ed25519PublicKey({
			publicKey: new Uint8Array(32).fill(5),
		});
		const expectedPeerId = expectedPublicKey.toPeerId().toString();

		const address = "bafycanonicaltestaddress3";
		const storedProgram = new TestProgram({ name: "hello-3" });
		const storedBytes = serialize(storedProgram);

		const adapter = createVariantAdapter<TestProgram, any>({
			name: "test-adapter-3",
			variant: "canonical-test-program-3",
			open: async ({ program }) => {
				return {
					proxy: {
						name: program.name,
						close: async () => {},
					},
					address: program.address,
				};
			},
		});

		const control = new MessageChannel();
		control.port2.start();
		control.port2.addEventListener("message", (ev) => {
			const bytes = ev.data as Uint8Array;
			const msg = deserialize(bytes, CanonicalFrame) as CanonicalFrame;
			if (!(msg instanceof CanonicalControlRequest)) return;

			if (msg.op === "peerInfo") {
				control.port2.postMessage(
					serialize(
						new CanonicalControlResponse({
							id: msg.id,
							ok: true,
							peerId: expectedPeerId,
							payload: expectedPublicKey.bytes,
							strings: [],
						}),
					),
				);
				return;
			}

			if (msg.op === "loadProgram") {
				control.port2.postMessage(
					serialize(
						new CanonicalControlResponse({
							id: msg.id,
							ok: true,
							payload: storedBytes,
						}),
					),
				);
				return;
			}

			control.port2.postMessage(
				serialize(
					new CanonicalControlResponse({
						id: msg.id,
						ok: false,
						error: "Unknown op",
					}),
				),
			);
		});

		const canonical = new CanonicalClient(control.port1, {
			requestTimeoutMs: 500,
		});
		const peer = await PeerbitCanonicalClient.create(canonical, {
			adapters: [adapter],
		});

		const proxy = (await peer.open<TestProgram>(address)) as any;
		expect(proxy).to.have.property("name", "hello-3");
		expect(proxy).to.have.property("address", address);

		await proxy.close();
		peer.close();
		try {
			control.port2.close();
		} catch {}
	});
});
