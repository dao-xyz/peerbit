import { CanonicalClient } from "@peerbit/canonical-client";
import { createMessagePortTransport } from "@peerbit/canonical-transport";
import { Ed25519PublicKey } from "@peerbit/crypto";
import { expect } from "chai";
import {
	type CanonicalContext,
	CanonicalHost,
	type CanonicalModule,
} from "../src/index.js";

const waitFor = async (fn: () => boolean, timeoutMs = 1000) => {
	const start = Date.now();
	while (true) {
		if (fn()) return;
		if (Date.now() - start > timeoutMs) {
			throw new Error("Timed out");
		}
		await new Promise((r) => setTimeout(r, 10));
	}
};

describe("@peerbit/canonical-host", () => {
	it("handles peerId and opens module ports", async () => {
		const ctx: CanonicalContext = {
			peer: async () => {
				throw new Error("not used");
			},
			peerId: async () => "peer-id",
		};

		const host = new CanonicalHost(ctx);

		let openedPayload: Uint8Array | undefined;
		const echoModule: CanonicalModule = {
			name: "echo",
			open: async (_ctx, channel, payload) => {
				openedPayload = payload;
				channel.onMessage((data) => {
					channel.send(data);
				});
			},
		};
		host.registerModule(echoModule);

		const control = new MessageChannel();
		host.attachControlPort(control.port1);
		const client = new CanonicalClient(control.port2);

		expect(await client.peerId()).to.equal("peer-id");

		const payload = new Uint8Array([1, 2, 3]);
		const channel = await client.openPort("echo", payload);
		await waitFor(
			() =>
				!!openedPayload &&
				Buffer.from(openedPayload).equals(Buffer.from(payload)),
			1000,
		);

		const encoder = new TextEncoder();
		const decoder = new TextDecoder();
		let echoed: string | undefined;
		channel.onMessage((data) => {
			echoed = decoder.decode(data);
		});
		channel.send(encoder.encode(JSON.stringify({ hello: "world" })));
		await waitFor(() => !!echoed, 1000);
		expect(JSON.parse(echoed!)).to.deep.equal({ hello: "world" });
	});

	it("accepts peerId strings for hangUp", async () => {
		const peerIdString = new Ed25519PublicKey({
			publicKey: new Uint8Array(32).fill(3),
		})
			.toPeerId()
			.toString();

		const calls: any[] = [];
		const ctx: CanonicalContext = {
			peer: async () =>
				({
					hangUp: async (address: any) => {
						calls.push(address);
						if (typeof address === "string") {
							throw new Error("Expected PeerId");
						}
					},
				}) as any,
			peerId: async () => peerIdString,
		};

		const host = new CanonicalHost(ctx);
		const control = new MessageChannel();
		host.attachControlPort(control.port1);
		const client = new CanonicalClient(control.port2);

		await client.hangUp(peerIdString);

		expect(calls).to.have.length(2);
		expect(calls[0]).to.equal(peerIdString);
		expect(calls[1]?.toString?.()).to.equal(peerIdString);
	});

	it("closes channels when the client closes", async () => {
		const ctx: CanonicalContext = {
			peer: async () => {
				throw new Error("not used");
			},
			peerId: async () => "peer-id",
		};

		const host = new CanonicalHost(ctx);
		let channelClosed = false;
		const mod: CanonicalModule = {
			name: "close-test",
			open: async (_ctx, channel) => {
				channel.onClose?.(() => {
					channelClosed = true;
				});
			},
		};
		host.registerModule(mod);

		const control = new MessageChannel();
		const hostTransport = createMessagePortTransport(control.port1);
		const closeHost = host.attachControlTransport({
			...hostTransport,
			close: undefined,
		});
		const clientTransport = createMessagePortTransport(control.port2);
		const client = new CanonicalClient({
			...clientTransport,
			close: undefined,
		} as any);

		await client.openPort("close-test", new Uint8Array());
		client.close();
		await waitFor(() => channelClosed, 1000);

		closeHost();
		try {
			control.port1.close();
			control.port2.close();
		} catch {}
	});

	it("notifies client channels when the host closes", async () => {
		const ctx: CanonicalContext = {
			peer: async () => {
				throw new Error("not used");
			},
			peerId: async () => "peer-id",
		};

		const host = new CanonicalHost(ctx);
		host.registerModule({
			name: "close-test",
			open: async (_ctx, channel) => {
				channel.onMessage((_data) => {});
			},
		});

		const control = new MessageChannel();
		const hostTransport = createMessagePortTransport(control.port1);
		const closeHost = host.attachControlTransport({
			...hostTransport,
			close: undefined,
		});
		const clientTransport = createMessagePortTransport(control.port2);
		const client = new CanonicalClient({
			...clientTransport,
			close: undefined,
		} as any);

		const channel = await client.openPort("close-test", new Uint8Array());
		let closed = false;
		channel.onClose?.(() => {
			closed = true;
		});

		closeHost();
		await waitFor(() => closed, 1000);

		client.close();
		try {
			control.port1.close();
			control.port2.close();
		} catch {}
	});

	it("closes inactive connections after idleTimeoutMs", async () => {
		const ctx: CanonicalContext = {
			peer: async () => {
				throw new Error("not used");
			},
			peerId: async () => "peer-id",
		};

		const host = new CanonicalHost(ctx, {
			idleTimeoutMs: 30,
			idleCheckIntervalMs: 5,
		});
		let channelClosed = false;
		host.registerModule({
			name: "idle-test",
			open: async (_ctx, channel) => {
				channel.onClose?.(() => {
					channelClosed = true;
				});
			},
		});

		const control = new MessageChannel();
		host.attachControlTransport({
			...createMessagePortTransport(control.port1),
			close: undefined,
		});
		const client = new CanonicalClient({
			...createMessagePortTransport(control.port2),
			close: undefined,
		} as any);

		await client.openPort("idle-test", new Uint8Array());
		await waitFor(() => channelClosed, 1000);

		client.close();
		try {
			control.port1.close();
			control.port2.close();
		} catch {}
	});
});
