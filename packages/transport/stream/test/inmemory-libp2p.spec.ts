import { delay } from "@peerbit/time";
import { expect } from "chai";
import {
	InMemoryConnectionManager,
	InMemoryNetwork,
	publicKeyHash,
} from "../benchmark/sim/inmemory-libp2p.js";

describe("in-memory libp2p shim", () => {
	it("exposes dialQueue entries during dial delay", async () => {
		const network = new InMemoryNetwork({ dialDelayMs: 25 });
		const { runtime: a } = InMemoryNetwork.createPeer({
			index: 0,
			port: 31_000,
			network,
		});
		const { runtime: b } = InMemoryNetwork.createPeer({
			index: 1,
			port: 31_001,
			network,
		});

		a.connectionManager = new InMemoryConnectionManager(network, a);
		b.connectionManager = new InMemoryConnectionManager(network, b);
		network.registerPeer(a, 31_000);
		network.registerPeer(b, 31_001);

		const dialPromise = a.connectionManager.openConnection(
			b.addressManager.getAddresses()[0]!,
		);

		expect(a.connectionManager.getDialQueue().length).to.equal(1);
		await dialPromise;
		expect(a.connectionManager.getDialQueue().length).to.equal(0);
	});

	it("invokes protocol handler synchronously when negotiateFully=true", async () => {
		const network = new InMemoryNetwork();
		const { runtime: a } = InMemoryNetwork.createPeer({
			index: 0,
			port: 31_010,
			network,
		});
		const { runtime: b } = InMemoryNetwork.createPeer({
			index: 1,
			port: 31_011,
			network,
		});

		a.connectionManager = new InMemoryConnectionManager(network, a);
		b.connectionManager = new InMemoryConnectionManager(network, b);
		network.registerPeer(a, 31_010);
		network.registerPeer(b, 31_011);

		const protocol = "/proto/1.0.0";
		let called = 0;
		await b.registrar.handle(protocol, async () => {
			called += 1;
		});

		const conn = await a.connectionManager.openConnection(
			b.addressManager.getAddresses()[0]!,
		);

		const streamPromise = (conn as any).newStream(protocol, {
			negotiateFully: true,
		});

		expect(called).to.equal(1);
		await streamPromise;
	});

	it("invokes protocol handler asynchronously when negotiateFully=false", async () => {
		const network = new InMemoryNetwork();
		const { runtime: a } = InMemoryNetwork.createPeer({
			index: 0,
			port: 31_020,
			network,
		});
		const { runtime: b } = InMemoryNetwork.createPeer({
			index: 1,
			port: 31_021,
			network,
		});

		a.connectionManager = new InMemoryConnectionManager(network, a);
		b.connectionManager = new InMemoryConnectionManager(network, b);
		network.registerPeer(a, 31_020);
		network.registerPeer(b, 31_021);

		const protocol = "/proto/1.0.0";
		let called = 0;
		await b.registrar.handle(protocol, async () => {
			called += 1;
		});

		const conn = await a.connectionManager.openConnection(
			b.addressManager.getAddresses()[0]!,
		);

		const streamPromise = (conn as any).newStream(protocol, {
			negotiateFully: false,
		});

		expect(called).to.equal(0);
		await delay(0);
		expect(called).to.equal(1);
		await streamPromise;
	});

	it("connection.close() tears down both sides and emits topology disconnects", async () => {
		const network = new InMemoryNetwork();
		const { runtime: a } = InMemoryNetwork.createPeer({
			index: 0,
			port: 31_030,
			network,
		});
		const { runtime: b } = InMemoryNetwork.createPeer({
			index: 1,
			port: 31_031,
			network,
		});

		a.connectionManager = new InMemoryConnectionManager(network, a);
		b.connectionManager = new InMemoryConnectionManager(network, b);
		network.registerPeer(a, 31_030);
		network.registerPeer(b, 31_031);

		const protocol = "/proto/1.0.0";
		await a.registrar.handle(protocol, async () => {});
		await b.registrar.handle(protocol, async () => {});

		let aConnect = 0;
		let aDisconnect = 0;
		let bConnect = 0;
		let bDisconnect = 0;

		await a.registrar.register(protocol, {
			onConnect: async () => {
				aConnect += 1;
			},
			onDisconnect: async () => {
				aDisconnect += 1;
			},
		});

		await b.registrar.register(protocol, {
			onConnect: async () => {
				bConnect += 1;
			},
			onDisconnect: async () => {
				bDisconnect += 1;
			},
		});

		const connA = await a.connectionManager.openConnection(
			b.addressManager.getAddresses()[0]!,
		);

		expect(aConnect).to.equal(1);
		expect(bConnect).to.equal(1);

		await connA.close();

		expect(a.connectionManager.getConnections()).to.have.length(0);
		expect(b.connectionManager.getConnections()).to.have.length(0);

		expect(aDisconnect).to.equal(1);
		expect(bDisconnect).to.equal(1);
		expect(network.metrics.connectionsClosed).to.equal(1);
	});

	it("createPeer produces unique public keys beyond 256 nodes", () => {
		const network = new InMemoryNetwork();
		const n = 600;
		const hashes = new Set<string>();
		for (let i = 0; i < n; i++) {
			const port = 32_000 + i;
			const { runtime } = InMemoryNetwork.createPeer({ index: i, port, network });
			hashes.add(publicKeyHash(runtime.peerId));
		}
		expect(hashes.size).to.equal(n);
	});

	it("peerStore.get returns multiaddrs for known peers", async () => {
		const network = new InMemoryNetwork();
		const { runtime: a } = InMemoryNetwork.createPeer({
			index: 0,
			port: 33_000,
			network,
		});
		const { runtime: b } = InMemoryNetwork.createPeer({
			index: 1,
			port: 33_001,
			network,
		});

		a.connectionManager = new InMemoryConnectionManager(network, a);
		b.connectionManager = new InMemoryConnectionManager(network, b);
		network.registerPeer(a, 33_000);
		network.registerPeer(b, 33_001);

		const peer = await a.peerStore.get(b.peerId);
		expect(peer.addresses?.length).to.be.greaterThan(0);
		const ma = peer.addresses[0]!.multiaddr;
		expect(ma.toString()).to.include("/tcp/33001");
	});
});
