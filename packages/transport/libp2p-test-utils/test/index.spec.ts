import { expect } from "chai";
import { TestSession } from "../src/session.js";
import {
	InMemoryConnectionManager,
	InMemoryNetwork,
} from "../src/inmemory-libp2p.js";

it("connect", async () => {
	const session = await TestSession.connected(3);
	await session.stop();
});

it("removes in-memory streams from both connections when a stream closes", async () => {
	const network = new InMemoryNetwork();
	const { runtime: a } = InMemoryNetwork.createPeer({
		index: 0,
		port: 34_000,
		network,
	});
	const { runtime: b } = InMemoryNetwork.createPeer({
		index: 1,
		port: 34_001,
		network,
	});

	a.connectionManager = new InMemoryConnectionManager(network, a);
	b.connectionManager = new InMemoryConnectionManager(network, b);
	network.registerPeer(a, 34_000);
	network.registerPeer(b, 34_001);

	const protocol = "/proto/1.0.0";
	await b.registrar.handle(protocol, async () => {});

	const connA = await a.connectionManager.openConnection(
		b.addressManager.getAddresses()[0]!,
	);
	const connB = b.connectionManager.getConnections(a.peerId)[0]!;
	const stream = await (connA as any).newStream(protocol, {
		negotiateFully: true,
	});

	expect((connA as any).streams).to.have.length(1);
	expect((connB as any).streams).to.have.length(1);

	await stream.close();

	expect((connA as any).streams).to.have.length(0);
	expect((connB as any).streams).to.have.length(0);

	await stream.close();
});

it("rejects writes when the paired in-memory stream endpoint has closed", async () => {
	const network = new InMemoryNetwork();
	const { runtime: a } = InMemoryNetwork.createPeer({
		index: 0,
		port: 34_010,
		network,
	});
	const { runtime: b } = InMemoryNetwork.createPeer({
		index: 1,
		port: 34_011,
		network,
	});

	a.connectionManager = new InMemoryConnectionManager(network, a);
	b.connectionManager = new InMemoryConnectionManager(network, b);
	network.registerPeer(a, 34_010);
	network.registerPeer(b, 34_011);

	const protocol = "/proto/1.0.0";
	await b.registrar.handle(protocol, async () => {});

	const connA = await a.connectionManager.openConnection(
		b.addressManager.getAddresses()[0]!,
	);

	const wholeFrameStream = await (connA as any).newStream(protocol, {
		negotiateFully: true,
	});
	wholeFrameStream.peer.closeLocal();
	expect(() => wholeFrameStream.send(new Uint8Array([0x01, 0x00]))).to.throw(
		"Remote stream endpoint is closed",
	);
	await wholeFrameStream.close();

	const splitFrameStream = await (connA as any).newStream(protocol, {
		negotiateFully: true,
	});
	expect(splitFrameStream.send(new Uint8Array([1]))).to.equal(true);
	splitFrameStream.peer.closeLocal();
	expect(() => splitFrameStream.send(new Uint8Array([0x01]))).to.throw(
		"Remote stream endpoint is closed",
	);
	await splitFrameStream.close();
});
