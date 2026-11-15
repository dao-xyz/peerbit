import type { Connection, PeerId } from "@libp2p/interface";
import { delay } from "@peerbit/time";
import { expect } from "chai";
import {
	InMemoryConnectionManager,
	InMemoryNetwork,
} from "../src/inmemory-libp2p.js";

const protocol = "/inmemory-abort/1.0.0";

type TopologyHooks = {
	onConnect?: (peerId: PeerId, connection: Connection) => void;
};

const setup = async (
	basePort: number,
	hooks: { a?: TopologyHooks; b?: TopologyHooks } = {},
) => {
	const network = new InMemoryNetwork();
	const { runtime: a } = InMemoryNetwork.createPeer({
		index: 0,
		port: basePort,
		network,
	});
	const { runtime: b } = InMemoryNetwork.createPeer({
		index: 1,
		port: basePort + 1,
		network,
	});
	a.connectionManager = new InMemoryConnectionManager(network, a);
	b.connectionManager = new InMemoryConnectionManager(network, b);
	network.registerPeer(a, basePort);
	network.registerPeer(b, basePort + 1);

	const inbound: any[] = [];
	await a.registrar.handle(protocol, async () => {});
	await b.registrar.handle(protocol, async (stream) => {
		inbound.push(stream);
	});

	// Ordered topology notifications, tagged with the connection status seen.
	const events: string[] = [];
	for (const [name, runtime] of [
		["a", a],
		["b", b],
	] as const) {
		await runtime.registrar.register(protocol, {
			onConnect: (peerId, connection) => {
				events.push(`${name}:connect:${connection.status}`);
				hooks[name]?.onConnect?.(peerId, connection);
			},
			onDisconnect: (_peerId, connection) => {
				events.push(`${name}:disconnect:${connection.status}`);
			},
		});
	}

	const dial = () =>
		a.connectionManager.openConnection(b.addressManager.getAddresses()[0]!);

	return { network, a, b, inbound, events, dial };
};

describe("in-memory connection abort", () => {
	it("tears down both ends and their streams before returning", async () => {
		const { network, a, b, inbound, events, dial } = await setup(35_000);
		const connA = await dial();
		const connB = b.connectionManager.getConnections(a.peerId)[0]!;
		const outbound = await connA.newStream(protocol, { negotiateFully: true });
		expect(inbound).to.have.length(1);

		const closed: string[] = [];
		outbound.addEventListener("close", () => closed.push("outbound"));
		inbound[0].addEventListener("close", () => closed.push("inbound"));

		const result: unknown = connA.abort(new Error("abort test"));
		expect(result).to.equal(undefined);

		// Everything below holds synchronously, before any await.
		expect(connA.status).to.equal("aborted");
		expect(connB.status).to.equal("reset");
		expect(connA.timeline.close).to.be.a("number");
		expect(connB.timeline.close).to.be.a("number");
		expect(closed).to.have.members(["outbound", "inbound"]);
		expect(connA.streams).to.have.length(0);
		expect(connB.streams).to.have.length(0);
		expect(() => outbound.send(new Uint8Array([1, 0]))).to.throw(
			"closed stream",
		);
		expect(a.connectionManager.getConnections()).to.have.length(0);
		expect(b.connectionManager.getConnections()).to.have.length(0);
		expect(network.metrics.connectionsClosed).to.equal(1);

		// Topologies hear the disconnect asynchronously, as they do after close().
		expect(events).to.deep.equal(["a:connect:open", "b:connect:open"]);
		await delay(0);
		expect(events).to.deep.equal([
			"a:connect:open",
			"b:connect:open",
			"a:disconnect:aborted",
			"b:disconnect:reset",
		]);

		let newStreamError: Error | undefined;
		await connA.newStream(protocol).catch((error) => {
			newStreamError = error;
		});
		expect(newStreamError?.message).to.equal("Connection is not open");
	});

	it("is idempotent across abort and close on either end", async () => {
		const { network, a, b, events, dial } = await setup(35_010);
		const connA = await dial();
		const connB = b.connectionManager.getConnections(a.peerId)[0]!;
		const stream = await connA.newStream(protocol, { negotiateFully: true });
		// Stream close listeners run inside abort() and may re-enter it.
		let reentered = 0;
		stream.addEventListener("close", () => {
			reentered += 1;
			connB.abort(new Error("re-entrant"));
			void connA.close();
		});

		connA.abort(new Error("first"));
		expect(reentered).to.equal(1);
		connA.abort(new Error("second"));
		connB.abort(new Error("remote"));
		await connA.close();
		await connB.close();
		await delay(0);

		expect(connA.status).to.equal("aborted");
		expect(connB.status).to.equal("reset");
		expect(network.metrics.connectionsClosed).to.equal(1);
		expect(events).to.deep.equal([
			"a:connect:open",
			"b:connect:open",
			"a:disconnect:aborted",
			"b:disconnect:reset",
		]);
	});

	it("is a no-op once close() has begun", async () => {
		const { network, a, b, events, dial } = await setup(35_020);
		const connA = await dial();
		const connB = b.connectionManager.getConnections(a.peerId)[0]!;

		// close() shuts both ends synchronously; only the bookkeeping and the
		// topology notifications are still pending, so nothing is left to abort.
		const closing = connA.close();
		connA.abort(new Error("late"));
		await closing;

		expect(connA.status).to.equal("closed");
		expect(connB.status).to.equal("closed");
		expect(network.metrics.connectionsClosed).to.equal(1);
		expect(events).to.deep.equal([
			"a:connect:open",
			"b:connect:open",
			"a:disconnect:closed",
			"b:disconnect:closed",
		]);
	});

	it("lets a topology abort a connection while it is being announced", async () => {
		// Mirrors DirectStream rejecting a recently pruned peer from onConnect.
		let prune = true;
		const { network, a, b, events, dial } = await setup(35_030, {
			a: {
				onConnect: (_peerId, connection) => {
					if (!prune) return;
					prune = false;
					connection.abort(new Error("Connection was pruned"));
				},
			},
		});

		const pruned = await dial();
		expect(pruned.status).to.equal("aborted");
		expect(a.connectionManager.getConnections()).to.have.length(0);
		expect(b.connectionManager.getConnections()).to.have.length(0);
		await delay(0);
		// Each side hears connect before disconnect, exactly once.
		expect(events).to.deep.equal([
			"a:connect:open",
			"b:connect:reset",
			"a:disconnect:aborted",
			"b:disconnect:reset",
		]);
		expect(network.metrics.connectionsOpened).to.equal(1);
		expect(network.metrics.connectionsClosed).to.equal(1);

		// A later dial gets a fresh connection instead of the aborted one.
		const fresh = await dial();
		expect(fresh).to.not.equal(pruned);
		expect(fresh.status).to.equal("open");
		expect(b.connectionManager.getConnections(a.peerId)).to.have.length(1);
		await fresh.close();
	});
});
