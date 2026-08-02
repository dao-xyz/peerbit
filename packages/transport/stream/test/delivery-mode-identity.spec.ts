import { field, serialize, variant, vec } from "@dao-xyz/borsh";
import { TestSession } from "@peerbit/libp2p-test-utils";
import {
	ACK_CONTROL_PRIORITY,
	BACKGROUND_MESSAGE_PRIORITY,
	CONVERGENCE_MESSAGE_PRIORITY,
	DataMessage,
	AcknowledgeAnyWhere as LocalAcknowledgeAnyWhere,
	AcknowledgeDelivery as LocalAcknowledgeDelivery,
	AnyWhere as LocalAnyWhere,
	type DeliveryMode as LocalDeliveryMode,
	SilentDelivery as LocalSilentDelivery,
	TracedDelivery as LocalTracedDelivery,
	MessageHeader,
	appendDeliveryHop,
	deliveryModeHasReceiver,
	getDeliveryHopTrace,
	hasDeliveryHop,
	isAcknowledgeAnyWhereDeliveryMode,
	isAcknowledgeDeliveryMode,
	isAcknowledgedDeliveryMode,
	isAnyWhereDeliveryMode,
	isSilentDeliveryMode,
	isTracedDeliveryMode,
	setDeliveryOriginHop,
} from "@peerbit/stream-interface";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import {
	DirectStream,
	type DirectStreamComponents,
	waitForNeighbour,
} from "../src/index.js";

abstract class ForeignDeliveryMode {}

@variant(0)
class ForeignSilentDelivery extends ForeignDeliveryMode {
	@field({ type: vec("string") })
	to: string[];

	@field({ type: "u8" })
	redundancy: number;

	constructor(to: string[], redundancy: number) {
		super();
		this.to = to;
		this.redundancy = redundancy;
	}
}

@variant(1)
class ForeignAcknowledgeDelivery extends ForeignDeliveryMode {
	@field({ type: vec("string") })
	to: string[];

	@field({ type: "u8" })
	redundancy: number;

	@field({ type: vec("string") })
	hops: string[];

	constructor(to: string[], redundancy: number, hops: string[] = []) {
		super();
		this.to = to;
		this.redundancy = redundancy;
		this.hops = hops;
	}
}

@variant(3)
class ForeignTracedDelivery extends ForeignDeliveryMode {
	@field({ type: vec("string") })
	trace: string[];

	constructor(trace: string[]) {
		super();
		this.trace = trace;
	}
}

@variant(4)
class ForeignAnyWhere extends ForeignDeliveryMode {}

@variant(5)
class ForeignAcknowledgeAnyWhere extends ForeignDeliveryMode {
	@field({ type: "u8" })
	redundancy: number;

	@field({ type: vec("string") })
	hops: string[];

	constructor(redundancy: number, hops: string[] = []) {
		super();
		this.redundancy = redundancy;
		this.hops = hops;
	}
}

@variant(2)
class ForeignUnknownDelivery extends ForeignDeliveryMode {}

abstract class MalformedDeliveryMode {}

@variant(0)
class ForeignSilentWithExtraField extends MalformedDeliveryMode {
	@field({ type: vec("string") })
	to: string[];

	@field({ type: "u8" })
	redundancy: number;

	@field({ type: "u8" })
	extra: number;

	constructor(to: string[], redundancy: number) {
		super();
		this.to = to;
		this.redundancy = redundancy;
		this.extra = 0;
	}
}

@variant(0)
class StandaloneSilentDelivery {
	@field({ type: vec("string") })
	to: string[];

	@field({ type: "u8" })
	redundancy: number;

	constructor(to: string[], redundancy: number) {
		this.to = to;
		this.redundancy = redundancy;
	}
}

abstract class CustomCodecDeliveryMode {}

@variant(0)
class CustomCodecSilentDelivery extends CustomCodecDeliveryMode {
	@field({
		elementType: "string",
		sizeEncoding: "u32",
		serialize: () => {},
		deserialize: (): string[] => [],
	} as any)
	to: string[];

	@field({ type: "u8" })
	redundancy: number;

	constructor(to: string[], redundancy: number) {
		super();
		this.to = to;
		this.redundancy = redundancy;
	}
}

class LocalSilentSubclass extends LocalSilentDelivery {}

const asDeliveryMode = (mode: ForeignDeliveryMode) =>
	mode as unknown as LocalDeliveryMode;

class DeliveryModeIdentityStream extends DirectStream {
	constructor(components: DirectStreamComponents) {
		super(components, ["/peerbit/delivery-mode-identity/1.0.0"], {
			canRelayMessage: true,
			connectionManager: false,
		});
	}
}

describe("delivery mode identity", () => {
	it("classifies delivery modes from another module identity", () => {
		const silent = new ForeignSilentDelivery(["recipient"], 1);
		const acknowledge = new ForeignAcknowledgeDelivery(["recipient"], 2, [
			"origin",
		]);
		const traced = new ForeignTracedDelivery(["origin"]);
		const anywhere = new ForeignAnyWhere();
		const acknowledgeAnyWhere = new ForeignAcknowledgeAnyWhere(2, ["origin"]);

		expect(silent).to.not.be.instanceOf(LocalSilentDelivery);
		expect(acknowledge).to.not.be.instanceOf(LocalAcknowledgeDelivery);
		expect(traced).to.not.be.instanceOf(LocalTracedDelivery);
		expect(anywhere).to.not.be.instanceOf(LocalAnyWhere);
		expect(acknowledgeAnyWhere).to.not.be.instanceOf(LocalAcknowledgeAnyWhere);

		expect(isSilentDeliveryMode(silent)).to.be.true;
		expect(isAcknowledgeDeliveryMode(acknowledge)).to.be.true;
		expect(isTracedDeliveryMode(traced)).to.be.true;
		expect(isAnyWhereDeliveryMode(anywhere)).to.be.true;
		expect(isAcknowledgeAnyWhereDeliveryMode(acknowledgeAnyWhere)).to.be.true;
		expect(deliveryModeHasReceiver(asDeliveryMode(silent))).to.be.true;
		expect(deliveryModeHasReceiver(asDeliveryMode(acknowledge))).to.be.true;
		expect(
			deliveryModeHasReceiver(asDeliveryMode(new ForeignSilentDelivery([], 1))),
		).to.be.false;
	});

	it("preserves acknowledgement helpers and default priorities", () => {
		const acknowledge = asDeliveryMode(
			new ForeignAcknowledgeDelivery(["recipient"], 2, ["first"]),
		);
		const acknowledgeAnyWhere = asDeliveryMode(
			new ForeignAcknowledgeAnyWhere(2, ["first"]),
		);

		for (const mode of [acknowledge, acknowledgeAnyWhere]) {
			expect(isAcknowledgedDeliveryMode(mode)).to.be.true;
			expect(getDeliveryHopTrace(mode)).to.deep.equal(["first"]);
			expect(hasDeliveryHop(mode, "first")).to.be.true;
			appendDeliveryHop(mode, "second");
			expect(getDeliveryHopTrace(mode)).to.deep.equal(["first", "second"]);
			setDeliveryOriginHop(mode, "origin");
			expect(getDeliveryHopTrace(mode)).to.deep.equal(["origin"]);
		}

		const priorities: ReadonlyArray<
			readonly [LocalDeliveryMode, number, number | undefined]
		> = [
			[
				asDeliveryMode(new ForeignSilentDelivery(["recipient"], 1)),
				BACKGROUND_MESSAGE_PRIORITY,
				undefined,
			],
			[
				asDeliveryMode(new ForeignAnyWhere()),
				BACKGROUND_MESSAGE_PRIORITY,
				undefined,
			],
			[
				asDeliveryMode(new ForeignAcknowledgeDelivery(["recipient"], 1)),
				CONVERGENCE_MESSAGE_PRIORITY,
				ACK_CONTROL_PRIORITY,
			],
			[
				asDeliveryMode(new ForeignAcknowledgeAnyWhere(1)),
				CONVERGENCE_MESSAGE_PRIORITY,
				ACK_CONTROL_PRIORITY,
			],
			[
				asDeliveryMode(new ForeignTracedDelivery([])),
				ACK_CONTROL_PRIORITY,
				undefined,
			],
		];

		for (const [mode, priority, responsePriority] of priorities) {
			const header = new MessageHeader({ mode, session: 1 });
			expect(header.priority).to.equal(priority);
			expect(header.responsePriority).to.equal(responsePriority);
		}
	});

	it("fails closed for unknown schemas and malformed values", () => {
		const lookalike = { to: ["recipient"], redundancy: 1 };
		const spoofedConstructor = {
			constructor: ForeignSilentDelivery,
			to: ["recipient"],
			redundancy: 1,
		};
		const unknown = new ForeignUnknownDelivery();
		const extraField = new ForeignSilentWithExtraField(["recipient"], 1);
		const standalone = new StandaloneSilentDelivery(["recipient"], 1);
		const customCodec = new CustomCodecSilentDelivery(["recipient"], 1);
		const localSubclass = new LocalSilentSubclass({
			to: ["recipient"],
			redundancy: 1,
		});
		const hostileProxy = new Proxy(
			{},
			{
				getPrototypeOf: () => {
					throw new Error("hostile prototype");
				},
			},
		);
		const malformedTo = new ForeignSilentDelivery(["recipient"], 1);
		(malformedTo as unknown as { to: unknown }).to = [1];
		const sparseTo = new ForeignSilentDelivery(new Array<string>(1), 1);
		const malformedRedundancy = new ForeignAcknowledgeDelivery(
			["recipient"],
			256,
		);
		const values = [
			undefined,
			null,
			lookalike,
			spoofedConstructor,
			unknown,
			extraField,
			standalone,
			customCodec,
			localSubclass,
			hostileProxy,
			malformedTo,
			sparseTo,
			malformedRedundancy,
		];

		for (const value of values) {
			expect(isSilentDeliveryMode(value)).to.be.false;
			expect(isAcknowledgeDeliveryMode(value)).to.be.false;
			expect(isTracedDeliveryMode(value)).to.be.false;
			expect(isAnyWhereDeliveryMode(value)).to.be.false;
			expect(isAcknowledgeAnyWhereDeliveryMode(value)).to.be.false;
		}
	});

	it("preserves the canonical wire representation", () => {
		const local = new LocalSilentDelivery({
			to: ["recipient"],
			redundancy: 1,
		});
		const foreign = new ForeignSilentDelivery(["recipient"], 1);
		expect(serialize(foreign)).to.deep.equal(serialize(local));
	});

	it("does not acknowledge an empty targeted mode", async () => {
		const session = await TestSession.connected<{
			directstream: DeliveryModeIdentityStream;
		}>(1, {
			services: {
				directstream: (components) =>
					new DeliveryModeIdentityStream(components),
			},
		});
		try {
			const stream = session.peers[0]!.services.directstream;
			const publishAck = sinon.stub(stream as any, "publishMessageMaybe");
			await stream.maybeAcknowledgeMessage(
				{} as any,
				new DataMessage({
					header: new MessageHeader({
						mode: asDeliveryMode(new ForeignAcknowledgeDelivery([], 1)),
						session: 1,
					}),
				}),
				0,
			);
			expect(publishAck.called).to.equal(false);
		} finally {
			await session.stop();
		}
	});

	it("routes silent and acknowledged modes from another identity", async () => {
		const session = await TestSession.connected<{
			directstream: DeliveryModeIdentityStream;
		}>(2, {
			services: {
				directstream: (components) =>
					new DeliveryModeIdentityStream(components),
			},
		});
		try {
			const sender = session.peers[0]!.services.directstream;
			const receiver = session.peers[1]!.services.directstream;
			await waitForNeighbour(sender, receiver);
			const received: number[] = [];
			receiver.addEventListener("data", (event) => {
				received.push(event.detail.data![0]!);
			});

			await sender.publish(new Uint8Array([1]), {
				mode: asDeliveryMode(
					new ForeignSilentDelivery([receiver.publicKeyHash], 1),
				),
			});
			await waitForResolved(() => expect(received).to.deep.equal([1]));

			await sender.publish(new Uint8Array([2]), {
				mode: asDeliveryMode(
					new ForeignAcknowledgeDelivery([receiver.publicKeyHash], 1),
				),
			});
			await waitForResolved(() => expect(received).to.deep.equal([1, 2]));
		} finally {
			await session.stop();
		}
	});
});
