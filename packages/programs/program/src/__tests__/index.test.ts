import {
	field,
	variant,
	vec,
	option,
	deserialize,
	serialize,
} from "@dao-xyz/borsh";
import { AbstractProgram, ComposableProgram, Program, ProgramClient } from "..";
import { getValuesWithType } from "../utils.js";
import {
	Ed25519Keypair,
	PublicSignKey,
	randomBytes,
	sha256Base64Sync,
} from "@peerbit/crypto";
import {
	Subscription,
	SubscriptionEvent,
	UnsubcriptionEvent,
	Unsubscription,
} from "@peerbit/pubsub-interface";
import { CustomEvent } from "@libp2p/interfaces/events";

@variant(0)
class Log {}

@variant("x1")
class P1 extends ComposableProgram {
	@field({ type: Uint8Array })
	id: Uint8Array;

	constructor() {
		super();
		this.id = randomBytes(32);
	}

	async open(): Promise<void> {}
}

class EmbeddedStore {
	@field({ type: Log })
	log: Log;

	constructor(properties?: { log: Log }) {
		if (properties) {
			this.log = properties.log;
		}
	}
}
class ExtendedEmbeddedStore extends EmbeddedStore {
	constructor(properties?: { log: Log }) {
		super(properties);
	}
}
@variant("p2")
class P2 extends Program {
	@field({ type: option(vec(ExtendedEmbeddedStore)) })
	log?: ExtendedEmbeddedStore[];

	@field({ type: P1 })
	program: P1;

	constructor(log: Log) {
		super();
		this.log = [new ExtendedEmbeddedStore({ log: log })];
		this.program = new P1();
	}

	async open(): Promise<void> {
		await this.program.open();
	}
}

@variant("p3")
class P3 extends Program {
	@field({ type: Uint8Array })
	id: Uint8Array;

	constructor() {
		super();
		this.id = randomBytes(32);
	}
	static TOPIC = "abc";

	async open(): Promise<void> {
		this.node.services.pubsub.subscribe(P3.TOPIC);
	}

	async close(from?: AbstractProgram | undefined): Promise<boolean> {
		this.node.services.pubsub.unsubscribe(P3.TOPIC);
		return super.close(from);
	}

	async setup(): Promise<void> {}

	getTopics(): string[] {
		return [P3.TOPIC];
	}
}

@variant("p4")
class P4 extends Program {
	@field({ type: P2 })
	program: P2;

	constructor() {
		super();
		this.program = new P2(new Log());
	}

	async open(): Promise<void> {
		await this.program.open();
	}
}

describe("getValuesWithType", () => {
	it("can stop at type", () => {
		const log = new Log();
		const p = new P2(log);

		let stores = getValuesWithType(p, Log, EmbeddedStore);
		expect(stores).toEqual([]);
		stores = getValuesWithType(p, Log);
		expect(stores).toEqual([log]);
	});
});

const createPeer = async (
	state: {
		subsribers: Map<
			string,
			Map<
				string,
				{
					timestamp: bigint;
					data?: Uint8Array | undefined;
				}
			>
		>;
		pubsubEventHandlers: Map<string, { fn: any; publicKey: PublicSignKey }[]>;
		peers: Map<string, ProgramClient>;
	} = {
		pubsubEventHandlers: new Map(),
		subsribers: new Map(),
		peers: new Map(),
	}
): Promise<ProgramClient> => {
	const keypair = await Ed25519Keypair.create();
	let blocks: Map<string, Uint8Array> = new Map();

	const dispatchEvent = (e: CustomEvent<any>, emitSelf: boolean = false) => {
		const handlers = state.pubsubEventHandlers.get(e.type);
		if (handlers) {
			handlers.forEach(({ fn, publicKey }) => {
				if (!publicKey.equals(keypair.publicKey) || emitSelf) {
					fn(e);
				}
			});
			return true;
		}
		return false;
	};
	const peer: ProgramClient = {
		peerId: await keypair.toPeerId(),
		identity: keypair,
		getMultiaddrs: () => [],
		dial: () => Promise.resolve(false),
		services: {
			blocks: {
				get: (c) => blocks.get(c),
				has: (c) => blocks.has(c),
				put: (c) => {
					const hash = sha256Base64Sync(c);
					blocks.set(hash, c);
					return hash;
				},
				rm: (c) => {
					blocks.delete(c);
				},
				waitFor: () => Promise.resolve(),
			},
			pubsub: {
				subscribe: async (topic, opts) => {
					let map = state.subsribers.get(topic);
					if (!map) {
						map = new Map();
						state.subsribers.set(topic, map);
					}
					map.set(keypair.publicKey.hashcode(), {
						timestamp: BigInt(+new Date()),
						data: opts?.data,
					});
					dispatchEvent(
						new CustomEvent<SubscriptionEvent>("subscribe", {
							detail: {
								from: keypair.publicKey,
								subscriptions: [new Subscription(topic, opts?.data)],
							},
						})
					);
				},
				getSubscribers: (topic) => {
					return state.subsribers.get(topic);
				},

				unsubscribe: async (topic) => {
					let map = state.subsribers.get(topic);
					if (!map) {
						return false;
					}
					const ret = map.delete(keypair.publicKey.hashcode());
					if (ret) {
						dispatchEvent(
							new CustomEvent<UnsubcriptionEvent>("unsubscribe", {
								detail: {
									from: keypair.publicKey,
									unsubscriptions: [new Unsubscription(topic)],
								},
							})
						);
					}
					return ret;
				},

				publish: (d, o) => Promise.resolve(randomBytes(32)),

				addEventListener: (type, fn) => {
					const arr = state.pubsubEventHandlers.get(type) || [];
					arr.push({ fn, publicKey: keypair.publicKey });
					state.pubsubEventHandlers.set(type, arr);
				},

				removeEventListener: (type, e) => {
					let fns = state.pubsubEventHandlers.get(type);
					const idx = fns?.findIndex((x) => x.fn == e);
					if (idx == null || idx < 0) {
						throw new Error("Missing handler");
					}
					fns?.splice(idx, 1);
				},
				dispatchEvent,

				requestSubscribers: async () => {
					for (const [topic, data] of state.subsribers) {
						for (const [hash, opts] of data) {
							if (hash !== keypair.publicKey.hashcode()) {
								dispatchEvent(
									new CustomEvent<SubscriptionEvent>("subscribe", {
										detail: {
											from: state.peers.get(hash)?.identity.publicKey!,
											subscriptions: [new Subscription(topic, opts?.data)],
										},
									}),
									true
								);
							}
						}
					}
				},
				waitFor: () => Promise.resolve(),
			},
		},
		memory: undefined,
		keychain: undefined,
		start: () => Promise.resolve(),
		stop: () => Promise.resolve(),
		open: async (p, o) => {
			if (typeof p === "string") {
				throw new Error("Unsupported");
			}
			await p.beforeOpen(peer, o);
			await p.open(o?.args);
			await p.afterOpen();
			return p;
		},
	};
	state.peers.set(peer.identity.publicKey.hashcode(), peer);
	return peer;
};

describe("program", () => {
	describe("lifecycle", () => {
		let peer: ProgramClient;
		beforeEach(async () => {
			peer = await createPeer();
		});
		afterEach(async () => {
			await peer.stop();
		});

		describe("open/close", () => {
			it("with args", async () => {
				@variant("p5")
				class P5 extends Program<{ number: number }> {
					constructor() {
						super();
					}

					number: number | undefined;
					async open(args?: { number: number } | undefined): Promise<void> {
						this.number = args?.number;
					}
				}

				const p5 = await peer.open(new P5(), { args: { number: 123 } });
				expect(p5.number).toEqual(123);
			});

			it("fails to init without schema", async () => {
				class NoVariant extends Program {
					constructor() {
						super();
					}
					async open(): Promise<void> {}
				}
				await expect(peer.open(new NoVariant())).rejects.toThrowError(
					'Expecting class to be decorated with a string variant. Example:\n\'import { variant } "@dao-xyz/borsh"\n@variant("example-db")\nclass NoVariant { ...'
				);
			});

			it("fails to init without variant", async () => {
				class NoVariant extends Program {
					@field({ type: "u8" })
					number: number;

					constructor() {
						super();
						this.number = 123;
					}
					async open(): Promise<void> {}
				}

				await expect(peer.open(new NoVariant())).rejects.toThrowError(
					'Expecting class to be decorated with a string variant. Example:\n\'import { variant } "@dao-xyz/borsh"\n@variant("example-db")\nclass NoVariant { ...'
				);

				// Remove NoVariant for globals to prevent sideeffects
				const idx = Program.prototype[1001].findIndex((x) => x == NoVariant);
				(Program.prototype[1001] as Array<() => void>).splice(idx, 1);
			});

			it("static", async () => {
				const p2 = new P2(new Log());
				const cid = await p2.save(peer.services.blocks);
				const p2Loaded = await P2.open(cid, peer);
				expect(p2Loaded).toBeInstanceOf(P2);
			});

			it("can re-open from dropped", async () => {
				const p = new P3();
				await peer.open(p);
				expect(p.closed).toBeFalse();
				await p.drop();
				expect(p.closed).toBeTrue();
				await peer.open(p);
				expect(p.closed).toBeFalse();
			});
			it("can re-open from closed", async () => {
				const p = new P3();
				await peer.open(p);

				expect(p.closed).toBeFalse();
				await p.close();
				expect(p.closed).toBeTrue();
				await peer.open(p);
				expect(p.closed).toBeFalse();
			});

			it("can resolve programs", () => {
				const log = new Log();
				const p = new P2(log);

				// programs
				const programs = p.programs;
				expect(programs).toHaveLength(1);
				expect(programs[0]).toEqual(p.program);
			});

			it("open/closes children", async () => {
				let p = new P4();
				await peer.open(p);
				expect(p.closed).toBeFalse();
				expect(p.program.closed).toBeFalse();
				await p.close();
				expect(p.closed).toBeTrue();
				expect(p.program.closed).toBeTrue();
			});
		});

		describe("clear", () => {
			it("clears stores and programs on clear", async () => {
				const log = new Log();
				const p = new P2(log);
				p.allPrograms;
				expect(p["_allPrograms"]).toBeDefined();
				await p["_clear"]();
				expect(p["_allPrograms"]).toBeUndefined();
			});

			it("invokes clear on close", async () => {
				const log = new Log();
				const p = new P2(log);

				await peer.open(p);

				expect(p.closed).toBeFalse();
				let cleared = false;
				p["_clear"] = () => {
					cleared = true;
				};
				await p.close();
				expect(cleared).toBeTrue();
			});

			it("does not invoke clear on close non-initialized", async () => {
				const log = new Log();
				const p = new P2(log);
				expect(p.closed).toBeTrue();
				let cleared = false;
				p["_clear"] = () => {
					cleared = true;
				};
				await p.close();
				expect(cleared).toBeFalse();
			});

			it("invokes clear on drop", async () => {
				const log = new Log();
				const p = new P2(log);

				await peer.open(p);
				expect(p.closed).toBeFalse();
				let cleared = false;
				p["_clear"] = () => {
					cleared = true;
				};
				await p.drop();
				expect(p.closed).toBeTrue();
			});

			it("invokes clear on drop non-initialized", async () => {
				const log = new Log();
				const p = new P2(log);
				let cleared = false;
				p["_clear"] = () => {
					cleared = true;
				};
				let deleted = false;
				p.delete = async () => {
					deleted = true;
				};
				await p.drop();
				expect(cleared).toBeTrue();
				expect(deleted).toBeTrue();
			});
		});

		describe("subprogram", () => {
			it("subprogram will not close if opened outside a program", async () => {
				const p = new P3();
				const p2 = new P3();

				await peer.open(p);
				await peer.open(p2);

				let p3 = await peer.open(new P3(), { parent: p });
				let closeCounter = 0;

				// Open it outside a program (call init on p3)
				await peer.open(p3);

				expect(p.children).toHaveLength(1);
				expect(p3.children).toBeUndefined();
				expect(p3.parents).toContainAllValues([undefined, p]);

				await peer.open(p3, { parent: p2 });
				expect(p.children).toHaveLength(1);
				expect(p2.children).toHaveLength(1);
				expect(p3.children).toBeUndefined();
				expect(p3.parents).toContainAllValues([undefined, p, p2]);

				await p2.close();
				expect(p3.parents).toContainAllValues([undefined, p]);
				expect(closeCounter).toEqual(0);
				expect(p3.closed).toBeFalse();

				await p.close();
				expect(p3.parents).toContainAllValues([undefined]);
				expect(closeCounter).toEqual(0);
				expect(p3.closed).toBeFalse();
			});

			it("subprogram will close if no dependency", async () => {
				const p = new P3();
				const p2 = new P3();

				const closeEvents1: Map<string, number> = new Map();

				p.events.addEventListener("close", (p) => {
					closeEvents1.set(
						p.detail.address,
						(closeEvents1.get(p.detail.address) || 0) + 1
					);
				});

				const closeEvents2: Map<string, number> = new Map();

				p2.events.addEventListener("close", (p) => {
					closeEvents2.set(
						p.detail.address,
						(closeEvents2.get(p.detail.address) || 0) + 1
					);
				});

				await peer.open(p);

				await peer.open(p2);

				let p3 = await peer.open(new P3(), { parent: p });
				expect(p3.parents).toEqual([p]);

				const closeEvents3: Map<string, number> = new Map();
				p3.events.addEventListener("close", (p) => {
					closeEvents3.set(
						p.detail.address,
						(closeEvents3.get(p.detail.address) || 0) + 1
					);
				});

				expect(p.children).toHaveLength(1);
				expect(p3.children).toBeUndefined();
				expect(p3.parents).toContainAllValues([p]);
				expect(p3.closed).toBeFalse();

				await peer.open(p3, { parent: p2 });
				expect(p.children).toHaveLength(1);
				expect(p2.children).toHaveLength(1);
				expect(p3.children).toBeUndefined();
				expect(p3.parents).toContainAllValues([p, p2]);
				expect(p3.closed).toBeFalse();

				await p2.close();
				expect(p3.parents).toContainAllValues([p]);
				expect(p3.closed).toBeFalse();
				expect(closeEvents1.size).toEqual(0);
				expect([...closeEvents2.keys()]).toEqual([p2.address]);
				expect(closeEvents3.size).toEqual(0);

				await p.close();
				expect(p3.parents).toContainAllValues([]);
				expect(closeEvents3.size).toEqual(1);
				expect(p3.closed).toBeTrue();
				expect([...closeEvents1.keys()]).toEqual([p.address, p3.address]);
				expect([...closeEvents2.keys()]).toEqual([p2.address]);
				expect([...closeEvents3.keys()]).toEqual([p3.address]);
			});

			it("can drop", async () => {
				let p = new P4();
				await peer.open(p);
				await p.drop();
			});
		});
	});

	describe("events", () => {
		it("join/leave", async () => {
			const eventHandlers = new Map();
			const subscriptions = new Map();
			const peers = new Map();
			const peer = await createPeer({
				subsribers: subscriptions,
				pubsubEventHandlers: eventHandlers,
				peers,
			});
			const peer2 = await createPeer({
				subsribers: subscriptions,
				pubsubEventHandlers: eventHandlers,
				peers,
			});

			const p = new P3();
			const joinEvents: string[] = [];
			const leaveEvents: string[] = [];

			p.events.addEventListener("join", (e) => {
				joinEvents.push(e.detail.hashcode());
			});
			p.events.addEventListener("leave", (e) => {
				leaveEvents.push(e.detail.hashcode());
			});

			await peer.open(p);

			const joinEvents2: string[] = [];
			const leaveEvents2: string[] = [];

			const p2 = deserialize(serialize(p), P3);
			p2.events.addEventListener("join", (e) => {
				joinEvents2.push(e.detail.hashcode());
			});

			p2.events.addEventListener("leave", (e) => {
				leaveEvents2.push(e.detail.hashcode());
			});

			await peer2.open(p2);

			expect(joinEvents).toEqual([peer2.identity.publicKey.hashcode()]);
			expect(joinEvents2).toEqual([]);

			await peer2.services.pubsub.requestSubscribers(p.getTopics()[0]);

			expect(joinEvents2).toEqual([peer.identity.publicKey.hashcode()]);

			expect(leaveEvents).toHaveLength(0);
			expect(leaveEvents2).toHaveLength(0);

			await p2.close();

			expect(leaveEvents).toEqual([peer2.identity.publicKey.hashcode()]);
			expect(leaveEvents2).toHaveLength(0);
		});
	});
});
