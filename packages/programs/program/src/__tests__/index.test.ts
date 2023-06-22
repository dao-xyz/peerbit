import { field, variant, vec, option } from "@dao-xyz/borsh";
import { ComposableProgram, Program } from "..";
import { getValuesWithType } from "../utils.js";
import { Peerbit } from "@peerbit/interface";
import { Ed25519Keypair, sha256Base64Sync } from "@peerbit/crypto";

@variant(0)
class Log {}

@variant("x1")
class P1 extends ComposableProgram {
	constructor() {
		super();
	}
	async setup(): Promise<void> {}
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

	setupCalls = 0;
	async setup(): Promise<void> {
		this.setupCalls = (this.setupCalls || 0) + 1;
	}
}

@variant("p3")
class P3 extends Program {
	constructor() {
		super();
	}

	async setup(): Promise<void> {}
}

@variant("p4")
class P4 extends Program {
	@field({ type: P2 })
	program: P2;

	constructor() {
		super();
		this.program = new P2(new Log());
	}

	async setup(): Promise<void> {}
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
describe("program", () => {
	const createPeer = async (): Promise<Peerbit> => {
		const keypair = await Ed25519Keypair.create();
		let blocks: Map<string, Uint8Array> = new Map();
		return {
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
					subscribe: (topic) => Promise.resolve(),
					getSubscribers: () => new Map(),
					unsubscribe: (topic) => Promise.resolve(true),
					publish: (d, o) => Promise.resolve(),
					addEventListener: (e) => {},
					removeEventListener: (e) => {},
					dispatchEvent: (e) => true,
					requestSubscribers: () => Promise.resolve(),
					waitFor: () => Promise.resolve(),
				},
			},
			memory: undefined,
			keychain: undefined,
			stop: () => Promise.resolve(),
		};
	};
	let peer: Peerbit;
	beforeEach(async () => {
		peer = await createPeer();
	});
	afterEach(async () => {
		await peer.stop();
	});
	it("can re-open from closed", async () => {
		const p = new P3();
		let open = async (open: Program): Promise<Program> => {
			return open;
		};
		await p.open(peer, {
			open,
			log: {},
		} as any);

		expect(p.closed).toBeFalse();
		await p.close();
		expect(p.closed).toBeTrue();
		await p.open(peer, {
			open,
			log: {},
		} as any);
		expect(p.closed).toBeFalse();
	});

	it("can re-open from dropped", async () => {
		const p = new P3();
		let open = async (open: Program): Promise<Program> => {
			return open;
		};
		await p.open(peer, {
			open,
			log: {},
		} as any);

		expect(p.closed).toBeFalse();
		await p.drop();
		expect(p.closed).toBeTrue();
		await p.open(peer, {
			open,
			log: {},
		} as any);
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

	describe("open", () => {
		it("fails to init without schema", async () => {
			class NoVariant extends Program {
				constructor() {
					super();
				}
				async setup(): Promise<void> {}
			}
			await expect(new NoVariant().open(peer, {} as any)).rejects.toThrowError(
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
				async setup(): Promise<void> {}
			}

			await expect(new NoVariant().open(peer, {} as any)).rejects.toThrowError(
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
	});

	describe("setup", () => {
		@variant("p5")
		class P5 extends Program {
			@field({ type: P2 })
			p2: P2;

			constructor() {
				super();
				this.p2 = new P2(new Log());
			}

			async setup(): Promise<void> {
				return this.p2.setup(); // unnecessary call
			}
		}

		@variant("p6")
		class P6 extends Program {
			@field({ type: P2 })
			p2: P2;

			number: number;
			constructor() {
				super();
				this.p2 = new P2(new Log());
			}

			async setup(number?: number): Promise<void> {
				this.number = number || -1;
				return this.p2.setup(); // unnecessary call
			}
		}

		it("setup on open", async () => {
			const log = new Log();
			const p = new P2(log);
			await p.open(peer);
			expect(p.setupCalls).toEqual(1);
		});

		it("will not invoke setup automatically twice", async () => {
			const p5 = new P5();
			await p5.open(peer);
			expect(p5.p2.setupCalls).toEqual(1);
		});

		it("can setup twice manually", async () => {
			const p = new P6();
			await p.setup(1);
			expect(p.number).toEqual(1);
			await p.setup(2);
			expect(p.number).toEqual(2);
			await p.open(peer);
			expect(p.number).toEqual(2);
		});

		it("setup counter is setup after load", async () => {
			const p = new P6();
			expect(p["__SETUP_CALLS"]).toBeDefined();
			expect(p["__SETUP_INNER_FUNCTION"]).toBeDefined();

			const cid = await p.save(peer.services.blocks);
			const pLoaded = await P6.open(cid, peer);
			expect(pLoaded).toBeInstanceOf(P6);
			expect(pLoaded["__SETUP_CALLS"]).toBeDefined();
			expect(pLoaded["__SETUP_INNER_FUNCTION"]).toBeDefined();
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

			let open = async (open: Program): Promise<Program> => {
				return open;
			};
			await p.open(peer, {
				open,
				log: {},
			} as any);
			expect(p.closed).toBeFalse();
			let cleared = false;
			p["_clear"] = () => {
				cleared = true;
			};
			await p.close();
			expect(cleared).toBeTrue();
		});

		it("invokes clear on close non-initialized", async () => {
			const log = new Log();
			const p = new P2(log);
			expect(p.closed).toBeTrue();
			let cleared = false;
			p["_clear"] = () => {
				cleared = true;
			};
			await p.close();
			expect(cleared).toBeTrue();
		});

		it("invokes clear on drop", async () => {
			const log = new Log();
			const p = new P2(log);

			let open = async (open: Program): Promise<Program> => {
				return open;
			};
			await p.open(peer, {
				open,
				log: {},
			} as any);
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

			let open = async (open: Program): Promise<Program> => {
				return open;
			};
			await p.open(peer, {
				open,
				log: {},
			} as any);
			await p2.open(peer, {
				open,
				log: {},
			} as any);

			let p3 = await p.subOpen!(new P3());
			let closeCounter = 0;

			// Open it outside a program (call init on p3)
			await p3.open(peer, {
				onClose: () => {
					closeCounter++;
				},
				log: {},
			} as any);

			expect(p.programsOpened).toHaveLength(1);
			expect(p3.programsOpened).toBeUndefined();
			expect(p3.openedByPrograms).toContainAllValues([undefined, p]);

			await p2.subOpen!(p3);
			expect(p.programsOpened).toHaveLength(1);
			expect(p2.programsOpened).toHaveLength(1);
			expect(p3.programsOpened).toBeUndefined();
			expect(p3.openedByPrograms).toContainAllValues([undefined, p, p2]);

			await p2.close();
			expect(p3.openedByPrograms).toContainAllValues([undefined, p]);
			expect(closeCounter).toEqual(0);
			expect(p3.closed).toBeFalse();
			await p.close();
			expect(p3.openedByPrograms).toContainAllValues([undefined]);
			expect(closeCounter).toEqual(0);
			expect(p3.closed).toBeFalse();
		});

		it("subprogram will close if no dependency", async () => {
			const p = new P3();
			const p2 = new P3();

			let closeCounter = 0;

			let open = async (open: Program): Promise<Program> => {
				open["_onClose"] = () => {
					closeCounter += 1;
				};
				open["_closed"] = false;
				open["_initialized"] = true;
				return open;
			};

			await p.open(peer, {
				open,
				log: {},
			} as any);
			await p2.open(peer, {
				open,
				log: {},
			} as any);

			let p3 = await p.subOpen!(new P3());

			expect(p.programsOpened).toHaveLength(1);
			expect(p3.programsOpened).toBeUndefined();
			expect(p3.openedByPrograms).toContainAllValues([p]);
			expect(p3.closed).toBeFalse();

			await p2.subOpen!(p3);
			expect(p.programsOpened).toHaveLength(1);
			expect(p2.programsOpened).toHaveLength(1);
			expect(p3.programsOpened).toBeUndefined();
			expect(p3.openedByPrograms).toContainAllValues([p, p2]);
			expect(p3.closed).toBeFalse();

			await p2.close();
			expect(p3.openedByPrograms).toContainAllValues([p]);
			expect(closeCounter).toEqual(0);
			expect(p3.closed).toBeFalse();

			await p.close();
			expect(p3.openedByPrograms).toContainAllValues([]);
			expect(closeCounter).toEqual(1);
			expect(p3.closed).toBeTrue();
		});

		/* it("will create indices", async () => {
			@variant("pa")
			class ProgramA extends ComposableProgram {
				@field({ type: Log })
				logA: Log = new Log();
			}
	
			@variant("pb")
			class ProgramB extends ComposableProgram {
				@field({ type: Log })
				logB: Log = new Log();
	
				@field({ type: ProgramA })
				programA = new ProgramA();
			}
	
			@variant("pc")
			class ProgramC extends Program {
				@field({ type: ProgramA })
				programA = new ProgramA();
	
				@field({ type: ProgramB })
				programB = new ProgramB();
	
				@field({ type: Log })
				logC = new Log();
	
				async setup(): Promise<void> { }
			}
	
			const pr = new ProgramC();
			const mem = await new MemoryLevelBlockStore().open();
			await pr.save(mem);
	
			let ids: (Uint8Array | undefined)[] = [];
			for (const [_ix, log] of pr.allLogs.entries()) {
				ids.push(log.id);
				log.id = undefined;
			}
			const prehash = sha256Sync(serialize(pr));
	
			for (const [ix, log] of pr.allLogs.entries()) {
				log.id = ids[ix];
			}
	
			let logAId = sha256Sync(prehash);
			let logBId = sha256Sync(logAId);
			let logA2Id = sha256Sync(logBId);
			let logCId = sha256Sync(logA2Id);
	
			expect(pr.programA.logA.id).toEqual(logAId);
			expect(pr.programB.logB.id).toEqual(logBId);
			expect(pr.programB.programA.logA.id).toEqual(logA2Id);
			expect(pr.logC.id).toEqual(logCId);
	
			await mem.close();
		}); */

		it("can drop", async () => {
			let p = new P4();

			let open = async (open: Program): Promise<Program> => {
				return open;
			};
			await p.open(peer, {
				open,
				log: {},
			} as any);

			await p.drop();
		});
	});

	describe("waitFor", () => {
		/* it("invokes getReady on all components", async () => {
			const p = new P2(new Log());
	
			let open = async (open: Program): Promise<Program> => {
				return open;
			};
			let fromInit = false;
			await p.init(peer, {
				open,
				log: {},
				getReady: async () => {
					fromInit = true;
					return new Set();
				},
			} as any);
	
			let outer = false;
			let getReadyFn = p.getReady.bind(p);
			p.getReady = async () => {
				outer = true;
				return getReadyFn();
			};
	
			let inner = false;
			let getReadyFn2 = p.program.getReady.bind(p.program);
	
			p.program.getReady = async () => {
				inner = true;
				return getReadyFn2();
			};
	
			await p.waitFor();
			expect(outer).toBeTrue();
			expect(inner).toBeTrue();
			expect(fromInit).toBeTrue();
		});
	
		it("isReady", async () => {
			const p = new P2(new Log());
	
			let open = async (open: Program): Promise<Program> => {
				return open;
			};
			let fromInit = false;
			const kpReady = await Ed25519Keypair.create();
			const kpNotReady = await Ed25519Keypair.create();
	
			await p.init(peer, {
				open,
				log: {},
				getReady: async () => {
					fromInit = true;
					return new Set([kpReady.publicKey.hashcode()]);
				},
			} as any);
			expect(await p.isReady(kpReady.publicKey)).toBeTrue();
			expect(await p.isReady(kpNotReady.publicKey)).toBeFalse();
		}); */
	});
});
