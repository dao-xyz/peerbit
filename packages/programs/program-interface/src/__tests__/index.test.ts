import { field, variant, vec, option, serialize } from "@dao-xyz/borsh";
import { ComposableProgram, Program } from "..";
import { getValuesWithType } from "../utils.js";
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { MemoryLevelBlockStore } from "@dao-xyz/libp2p-direct-block";
import { Ed25519Keypair, sha256Sync } from "@dao-xyz/peerbit-crypto";
import { Log } from "@dao-xyz/peerbit-log";

@variant("x1")
class P1 extends ComposableProgram {}
class EmbeddedStore {
	@field({ type: Log })
	log: Log<any>;
	constructor(properties?: { log: Log<any> }) {
		if (properties) {
			this.log = properties.log;
		}
	}
}
class ExtendedEmbeddedStore extends EmbeddedStore {
	constructor(properties?: { log: Log<any> }) {
		super(properties);
	}
}
@variant("p2")
class P2 extends Program {
	@field({ type: option(vec(ExtendedEmbeddedStore)) })
	log?: ExtendedEmbeddedStore[];

	@field({ type: P1 })
	program: P1;

	constructor(log: Log<any>) {
		super();
		this.log = [new ExtendedEmbeddedStore({ log: log })];
		this.program = new P1();
	}

	async setup(): Promise<void> {
		if (this.log) {
			await Promise.all(this.log?.map((x) => x.log.setup()));
		}
	}
}

@variant("p3")
class P3 extends Program {
	constructor() {
		super();
	}

	async setup(): Promise<void> {
		return;
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

	async setup(): Promise<void> {
		return;
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
describe("program", () => {
	let session: LSession;
	beforeAll(async () => {
		session = await LSession.connected(1);
	});

	afterAll(async () => {
		await session.stop();
	});

	it("can re-open from closed", async () => {
		const p = new P3();
		let open = async (open: Program): Promise<Program> => {
			return open;
		};
		await p.init(session.peers[0], await Ed25519Keypair.create(), {
			open,
			log: {},
		} as any);

		expect(p.closed).toBeFalse();
		expect(p.initialized).toBeTrue();
		await p.close();
		expect(p.closed).toBeTrue();
		expect(p.initialized).toBeTrue();
		await p.init(session.peers[0], await Ed25519Keypair.create(), {
			open,
			log: {},
		} as any);
		expect(p.closed).toBeFalse();
		expect(p.initialized).toBeTrue();
	});

	it("can re-open from dropped", async () => {
		const p = new P3();
		let open = async (open: Program): Promise<Program> => {
			return open;
		};
		await p.init(session.peers[0], await Ed25519Keypair.create(), {
			open,
			log: {},
		} as any);

		expect(p.closed).toBeFalse();
		expect(p.initialized).toBeTrue();
		await p.drop();
		expect(p.closed).toBeTrue();
		expect(p.initialized).toBeFalse();
		await p.init(session.peers[0], await Ed25519Keypair.create(), {
			open,
			log: {},
		} as any);
		expect(p.closed).toBeFalse();
		expect(p.initialized).toBeTrue();
	});

	it("can resolve stores and programs", () => {
		const log = new Log();
		const p = new P2(log);

		// stores
		const logs = p.allLogs;
		expect(logs).toEqual([log]);

		// programs
		const programs = p.programs;
		expect(programs).toHaveLength(1);
		expect(programs[0]).toEqual(p.program);
	});

	describe("init", () => {
		it("inits before setup", async () => {
			const log = new Log();
			const p = new P2(log);

			let initializedProgram = false;
			let setupProgram = false;
			let pInit = p.init.bind(p);
			p.init = (a, b, c): any => {
				initializedProgram = true;
				return pInit(a, b, c);
			};

			let pSetup = p.setup.bind(p);
			p.setup = (): any => {
				if (!initializedProgram) {
					throw new Error("Not initialized");
				} else {
					setupProgram = true;
				}
				return pSetup();
			};

			let setupLog = false;
			let openLog = false;

			const lSetup = log.setup.bind(log);
			p.log![0]!.log.setup = (o): any => {
				if (openLog) {
					throw new Error("Already open!");
				} else {
					setupLog = true;
				}
				return lSetup(o);
			};

			const lInit = log.open.bind(log);
			p.log![0]!.log.open = (a, b, c): any => {
				openLog = true;
				if (!setupLog) {
					throw new Error("Not setup");
				}
				return lInit(a, b, c);
			};

			let open = async (open: Program): Promise<Program> => {
				return open;
			};

			await p.init(session.peers[0], await Ed25519Keypair.create(), {
				open,
				log: {},
			} as any);

			expect(initializedProgram).toBeTrue();
			expect(openLog).toBeTrue();
			expect(setupLog).toBeTrue();
			expect(setupProgram).toBeTrue();
		});
	});

	describe("clear", () => {
		it("clears stores and programs on clear", async () => {
			const log = new Log();
			const p = new P2(log);
			p.logs;
			p.allLogs;
			p.allLogsMap;
			p.allPrograms;
			expect(p["_logs"]).toBeDefined();
			expect(p["_allLogs"]).toBeDefined();
			expect(p["_allLogsMap"]).toBeDefined();
			expect(p["_allPrograms"]).toBeDefined();
			await p["_clear"]();
			expect(p["_logs"]).toBeUndefined();
			expect(p["_allLogs"]).toBeUndefined();
			expect(p["_allLogsMap"]).toBeUndefined();
			expect(p["_allPrograms"]).toBeUndefined();
		});

		it("invokes clear on close", async () => {
			const log = new Log();
			const p = new P2(log);

			let open = async (open: Program): Promise<Program> => {
				return open;
			};
			await p.init(session.peers[0], await Ed25519Keypair.create(), {
				open,
				log: {},
			} as any);
			expect(p.initialized).toBeTrue();
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
			expect(p.initialized).toBeUndefined(); // TODO false?
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
			await p.init(session.peers[0], await Ed25519Keypair.create(), {
				open,
				log: {},
			} as any);
			expect(p.initialized).toBeTrue();
			let cleared = false;
			p["_clear"] = () => {
				cleared = true;
			};
			await p.drop();
			expect(cleared).toBeTrue();
		});

		it("invokes clear on drop non-initialized", async () => {
			const log = new Log();
			const p = new P2(log);
			expect(p.initialized).toBeUndefined();
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
			await p.init(session.peers[0], await Ed25519Keypair.create(), {
				open,
				log: {},
			} as any);
			await p2.init(session.peers[0], await Ed25519Keypair.create(), {
				open,
				log: {},
			} as any);

			let p3 = await p.open!(new P3());
			let closeCounter = 0;

			// Open it outside a program (call init on p3)
			await p3.init(session.peers[0], await Ed25519Keypair.create(), {
				onClose: () => {
					closeCounter++;
				},
				log: {},
			} as any);

			expect(p.programsOpened).toHaveLength(1);
			expect(p3.programsOpened).toBeUndefined();
			expect(p3.openedByPrograms).toContainAllValues([undefined, p]);

			await p2.open!(p3);
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

			await p.init(session.peers[0], await Ed25519Keypair.create(), {
				open,
				log: {},
			} as any);
			await p2.init(session.peers[0], await Ed25519Keypair.create(), {
				open,
				log: {},
			} as any);

			let p3 = await p.open!(new P3());

			expect(p.programsOpened).toHaveLength(1);
			expect(p3.programsOpened).toBeUndefined();
			expect(p3.openedByPrograms).toContainAllValues([p]);
			expect(p3.closed).toBeFalse();

			await p2.open!(p3);
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

		it("will create indices", async () => {
			@variant("pa")
			class ProgramA extends ComposableProgram {
				@field({ type: Log })
				logA: Log<any> = new Log();
			}

			@variant("pb")
			class ProgramB extends ComposableProgram {
				@field({ type: Log })
				logB: Log<any> = new Log();

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

				async setup(): Promise<void> {}
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
		});

		it("can drop", async () => {
			let p = new P4();

			let open = async (open: Program): Promise<Program> => {
				return open;
			};
			await p.init(session.peers[0], await Ed25519Keypair.create(), {
				open,
				log: {},
			} as any);

			await p.drop();
		});
	});

	describe("waitFor", () => {
		it("invokes waitFor on all components", async () => {
			const p = new P2(new Log());

			let open = async (open: Program): Promise<Program> => {
				return open;
			};
			let fromInit = false;
			await p.init(session.peers[0], await Ed25519Keypair.create(), {
				open,
				log: {},
				waitFor: async () => {
					fromInit = true;
				},
			} as any);

			let outer = false;
			let waitForPeerFn = p.waitFor.bind(p);
			p.waitFor = async (o) => {
				outer = true;
				return waitForPeerFn(o);
			};

			let inner = false;
			let waitForPeerFn2 = p.program.waitFor.bind(p.program);

			p.program.waitFor = async (o) => {
				inner = true;
				return waitForPeerFn2(o);
			};

			await p.waitFor(undefined as any);
			expect(outer).toBeTrue();
			expect(inner).toBeTrue();
			expect(fromInit).toBeTrue();
		});
	});
});
