import { field, variant, vec, option } from "@dao-xyz/borsh";
import { Store } from "@dao-xyz/peerbit-store";
import { ComposableProgram, Program } from "..";
import { getValuesWithType } from "../utils.js";
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { MemoryLevelBlockStore } from "@dao-xyz/libp2p-direct-block";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";

@variant("x1")
class P1 extends ComposableProgram {}
class EmbeddedStore {
	@field({ type: Store })
	store: Store<any>;
	constructor(properties?: { store: Store<any> }) {
		if (properties) {
			this.store = properties.store;
		}
	}
}
class ExtendedEmbeddedStore extends EmbeddedStore {
	constructor(properties?: { store: Store<any> }) {
		super(properties);
	}
}
@variant("p2")
class P2 extends Program {
	@field({ type: vec(option(ExtendedEmbeddedStore)) })
	store?: ExtendedEmbeddedStore[];

	@field({ type: P1 })
	program: P1;

	constructor(store: Store<any>) {
		super();
		this.store = [new ExtendedEmbeddedStore({ store })];
		this.program = new P1();
	}

	async setup(): Promise<void> {
		return;
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

describe("getValuesWithType", () => {
	it("can stop at type", () => {
		const store = new Store();
		const p = new P2(store);

		let stores = getValuesWithType(p, Store, EmbeddedStore);
		expect(stores).toEqual([]);
		stores = getValuesWithType(p, Store);
		expect(stores).toEqual([store]);
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
			store: {},
		} as any);

		expect(p.closed).toBeFalse();
		expect(p.initialized).toBeTrue();
		await p.close();
		expect(p.closed).toBeTrue();
		expect(p.initialized).toBeFalse();
		await p.init(session.peers[0], await Ed25519Keypair.create(), {
			open,
			store: {},
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
			store: {},
		} as any);

		expect(p.closed).toBeFalse();
		expect(p.initialized).toBeTrue();
		await p.drop();
		expect(p.closed).toBeTrue();
		expect(p.initialized).toBeFalse();
		await p.init(session.peers[0], await Ed25519Keypair.create(), {
			open,
			store: {},
		} as any);
		expect(p.closed).toBeFalse();
		expect(p.initialized).toBeTrue();
	});

	it("can resolve stores and programs", () => {
		const store = new Store();
		const p = new P2(store);

		// stores
		const stores = p.allStores;
		expect(stores).toEqual([store]);

		// programs
		const programs = p.programs;
		expect(programs).toHaveLength(1);
		expect(programs[0]).toEqual(p.program);
	});

	it("create subprogram address", async () => {
		const store = new Store();
		const p = new P2(store);
		const mem = await new MemoryLevelBlockStore().open();
		await p.save(mem);
		expect(p.program.address.toString()).toEndWith("/0");
		await mem.close();
	});

	it("subprogram will not close if opened outside a program", async () => {
		const p = new P3();
		const p2 = new P3();

		let open = async (open: Program): Promise<Program> => {
			return open;
		};
		await p.init(session.peers[0], await Ed25519Keypair.create(), {
			open,
			store: {},
		} as any);
		await p2.init(session.peers[0], await Ed25519Keypair.create(), {
			open,
			store: {},
		} as any);

		let p3 = await p.open!(new P3());
		let closeCounter = 0;

		// Open it outside a program (call init on p3)
		await p3.init(session.peers[0], await Ed25519Keypair.create(), {
			onClose: () => {
				closeCounter++;
			},
			store: {},
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
			store: {},
		} as any);
		await p2.init(session.peers[0], await Ed25519Keypair.create(), {
			open,
			store: {},
		} as any);

		let p3 = await p.open!(new P3());

		expect(p.programsOpened).toHaveLength(1);
		expect(p3.programsOpened).toBeUndefined();
		expect(p3.openedByPrograms).toContainAllValues([p]);

		await p2.open!(p3);
		expect(p.programsOpened).toHaveLength(1);
		expect(p2.programsOpened).toHaveLength(1);
		expect(p3.programsOpened).toBeUndefined();
		expect(p3.openedByPrograms).toContainAllValues([p, p2]);

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
			@field({ type: Store })
			storeA: Store<any> = new Store();
		}

		@variant("pb")
		class ProgramB extends ComposableProgram {
			@field({ type: Store })
			storeB: Store<any> = new Store();

			@field({ type: ProgramA })
			programA = new ProgramA();
		}

		@variant("pc")
		class ProgramC extends Program {
			@field({ type: ProgramA })
			programA = new ProgramA();

			@field({ type: ProgramB })
			programB = new ProgramB();

			@field({ type: Store })
			storeC = new Store();

			async setup(): Promise<void> {}
		}

		const pr = new ProgramC();
		const mem = await new MemoryLevelBlockStore().open();
		await pr.save(mem);

		expect(pr._programIndex).toBeUndefined();
		expect(pr.programA._programIndex).toEqual(0);
		expect(pr.programB._programIndex).toEqual(1);
		expect(pr.programB.programA._programIndex).toEqual(2);
		expect(pr.programA.storeA._storeIndex).toEqual(0);
		expect(pr.programB.storeB._storeIndex).toEqual(1);
		expect(pr.programB.programA.storeA._storeIndex).toEqual(2);
		expect(pr.storeC._storeIndex).toEqual(3);

		await mem.close();
	});
});
