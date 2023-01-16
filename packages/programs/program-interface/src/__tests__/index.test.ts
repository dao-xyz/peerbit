import { field, variant, vec, option } from "@dao-xyz/borsh";
import { Store } from "@dao-xyz/peerbit-store";
import { ComposableProgram, Program } from "..";
import { getValuesWithType } from "../utils.js";
import { LSession } from "@dao-xyz/peerbit-test-utils";
import {
    BlockStore,
    MemoryLevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";

@variant(0)
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
        await p.save(new MemoryLevelBlockStore());
        expect(p.program.address.toString()).toEndWith("/0");
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
        await pr.save(new MemoryLevelBlockStore());

        expect(pr._programIndex).toBeUndefined();
        expect(pr.programA._programIndex).toEqual(0);
        expect(pr.programB._programIndex).toEqual(1);
        expect(pr.programB.programA._programIndex).toEqual(2);
        expect(pr.programA.storeA._storeIndex).toEqual(0);
        expect(pr.programB.storeB._storeIndex).toEqual(1);
        expect(pr.programB.programA.storeA._storeIndex).toEqual(2);
        expect(pr.storeC._storeIndex).toEqual(3);
    });
});
