import { field, variant } from "@dao-xyz/borsh"
import { Store } from "@dao-xyz/peerbit-dstore";
import { Program } from "..";

describe('program', () => {
    it('can resolve stores and programs', () => {

        @variant(0)
        class P1 extends Program {

        }
        @variant(1)
        class P2 extends Program {
            @field({ type: Store })
            store: Store<any>;

            @field({ type: P1 })
            program: P1;

            constructor() {
                super();
                this.store = new Store({});
                this.program = new P1();
            }
        }
        const p = new P2();
        const stores = p.stores;
        expect(stores).toHaveLength(1);
        expect(stores[0]).toEqual(p.store);
        const programs = p.programs;
        expect(programs).toHaveLength(1);
        expect(programs[0]).toEqual(p.program);
    })
})