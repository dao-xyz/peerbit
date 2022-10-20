import { field } from "@dao-xyz/borsh"
import { Store } from "@dao-xyz/peerbit-dstore";
import { Program } from "..";

describe('program', () => {
    it('can resolve stores and programs', () => {
        class P extends Program {

        }

        class P2 extends P {
            @field({ type: Store })
            store: Store<any>;

            @field({ type: Program })
            program: Program;

            constructor() {
                super();
                this.store = new Store({});
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