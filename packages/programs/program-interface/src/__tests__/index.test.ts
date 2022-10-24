import { field, variant, vec, option } from "@dao-xyz/borsh"
import { Store } from "@dao-xyz/peerbit-store";
import { ComposableProgram, Program } from "..";

describe('program', () => {
    it('can resolve stores and programs', () => {

        @variant(0)
        class P1 extends ComposableProgram {

        }
        class EmbeddedStore {

            @field({ type: Store })
            store: Store<any>
            constructor(properties?: { store: Store<any> }) {
                if (properties) {
                    this.store = properties.store
                }
            }
        }
        @variant(1)
        class P2 extends ComposableProgram {

            @field({ type: vec(option(EmbeddedStore)) })
            store?: EmbeddedStore[];

            @field({ type: P1 })
            program: P1;

            constructor(store: Store<any>) {
                super();
                this.store = [new EmbeddedStore({ store })];
                this.program = new P1();
            }
        }
        const store = new Store();
        const p = new P2(store);

        // stores
        const stores = p.stores;
        expect(stores).toHaveLength(1);
        expect(stores[0]).toEqual(store);

        // programs
        const programs = p.programs;
        expect(programs).toHaveLength(1);
        expect(programs[0]).toEqual(p.program);
    })
})