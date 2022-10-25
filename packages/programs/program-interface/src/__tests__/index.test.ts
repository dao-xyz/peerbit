import { field, variant, vec, option } from "@dao-xyz/borsh"
import { Store } from "@dao-xyz/peerbit-store";
import { ComposableProgram, Program } from "..";
import { getValuesWithType } from "../utils";

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
class ExtendedEmbeddedStore extends EmbeddedStore {
    constructor(properties?: { store: Store<any> }) {
        super(properties)
    }
}
@variant(1)
class P2 extends ComposableProgram {

    @field({ type: vec(option(ExtendedEmbeddedStore)) })
    store?: ExtendedEmbeddedStore[];

    @field({ type: P1 })
    program: P1;

    constructor(store: Store<any>) {
        super();
        this.store = [new ExtendedEmbeddedStore({ store })];
        this.program = new P1();
    }
}



describe('getValuesWithType', () => {

    it('can stop at type', () => {

        const store = new Store();
        const p = new P2(store);

        let stores = getValuesWithType(p, Store, EmbeddedStore)
        expect(stores).toEqual([]);
        stores = getValuesWithType(p, Store)
        expect(stores).toEqual([store]);

    })
})
describe('program', () => {
    it('can resolve stores and programs', () => {


        const store = new Store();
        const p = new P2(store);

        // stores
        const stores = p.allStores;
        expect(stores).toEqual([store]);

        // programs
        const programs = p.programs;
        expect(programs).toHaveLength(1);
        expect(programs[0]).toEqual(p.program);
    })
})