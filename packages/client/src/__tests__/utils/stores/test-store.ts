import { Store } from "@dao-xyz/peerbit-dstore";
import { variant } from '@dao-xyz/borsh';

@variant(254)
export class TestStore<T> extends Store<T> {

}