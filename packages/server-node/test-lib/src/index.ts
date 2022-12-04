import { field, variant } from "@dao-xyz/borsh";
import { Program } from "@dao-xyz/peerbit-program";
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";
import { network } from "@dao-xyz/peerbit";
import { DString } from "@dao-xyz/peerbit-string";

@variant("permissioned_string")
@network({ property: "_network" })
export class PermissionedString extends Program {
    @field({ type: DString })
    _store: DString;

    @field({ type: TrustedNetwork })
    _network: TrustedNetwork;

    constructor(properties?: { store?: DString; network: TrustedNetwork }) {
        super();
        if (properties) {
            this._network = properties.network;
            this._store = properties.store || new DString({});
        }
    }

    get network(): TrustedNetwork {
        return this._network;
    }

    get store(): DString {
        return this._store;
    }

    async setup(): Promise<void> {
        await this._store.setup();
        await this._network.setup();
    }
}
