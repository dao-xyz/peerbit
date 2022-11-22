import { field, variant } from "@dao-xyz/borsh";
import { Program } from "@dao-xyz/peerbit-program";
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";
import { Network } from "@dao-xyz/peerbit";
import { DString } from "@dao-xyz/peerbit-string";

@variant("permissioned_string")
export class PermissionedString extends Program implements Network {
    inNetwork: true = true;

    @field({ type: DString })
    _store: DString;
    ß;
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
