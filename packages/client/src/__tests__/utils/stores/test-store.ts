import { field, variant } from "@dao-xyz/borsh";
import { Program } from "@dao-xyz/peerbit-program";
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";
import { Network } from "../../../network";
import { EventStore } from "./event-store";

@variant("permissioned_program")
export class PermissionedEventStore extends Program implements Network {
  inNetwork: true = true;

  @field({ type: EventStore })
  _store: EventStore<string>;

  @field({ type: TrustedNetwork })
  _network: TrustedNetwork;

  constructor(properties?: {
    store?: EventStore<string>;
    network: TrustedNetwork;
  }) {
    super();
    if (properties) {
      this._network = properties.network;
      this._store =
        properties.store || new EventStore({ id: this._network.id });
    }
  }

  get network(): TrustedNetwork {
    return this._network;
  }

  get store(): EventStore<string> {
    return this._store;
  }

  async setup(): Promise<void> {
    await this._store.setup();
    await this._network.setup();
  }
}
