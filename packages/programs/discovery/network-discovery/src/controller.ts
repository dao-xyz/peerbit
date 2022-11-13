import { deserialize, field, variant } from "@dao-xyz/borsh";
import {
  Documents,
  DeleteOperation,
  Operation,
  PutOperation,
} from "@dao-xyz/peerbit-document";
import { Entry, Identity } from "@dao-xyz/ipfs-log";
import { IPFS } from "ipfs-core-types";
import { createDiscoveryStore, NetworkInfo } from "./state";
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";
import {
  Program,
  ProgramInitializationOptions,
} from "@dao-xyz/peerbit-program";
import { multiaddr } from "@multiformats/multiaddr";

@variant("network_discovery")
export class NetworkDiscovery extends Program {
  @field({ type: Documents })
  info: Documents<NetworkInfo>;

  _peerId: string;
  _options: ProgramInitializationOptions;

  constructor(props?: { id?: string; queryRegion?: string }) {
    super(props);
    this.info = createDiscoveryStore(props);
  }

  async init(
    ipfs: IPFS,
    identity: Identity,
    options: ProgramInitializationOptions
  ): Promise<this> {
    this._peerId = (await ipfs.id()).id.toString();
    this._options = options;
    return super.init(ipfs, identity, options);
  }

  async canAppend(entry: Entry<Operation<NetworkInfo>>): Promise<boolean> {
    // check if the peer id is trusted by the signature
    const operation = await entry.getPayloadValue();

    // i.e. load the network?
    if (
      operation instanceof PutOperation ||
      operation instanceof DeleteOperation
    ) {
      let info: NetworkInfo;
      if (operation instanceof DeleteOperation) {
        const retrievedValue = this.info.index.get(operation.key);
        if (!retrievedValue) {
          return false;
        }
        info = retrievedValue.value;
      } else {
        info = operation._value || deserialize(operation.data, NetworkInfo);
      }
      const existingAddresses = await this.info.store._ipfs.swarm.peers();
      const existingAddressesSet = new Set(
        existingAddresses.map((x) => x.addr.toString())
      );

      const suffix = "/p2p/" + info.peerId;
      const getMAddress = (a: string) =>
        multiaddr(a.toString() + (a.indexOf(suffix) === -1 ? suffix : ""));

      const isNotMe = info.peerId !== this._peerId;
      if (isNotMe) {
        await Promise.all(
          info.addresses
            .filter((a) => !existingAddressesSet.has(a))
            .map((a) => this._ipfs.swarm.connect(getMAddress(a)))
        );
      }
      const network: TrustedNetwork = await Program.load(
        this.info.store._ipfs,
        info.network
      );

      await network.init(this._ipfs, this._identity, {
        ...this._options,
        store: { ...this._options.store, replicate: false },
      });
      const isTrusted: boolean = await network.isTrusted(
        await entry.getPublicKey()
      );

      // Close open connections
      if (isNotMe) {
        await Promise.all(
          info.addresses
            .filter((a) => !existingAddressesSet.has(a))
            .map((a) => this._ipfs.swarm.disconnect(getMAddress(a)))
        );
      }
      return isTrusted;
    }

    return false;
  }

  async setup() {
    await this.info.setup({
      type: NetworkInfo,
      canAppend: this.canAppend.bind(this),
    }); // self referencing access controller
  }

  async addInfo(network: TrustedNetwork) {
    const id = await this._ipfs.id();
    const isNotLocalhostAddress = (addr: string) =>
      !addr.toString().includes("/127.0.0.1/");
    return this.info.put(
      new NetworkInfo({
        network: network.address?.toString(),
        peerId: id.id.toString(),
        addresses: id.addresses
          .map((x) => x.toString())
          .filter(isNotLocalhostAddress),
      })
    );
  }
}
