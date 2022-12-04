import { variant, field, serialize, vec } from "@dao-xyz/borsh";
import { TransportMessage } from "./message.js";
import { PeerIdAddress } from "@dao-xyz/peerbit-crypto";
import { MaybeSigned, SignatureWithKey } from "@dao-xyz/peerbit-crypto";
import { DecryptedThing } from "@dao-xyz/peerbit-crypto";
import { Identity } from "@dao-xyz/ipfs-log";
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";
import type { PeerId } from "@libp2p/interface-peer-id";
import type { AddressBook } from "@libp2p/interface-peer-store";
import { multiaddr } from "@multiformats/multiaddr";
import { peerIdFromString } from "@libp2p/peer-id";

@variant(0)
export class PeerInfo {
    @field({ type: "string" })
    id: string;

    @field({ type: vec("string") })
    addresses: string[];

    constructor(props: { id: string; addresses: string[] }) {
        this.id = props.id;
        this.addresses = props.addresses;
    }
    get peerId(): PeerId {
        return peerIdFromString(this.id);
    }
    get multiaddrs() {
        return this.addresses.map((a) => multiaddr(a));
    }
}

@variant([3, 0])
export class ExchangeSwarmMessage extends TransportMessage {
    @field({ type: vec(PeerInfo) })
    info: PeerInfo[];

    // TODO peer info for sending repsonse directly
    constructor(props: { info: PeerInfo[] }) {
        super();
        this.info = props.info;
    }
}

export const exchangeSwarmAddresses = async (
    send: (data: Uint8Array) => Promise<any>,
    identity: Identity,
    peerReciever: string,
    peers: PeerId[],
    addressBook: AddressBook,
    network?: TrustedNetwork,
    localNetwork?: boolean
) => {
    let trustedAddresses: PeerId[];
    if (network) {
        const isTrusted = (peer: PeerId) =>
            network.isTrusted(new PeerIdAddress({ address: peer.toString() }));
        trustedAddresses = await Promise.all(peers.map(isTrusted)).then(
            (results) => peers.filter((_v, index) => results[index])
        );
    } else {
        trustedAddresses = peers;
    }
    const isLocalhostAddress = (addr: string) =>
        addr.toString().includes("/127.0.0.1/");
    const filteredAddresses: PeerInfo[] = (
        await Promise.all(
            trustedAddresses
                .filter(
                    (x) =>
                        x.toString() !== peerReciever &&
                        (localNetwork || !isLocalhostAddress(x.toString()))
                )
                .map(async (x) => {
                    try {
                        const addresses = (await addressBook.get(x)).map(
                            (a) => a
                        );
                        return new PeerInfo({
                            id: x.toString(),
                            addresses: addresses.map((a) =>
                                a.multiaddr.toString()
                            ),
                        });
                    } catch (error) {
                        return undefined;
                    }
                })
        )
    ).filter((x) => !!x) as PeerInfo[];

    if (filteredAddresses.length === 0) {
        return;
    }

    const message = serialize(
        new ExchangeSwarmMessage({
            info: filteredAddresses,
        })
    );
    const signatureResult = await identity.sign(message);
    await send(
        serialize(
            await new DecryptedThing<ExchangeSwarmMessage>({
                data: serialize(
                    new MaybeSigned({
                        signature: new SignatureWithKey({
                            signature: signatureResult,
                            publicKey: identity.publicKey,
                        }),
                        data: message,
                    })
                ),
            })
        )
    );
};
