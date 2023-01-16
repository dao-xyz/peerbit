import { Libp2p } from "libp2p";
import { LSession as SSession } from "@dao-xyz/libp2p-test-utils";
import { RecursivePartial } from "@libp2p/interfaces";
import { Datastore } from "interface-datastore";
import { DirectSub } from "@dao-xyz/libp2p-direct-sub";
import {
    DirectBlock,
    MemoryLevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";

export type LibP2POptions = {
    datastore?: RecursivePartial<Datastore> | undefined;
};
export type Libp2pExtended = Libp2p & {
    directsub: DirectSub;
    directblock: DirectBlock;
};
export class LSession {
    private session: SSession<Libp2pExtended>;
    constructor(session: SSession<Libp2pExtended>) {
        this.session = session;
    }

    public get peers(): Libp2pExtended[] {
        return this.session.peers;
    }

    async connect(groups?: Libp2pExtended[][]) {
        await this.session.connect(groups);
        return;
    }
    async stop() {
        return this.session.stop();
    }

    static async connected(n: number) {
        const session = await LSession.disconnected(n);
        await session.connect();
        return session;
    }

    static async disconnected(n: number, options?: LibP2POptions) {
        const session: SSession<Libp2pExtended> =
            await SSession.disconnected<Libp2pExtended>(n, options);
        session.peers.forEach((peer) => {
            peer.directsub = new DirectSub(peer, {
                canRelayMessage: true,
                signaturePolicy: "StrictNoSign",
            });
            peer.directblock = new DirectBlock(peer, {
                localStore: new MemoryLevelBlockStore(),
            });
        });
        await Promise.all(session.peers.map((x) => x.directsub.start()));
        return new LSession(session);
    }
}
