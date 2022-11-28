import { LSession } from "../libp2p";
import { delay, waitFor } from "@dao-xyz/peerbit-time";

describe("session", () => {
    it("test pubsub", async () => {
        const session = await LSession.connected(2);
        let msg: Uint8Array | undefined = undefined;
        const data = new Uint8Array([1, 2, 3]);
        session.peers[0].pubsub.subscribe("abc");
        session.peers[0].pubsub.addEventListener("message", (message) => {
            msg = message.detail.data;
        });
        // wait for subscriptions to propagate
        await delay(1000);

        session.peers[1].pubsub.publish("abc", data);
        await waitFor(() => !!msg);
        expect(msg).toEqual(data);
    });
});
