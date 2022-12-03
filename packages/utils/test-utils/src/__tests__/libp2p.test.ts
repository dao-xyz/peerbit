import { LSession } from "../libp2p";
import { delay, waitFor } from "@dao-xyz/peerbit-time";
import waitForPeers from "../wait-for-peers";

it("test pubsub 2", async () => {
    const session = await LSession.connected(2);
    let msg: any = undefined;
    const data = new Uint8Array([1, 2, 3]);
    const topic = "abc";

    // Subscribe multiple times
    session.peers[0].pubsub.subscribe(topic);
    session.peers[0].pubsub.subscribe(topic);
    session.peers[0].pubsub.subscribe(topic);

    expect(session.peers[0].pubsub.getTopics()).toHaveLength(1);

    session.peers[1].pubsub.subscribe(topic);

    let counter = 0;
    session.peers[0].pubsub.addEventListener("message", (message) => {
        counter += 1;
        msg = Buffer.isBuffer(message.detail.data)
            ? new Uint8Array(message.detail.data as Buffer)
            : message.detail.data;
    });
    // wait for subscriptions to propagate
    await waitForPeers(session.peers[1], [session.peers[0].peerId], topic);
    await session.peers[1].pubsub.publish(topic, data);
    await waitFor(() => !!msg); // No publicates
    await delay(3000);
    expect(counter).toEqual(1);
    expect(msg).toEqual(data);
    await session.stop();
});

it("test pubsub 4", async () => {
    const session = await LSession.connected(4);
    let msg: Uint8Array | undefined = undefined;
    const data = new Uint8Array([1, 2, 3]);
    const topic = "xyz";
    session.peers[0].pubsub.subscribe(topic);
    session.peers[0].pubsub.addEventListener("message", (message) => {
        msg = Buffer.isBuffer(message.detail.data)
            ? new Uint8Array(message.detail.data as Buffer)
            : message.detail.data;
    });
    // wait for subscriptions to propagate
    await waitForPeers(session.peers[1], [session.peers[0].peerId], topic);
    await session.peers[3].pubsub.publish(topic, data);
    await waitFor(() => !!msg);
    expect(msg).toEqual(data);
    await session.stop();
});
