import { DirectChannel } from "@dao-xyz/ipfs-pubsub-direct-channel";
import { Peerbit } from "../peer";
import { EventStore } from "./utils/stores/event-store";
import { v4 as uuid } from "uuid";
import { waitForPeers, LSession } from "@dao-xyz/peerbit-test-utils";
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";
import { waitFor } from "@dao-xyz/peerbit-time";
import { PermissionedEventStore } from "./utils/stores/test-store";
import { DEFAULT_BLOCK_TRANSPORT_TOPIC } from "@dao-xyz/peerbit-block";

describe(`leaders`, function () {
    let session: LSession;
    let orbitdb1: Peerbit,
        orbitdb2: Peerbit,
        orbitdb3: Peerbit,
        db1: EventStore<string>,
        db2: EventStore<string>,
        db3: EventStore<string>;

    beforeAll(async () => {
        session = await LSession.connected(3, [DEFAULT_BLOCK_TRANSPORT_TOPIC]);
    });

    afterAll(async () => {
        await session.stop();
    });

    beforeEach(async () => {
        orbitdb1 = await Peerbit.create(session.peers[0], {});
        orbitdb2 = await Peerbit.create(session.peers[1], {});
        orbitdb3 = await Peerbit.create(session.peers[2], {});
    });

    afterEach(async () => {
        if (db1) await db1.drop();

        if (db2) await db2.drop();

        if (db3) await db3.drop();

        if (orbitdb1) await orbitdb1.stop();

        if (orbitdb2) await orbitdb2.stop();

        if (orbitdb3) await orbitdb3.stop();
    });

    it("will use trusted network for filtering", async () => {
        const network = new TrustedNetwork({
            id: "network-tests",
            rootTrust: orbitdb1.identity.publicKey,
        });
        const program = await orbitdb1.open(
            new PermissionedEventStore({ network })
        );
        await orbitdb1.join(program);

        // make client 2 trusted
        await network.add(orbitdb2.id);
        await network.add(orbitdb2.identity.publicKey);
        const program2 = await orbitdb2.open<PermissionedEventStore>(
            program.address!
        );
        await waitFor(() => program2.network.trustGraph.index.size === 3);
        await orbitdb2.join(program2);

        // but dont trust client 3
        // however open direct channels so client 3 could perhaps be a leader anyway (?)
        orbitdb1.getChannel(
            orbitdb3.id.toString(),
            network.address!.toString()
        );
        orbitdb3.getChannel(
            orbitdb1.id.toString(),
            network.address!.toString()
        );
        await waitFor(() => orbitdb1._directConnections.size === 2); // to 2 and 3
        await waitFor(() => orbitdb2._directConnections.size === 1); // to 1
        await waitFor(() => orbitdb3._directConnections.size === 1); // to 1
        await waitFor(() => program2.network.trustGraph.index.size === 4); // 1. identiy -> peer id, 1. -> 2 identity, 1. -> 2. peer id and 2. identity -> peer id,

        // now find 3 leaders from the network with 2 trusted participants (should return 2 leaders if trust control works correctly)
        const leadersFrom1 = await orbitdb1.findLeaders(
            program.address.toString(),
            network.address!.toString(),
            "",
            3
        );
        const leadersFrom2 = await orbitdb2.findLeaders(
            program.address.toString(),
            network.address!.toString(),
            "",
            3
        );
        expect(leadersFrom1).toEqual(leadersFrom2);
        expect(leadersFrom1).toHaveLength(2);
        expect(leadersFrom1).toContainAllValues([
            orbitdb1.id.toString(),
            orbitdb2.id.toString(),
        ]);
    });

    it("select leaders for one or two peers", async () => {
        // TODO fix test timeout, isLeader is too slow as we need to wait for peers
        // perhaps do an event based get peers using the pubsub peers api

        const topic = uuid();
        db1 = await orbitdb1.open(
            new EventStore<string>({ id: "replication-tests" }),
            { topic: topic }
        );

        const isLeaderAOneLeader = orbitdb1.isLeader(
            await orbitdb1.findLeaders(topic, db1.address!.toString(), 123, 1)
        );
        expect(isLeaderAOneLeader);
        const isLeaderATwoLeader = orbitdb1.isLeader(
            await orbitdb1.findLeaders(topic, db1.address!.toString(), 123, 2)
        );
        expect(isLeaderATwoLeader);

        db2 = await orbitdb2.open<EventStore<string>>(db1.address!, {
            topic: topic,
        });

        await waitFor(() => orbitdb1._directConnections.size === 1);
        await waitFor(() => orbitdb2._directConnections.size === 1);

        // leader rotation is kind of random, so we do a sequence of tests
        for (let slot = 0; slot < 3; slot++) {
            // One leader
            const isLeaderAOneLeader = orbitdb1.isLeader(
                await orbitdb1.findLeaders(
                    topic,
                    db1.address!.toString(),
                    slot,
                    1
                )
            );
            const isLeaderBOneLeader = orbitdb2.isLeader(
                await orbitdb2.findLeaders(
                    topic,
                    db1.address!.toString(),
                    slot,
                    1
                )
            );
            expect([isLeaderAOneLeader, isLeaderBOneLeader]).toContainAllValues(
                [false, true]
            );

            // Two leaders
            const isLeaderATwoLeaders = orbitdb1.isLeader(
                await orbitdb1.findLeaders(
                    topic,
                    db1.address!.toString(),
                    slot,
                    2
                )
            );
            const isLeaderBTwoLeaders = orbitdb2.isLeader(
                await orbitdb2.findLeaders(
                    topic,
                    db1.address!.toString(),
                    slot,
                    2
                )
            );
            expect([
                isLeaderATwoLeaders,
                isLeaderBTwoLeaders,
            ]).toContainAllValues([true, true]);
        }
    });

    it("leader are selected from 1 replicating peer", async () => {
        // TODO fix test timeout, isLeader is too slow as we need to wait for peers
        // perhaps do an event based get peers using the pubsub peers api

        const topic = uuid();
        db1 = await orbitdb1.open(
            new EventStore<string>({ id: "replication-tests" }),
            {
                topic: topic,
                replicate: false,
            }
        );
        db2 = await orbitdb2.open<EventStore<string>>(db1.address!, {
            topic: topic,
        });

        // One leader
        const slot = 0;

        // Two leaders, but only one will be leader since only one is replicating
        const isLeaderA = orbitdb1.isLeader(
            await orbitdb1.findLeaders(topic, db1.address!.toString(), slot, 2)
        );
        const isLeaderB = orbitdb2.isLeader(
            await orbitdb2.findLeaders(topic, db1.address!.toString(), slot, 2)
        );
        expect(!isLeaderA); // because replicate is false
        expect(isLeaderB);
    });

    it("leader are selected from 2 replicating peers", async () => {
        // TODO fix test timeout, isLeader is too slow as we need to wait for peers
        // perhaps do an event based get peers using the pubsub peers api

        const topic = uuid();
        db1 = await orbitdb1.open(
            new EventStore<string>({ id: "replication-tests" }),
            {
                topic: topic,
                replicate: false,
            }
        );
        db2 = await orbitdb2.open<EventStore<string>>(db1.address!, {
            topic: topic,
        });
        db3 = await orbitdb3.open<EventStore<string>>(db1.address!, {
            topic: topic,
        });

        await waitForPeers(
            session.peers[1],
            [orbitdb3.id],
            DirectChannel.getTopic([orbitdb2.id, orbitdb3.id])
        );
        await waitForPeers(
            session.peers[2],
            [orbitdb2.id],
            DirectChannel.getTopic([orbitdb2.id, orbitdb3.id])
        );
        await waitFor(() => orbitdb2._directConnections.size === 1);
        await waitFor(() => orbitdb3._directConnections.size === 1);

        expect(orbitdb1._directConnections.size).toEqual(0);
        // One leader
        const slot = 0;

        // Two leaders, but only one will be leader since only one is replicating
        const isLeaderA = orbitdb1.isLeader(
            await orbitdb1.findLeaders(topic, db1.address!.toString(), slot, 3)
        );
        const isLeaderB = orbitdb2.isLeader(
            await orbitdb2.findLeaders(topic, db1.address!.toString(), slot, 3)
        );
        const isLeaderC = orbitdb3.isLeader(
            await orbitdb3.findLeaders(topic, db1.address!.toString(), slot, 3)
        );

        expect(!isLeaderA); // because replicate is false
        expect(isLeaderB);
        expect(isLeaderC);
    });

    it("select leaders for three peers", async () => {
        // TODO fix test timeout, isLeader is too slow as we need to wait for peers
        // perhaps do an event based get peers using the pubsub peers api

        const topic = uuid();
        db1 = await orbitdb1.open(
            new EventStore<string>({ id: "replication-tests" }),
            { topic: topic }
        );
        db2 = await orbitdb2.open<EventStore<string>>(db1.address!, {
            topic: topic,
        });
        db3 = await orbitdb3.open<EventStore<string>>(db1.address!, {
            topic: topic,
        });

        await waitFor(() => orbitdb1._directConnections.size === 2);
        await waitFor(() => orbitdb2._directConnections.size === 2);
        await waitFor(() => orbitdb3._directConnections.size === 2);

        // One leader
        const slot = 0;

        const isLeaderAOneLeader = orbitdb1.isLeader(
            await orbitdb1.findLeaders(topic, db1.address!.toString(), slot, 1)
        );
        const isLeaderBOneLeader = orbitdb2.isLeader(
            await orbitdb2.findLeaders(topic, db1.address!.toString(), slot, 1)
        );
        const isLeaderCOneLeader = orbitdb3.isLeader(
            await orbitdb3.findLeaders(topic, db1.address!.toString(), slot, 1)
        );
        expect([
            isLeaderAOneLeader,
            isLeaderBOneLeader,
            isLeaderCOneLeader,
        ]).toContainValues([false, false, true]);

        // Two leaders
        const isLeaderATwoLeaders = orbitdb1.isLeader(
            await orbitdb1.findLeaders(topic, db1.address!.toString(), slot, 2)
        );
        const isLeaderBTwoLeaders = orbitdb2.isLeader(
            await orbitdb2.findLeaders(topic, db1.address!.toString(), slot, 2)
        );
        const isLeaderCTwoLeaders = orbitdb3.isLeader(
            await orbitdb3.findLeaders(topic, db1.address!.toString(), slot, 2)
        );
        expect([
            isLeaderATwoLeaders,
            isLeaderBTwoLeaders,
            isLeaderCTwoLeaders,
        ]).toContainValues([false, true, true]);

        // Three leders
        const isLeaderAThreeLeaders = orbitdb1.isLeader(
            await orbitdb1.findLeaders(topic, db1.address!.toString(), slot, 3)
        );
        const isLeaderBThreeLeaders = orbitdb2.isLeader(
            await orbitdb2.findLeaders(topic, db1.address!.toString(), slot, 3)
        );
        const isLeaderCThreeLeaders = orbitdb3.isLeader(
            await orbitdb3.findLeaders(topic, db1.address!.toString(), slot, 3)
        );
        expect([
            isLeaderAThreeLeaders,
            isLeaderBThreeLeaders,
            isLeaderCThreeLeaders,
        ]).toContainValues([true, true, true]);
    });
});
