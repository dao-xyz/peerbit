import { delay, waitFor } from "@dao-xyz/time";
import { Store } from '@dao-xyz/orbit-db-store';
const MAX_REPLICATION_WAIT_TIME = 15 * 1000;


export const waitForReplicationEvents = async (store: Store<any, any, any>, waitForReplicationEventsCount: number) => {
    /*
        * This method is flaky
        * First we check the progress of replicatoin
        * then we check a custom replicated boolean, as the replicationStatus
        * is not actually tracking whether the store is loaded
    */

    if (!waitForReplicationEventsCount)
        return

    await waitFor(() => !!store.replicationStatus && waitForReplicationEventsCount <= store.replicationStatus.max)

    let startTime = +new Date;
    while (store.replicationStatus.progress < store.replicationStatus.max) {
        await delay(50);
        if (+new Date - startTime > MAX_REPLICATION_WAIT_TIME) {
            console.warn("Max replication time, aborting wait for")
            return;
        }
    }
    // await waitFor(() => store["replicated"])
    return;
} 