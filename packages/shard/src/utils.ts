import Store from "orbit-db-store";
const MAX_REPLICATION_WAIT_TIME = 15 * 1000;

export const delay = (ms: number) => new Promise(res => setTimeout(res, ms));
export const waitFor = async (fn: () => boolean, timeout: number = 60 * 1000) => {

    let startTime = +new Date;
    while (+new Date - startTime < timeout) {
        if (fn()) {
            return;
        }
        await delay(50);
    }
    throw new Error("Timed out")

};



export const waitForReplicationEvents = async (store: Store<any, any>, waitForReplicationEventsCount: number) => {
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