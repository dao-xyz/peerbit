export class ReplicationInfo {
    progress: bigint;
    max: bigint;
    constructor() {
        this.progress = 0n;
        this.max = 0n;
    }

    reset() {
        this.progress = 0n;
        this.max = 0n;
    }
}
