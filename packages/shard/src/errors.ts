export class MemoryLimitExceededError extends Error {
    constructor(message?: string) {
        super(message);
    }
}