import { BlockStore, LevelBlockStore } from './store.js';
import { MemoryLevel } from 'memory-level';
export * from './store.js';
export * from './transport.js';

export class BlockClient {
    _store: BlockStore

    constructor(store: BlockStore = new LevelBlockStore(new MemoryLevel())) {
        this._store = store;
    }

    close(): Promise<void> {
        return this._store.close();
    }

    get()
}