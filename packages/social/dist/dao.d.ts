/**
 * A decentralized storage of DAO meta data info.
 * The nodes governing this storage are part of a "official" set of nodes
 * These nodes are trusted by the dao.xyz.dao to act truthfully to serve
 * the wider community a way of creating, modifying, deleting and searching
 * DAOs (organizations/communities/groups)
 */
/// <reference types="dao-xyz-orbit-db" />
import { ShardedDB, ShardChain } from '@dao-xyz/node';
import { BinaryDocumentStore } from '@dao-xyz/orbit-db-bdocstore';
import { Identity } from 'orbit-db-identity-provider';
export declare class DAO {
    name: string;
}
export declare class DaoDB extends ShardedDB {
    db: ShardedDB;
    daos: ShardChain<BinaryDocumentStore<DAO>>;
    constructor();
    create(options?: {
        rootId: string;
        local: boolean;
        identity?: Identity;
    }): Promise<void>;
    support(): Promise<void>;
}
export declare const getDAOs: () => void;
