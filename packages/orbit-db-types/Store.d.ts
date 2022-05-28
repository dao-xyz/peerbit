
declare module "orbit-db-store" {
    import AccessController from "orbit-db-access-controllers/src/access-controller-interface";

    import IPFS = require("ipfs");
    import { Identity } from "orbit-db-identity-provider";
    import { EventEmitter } from 'events';
    import * as elliptic from "elliptic";
    import ipfs from "ipfs";

    export default class Store<T, X> {

        /**
         * The identity is used to sign the database entries.
         */
        readonly identity: Identity;

        address: { root: string, path: string };
        /** 
         * Contains all entries of this Store
         */
        type: string;
        id: string;

        /**
         * Returns an instance of `elliptic.ec.KeyPair`.
         * The keypair is used to sign the database entries.
         * The key can also be accessed from the OrbitDB instance: `orbitdb.key.getPublic('hex')`.
         */
        key: elliptic.ec.KeyPair;
        replicationStatus: IReplicationStatus;
        events: EventEmitter;
        access: AccessController
        _index: X;
        options: any;


        /**
         * Apparently not meant for outside usage
         * @param ipfs 
         * @param identity 
         * @param address 
         * @param options 
         */
        constructor(ipfs: IPFS, identity: Identity, address: string, options: IStoreOptions);

        close(): Promise<void>;
        drop(): Promise<void>;

        setIdentity(identity: Identity): void;
        saveSnapshot(): Promise<any>

        /**
         * Load the locally persisted database state to memory.
         * @param amount Amount of entries loaded into memory
         * @returns a `Promise` that resolves once complete
         */
        public load(amount?: number, opts = {}): Promise<void>;
        public get all(): T[];

        protected _addOperation(data: any, options: { onProgressCallback?: (entry: any) => any, pin?: boolean }): Promise<string>;
        protected _addOperationBatch(data: any, batchOperation, lastOperation, onProgressCallback): Promise<any>
    }
}
