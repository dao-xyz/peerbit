/// <reference path="./DBOptions.d.ts" />
/// <reference path="./LogEntry.d.ts" />
declare module 'orbit-db' {
    import { Store } from '@dao-xyz/orbit-db-store';
    import { Keystore } from "orbit-db-keystore";
    import Cache from "@dao-xyz/orbit-db-cache";
    import { Identity } from "@dao-xyz/orbit-db-identity-provider";
    import * as IPFS from "ipfs";
    import * as elliptic from "elliptic";
    import OrbitDBAddress from 'orbit-db'
    export class OrbitDB {

        _ipfs: IPFS;
        _pubsub: {
            _subscriptions: { [key: string]: any }
            subscribe: (topic: string, onMessageCallback: (address: string, heads, peer) => void, onNewPeerCallback: (address: string, peer: any) => void, options = {}) => Promise<void>
            unsubscribe: (hash: string) => Promise<void>

        };
        _onMessage(address: string, heads, peer): Promise<void>
        _onPeerConnected(address: string, peer): Promise<void>
        _directConnections: { [key: string]: { close(): void } }

        id: string;
        identity: Identity;
        stores: any;
        directory: string;
        keystore: Keystore;

        // For OpenTelemetry Plugin
        span?: any;

        static databaseTypes: string[];


        constructor(ipfs: IPFS, directory?: string, options?: {
            peerId?: string,
            keystore?: Keystore
        });

        /**
         * Creates and returns an instance of OrbitDB. 
         * @param ipfs 
         * @param options Other options: 
         * <ul>
         * <li>directory (string): path to be used for the database files. By default it uses './orbitdb'.</li>
         * <li>peerId (string): By default it uses the base58 string of the ipfs peer id.</li>
         * <li>keystore (Keystore Instance) : By default creates an instance of Keystore.</li>
         * <li>cache (Cache Instance) : By default creates an instance of Cache. A custom cache instance can also be used.</li>
         * <li>identity (Identity Instance): By default it creates an instance of Identity</li>
         * </ul>
         */
        static createInstance(ipfs: IPFS, options?: {
            AccessControllers?: any,
            directory?: string,
            peerId?: string,
            keystore?: Keystore,
            cache?: Cache,
            identity?: Identity
            broker?: any,
        }): Promise<OrbitDB>

        create(name: string, type: TStoreType, options?: ICreateOptions): Promise<Store>;

        open<T extends Store>(address: string, options?: IOpenOptions): Promise<T>;

        disconnect(): Promise<void>;
        stop(): Promise<void>;
        determineAddress(name: string, type: TStoreType, options?: ICreateOptions): Promise<OrbitDBAddress>

        static isValidType(type: TStoreType): boolean;
        static addDatabaseType(type: string, store: typeof Store): void;
        static getDatabaseTypes(): {};
        static isValidAddress(address: string): boolean;
    }

    export default OrbitDB;
}
