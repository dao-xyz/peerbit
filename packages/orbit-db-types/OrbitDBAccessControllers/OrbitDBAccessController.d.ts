import Identities from "@dao-xyz/orbit-db-identity-provider";

declare module "orbit-db-access-controllers/src/orbitdb-access-controller" {
    import AccessController from "orbit-db-access-controllers/src/access-controller-interface"
    import OrbitDB from "orbit-db"
    import { AccessController } from '@dao-xyz/ipfs-log';

    export default class OrbitDBAccessController<T> implements AccessController<T> {
        constructor(orbitdb: OrbitDB, options: any)

        // Returns the type of the access controller
        static get type(): string

        // Returns the address of the OrbitDB used as the AC
        get address(): string?;

        // Return true if entry is allowed to be added to the database
        close?(): Promise<void>;

        load?(address: string): Promise<void>;

        canAppend?(entry: Entry<T>, identityProvider: Identities): Promise<void>;

        save?(): Promise<{ address: string }>;

        /*       grant(capability: string, key: string): Promise<void>
      
              revoke(capability: string, key: string): Promise<void> */

        /* Factory */
        static create(orbitdb: OrbitDB, options: any): Promise<OrbitDBAccessController>
    }

}