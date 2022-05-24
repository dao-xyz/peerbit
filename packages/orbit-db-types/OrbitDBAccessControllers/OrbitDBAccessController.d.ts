
declare module "orbit-db-access-controllers/src/orbitdb-access-controller" {
    import AccessController from "orbit-db-access-controllers/src/access-controller-interface"
    import OrbitDB from "orbit-db"

    export default class OrbitDBAccessController extends AccessController {
        constructor (orbitdb: OrbitDB, options: any)

        // Returns the type of the access controller
        static get type (): string

        // Returns the address of the OrbitDB used as the AC
        get address (): string

        // Return true if entry is allowed to be added to the database
        canAppend (entry: LogEntry<any>, identityProvider: any): Promise<boolean>

        get capabilities (): {[key: string]: Set<any>}
        get (capability: string): Set<any>

        close (): Promise<void>

        load (address: string): Promise<void>

        save(): Promise<{ address: string }>

        grant (capability: string, key: string): Promise<void>

        revoke (capability: string, key: string): Promise<void>

        /* Private methods */
        _onUpdate (): void

        /* Factory */
        static create (orbitdb: OrbitDB, options: any): Promise<OrbitDBAccessController>
    }

}