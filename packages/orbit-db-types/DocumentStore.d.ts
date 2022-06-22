declare module "orbit-db-docstore" {
    import { Store } from '@dao-xyz/orbit-db-store';

    export default class DocumentStore<T> extends Store<any, any> {

        put(doc: T): Promise<string>;
        get(key: any): T;

        query(mapper: (doc: T) => boolean): T[]

        del(key: any): Promise<string>;

    }
}
