declare module "orbit-db-kvstore" {
    import Store from "orbit-db-store";

    export default class KeyValueStore<V> extends Store {
        get(key: string): V;

        put(key: string, value: V, options?: {}): Promise<string>;
        set(key: string, value: V, options?: {}): Promise<string>;
        
        del(key: string, options?: {}): Promise<string>;

        all: {[key: string]: V};
    }
}