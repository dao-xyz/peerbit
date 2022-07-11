declare module "orbit-db-counterstore" {
    import { Store } from '@dao-xyz/orbit-db-store';

    export default class CounterStore extends Store<any, any, any> {
        value: number;

        inc(value?: number): Promise<string>;
    }
}