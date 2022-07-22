declare module "orbit-db-feedstore" {
    import { Store } from '@dao-xyz/orbit-db-store';

    export default class FeedStore<T> extends Store<any, any> {
        add(data: any): Promise<string>;
        get(hash: string): LogEntry

        remove(hash: string): Promise<string>;

        iterator(options?: {
            gt?: string,
            gte?: string,
            lt?: string,
            lte?: string,
            limit?: number,
            reverse?: boolean
        }): {
            [Symbol.iterator](): Iterator<LogEntry>,
            next(): { value: LogEntry, done: boolean },
            collect(): LogEntry[]
        };
    }
}
