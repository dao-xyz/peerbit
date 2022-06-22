declare module "orbit-db-eventstore" {
    import { Store } from '@dao-xyz/orbit-db-store';

    export default class EventStore<T> extends Store<any, any> {
        add(data: any): Promise<string>;
        get(hash: string): LogEntry<T>;
        iterator(options?: {
            gt?: string,
            gte?: string,
            lt?: string,
            lte?: string,
            limit?: number,
            reverse?: boolean
        }): {
            [Symbol.iterator](): Iterator<LogEntry<T>>,
            next(): { value: LogEntry<T>, done: boolean },
            collect(): LogEntry<T>[]
        };
    }
}
