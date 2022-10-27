import { Peerbit } from "../../peer";
import { EventStore } from "./stores";
// @ts-ignore 
import { v4 as uuid } from 'uuid';
export const databases = [
    {
        type: 'eventstore',
        create: (orbitdb: Peerbit, id: string) => orbitdb.open(new EventStore({ id: 'xyz1' }), uuid()),
        tryInsert: (db: EventStore<any>) => db.add('hello'),
        getTestValue: (db: EventStore<any>) => db.iterator().next().value?.payload.getValue().value as string,
        expectedValue: 'hello'

    }
]