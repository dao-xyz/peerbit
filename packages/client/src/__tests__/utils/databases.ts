import { OrbitDB } from "../../orbit-db";
import { SimpleAccessController } from "./access";
import { EventStore } from "./stores";

export const databases = [
    {
        type: 'eventstore',
        create: (orbitdb: OrbitDB, name: string) => orbitdb.open(new EventStore({ name: 'xyz1', accessController: new SimpleAccessController() })),
        tryInsert: (db: EventStore<any>) => db.add('hello'),
        getTestValue: (db: EventStore<any>) => db.iterator().next().value?.payload.getValue().value as string,
        expectedValue: 'hello'

    }
]