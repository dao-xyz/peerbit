import { field, vec } from "@dao-xyz/borsh";
import {
    SearchRequest,
    type IndexEngineInitProperties,
    type Index,
    type Indices,
    id,
    getIdProperty,
    BoolQuery
} from "@peerbit/indexer-interface";
import { v4 as uuid } from "uuid";
import sodium from "libsodium-wrappers";
import B from "benchmark";
import pDefer from "p-defer"

const setup = async <T>(
    properties: Partial<IndexEngineInitProperties<T, any>> & { schema: any },
    createIndicies: (directory?: string) => Indices | Promise<Indices>,
    type: 'transient' | 'persist' = 'transient'
): Promise<{ indices: Indices, store: Index<T, any>, directory?: string }> => {
    await sodium.ready;
    let directory = type === 'persist' ? ('./tmp/document-index/' + uuid()) : undefined;
    const indices = await createIndicies(directory);
    await indices.start()
    const indexProps: IndexEngineInitProperties<T, any> = {
        ...{
            indexBy: getIdProperty(properties.schema) || ["id"],
            iterator: { batch: { maxSize: 5e6, sizeProperty: ["__size"] } },
        },
        ...properties
    }
    const store = await indices.init(indexProps);
    return { indices, store, directory };
};

const boolQueryBenchmark = async (createIndicies: (directory?: string) => Indices | Promise<Indices>, type: 'transient' | 'persist' = 'transient') => {

    class BoolQueryDocument {

        @id({ type: 'string' })
        id: string;

        @field({ type: 'bool' })
        bool: boolean;

        constructor(id: string, bool: boolean) {
            this.id = id
            this.bool = bool;
        }
    }

    const fs = await import('fs');


    const boolIndexPrefilled = await setup({ schema: BoolQueryDocument }, createIndicies, type);
    let docCount = 1e4;
    for (let i = 0; i < docCount; i++) {
        await boolIndexPrefilled.store.put(new BoolQueryDocument(uuid(), Math.random() > 0.5 ? true : false))
    }

    const boolIndexEmpty = await setup({ schema: BoolQueryDocument }, createIndicies, type);

    let done = pDefer()
    const suite = new B.Suite({ delay: 100 });
    suite
        .add("bool query - " + type, {
            fn: async (deferred: any) => {
                const out = Math.random() > 0.5 ? true : false
                await boolIndexPrefilled.store.query(new SearchRequest({ query: new BoolQuery({ key: 'bool', value: out }) }))
                deferred.resolve()
            },
            defer: true,
            maxTime: 5,
        })
        .add("bool put - " + type, {
            fn: async (deferred: any) => {
                await boolIndexEmpty.store.put(new BoolQueryDocument(uuid(), Math.random() > 0.5 ? true : false))
                deferred.resolve()
            },
            defer: true,
            maxTime: 5,
        })
        .on("cycle", async (event: any) => {
            console.log(String(event.target));
        })
        .on("error", (err: any) => {
            throw err;
        })
        .on("complete", async () => {

            await boolIndexEmpty.indices.stop()
            boolIndexEmpty.directory && fs.rmSync(boolIndexEmpty.directory, { recursive: true, force: true })

            await boolIndexPrefilled.indices.stop()
            boolIndexPrefilled.directory && fs.rmSync(boolIndexPrefilled.directory, { recursive: true, force: true })

            done.resolve()
        })
        .on("error", (e) => {
            done.reject(e)
        })
        .run();
    return done.promise;

}


const nestedBoolQueryBenchmark = async (createIndicies: (directory?: string) => Indices | Promise<Indices>, type: 'transient' | 'persist' = 'transient') => {
    class Nested {
        @field({ type: 'bool' })
        bool: boolean;

        constructor(bool: boolean) {
            this.bool = bool;
        }
    }

    class NestedBoolQueryDocument {

        @id({ type: 'string' })
        id: string;

        @field({ type: Nested })
        nested: Nested

        constructor(id: string, bool: boolean) {
            this.id = id
            this.nested = new Nested(bool)
        }
    }


    const fs = await import('fs');


    const boolIndexPrefilled = await setup({ schema: NestedBoolQueryDocument }, createIndicies, type);

    let docCount = 1e4;
    for (let i = 0; i < docCount; i++) {
        await boolIndexPrefilled.store.put(new NestedBoolQueryDocument(uuid(), i % 2 === 0 ? true : false))
    }


    const boolIndexEmpty = await setup({ schema: NestedBoolQueryDocument }, createIndicies, type);


    let done = pDefer()
    const suite = new B.Suite({ delay: 100 });

    suite
        .add("nested bool query - " + type, {
            fn: async (deferred: any) => {
                const out = Math.random() > 0.5 ? true : false
                await boolIndexPrefilled.store.query(new SearchRequest({ query: new BoolQuery({ key: ['nested', 'bool'], value: out }), fetch: 10 }))
                deferred.resolve()
            },
            defer: true,
            maxTime: 5,
            async: true,
        })
        .add("nested bool put - " + type, {
            fn: async (deferred: any) => {
                await boolIndexEmpty.store.put(new NestedBoolQueryDocument(uuid(), Math.random() > 0.5 ? true : false))
                deferred.resolve()
            },
            defer: true,
            maxTime: 5,
            async: true
        })
        .on("cycle", async (event: any) => {
            console.log(String(event.target));
        })
        .on("error", (err: any) => {
            done.reject(err)
        })
        .on("complete", async () => {
            await boolIndexEmpty.indices.stop()
            boolIndexEmpty.directory && fs.rmSync(boolIndexEmpty.directory, { recursive: true, force: true })

            await boolIndexPrefilled.indices.stop()
            boolIndexPrefilled.directory && fs.rmSync(boolIndexPrefilled.directory, { recursive: true, force: true })
            done.resolve();
        })
        .run();
    return done.promise;


}

const shapedQueryBenchmark = async (createIndicies: (directory?: string) => Indices | Promise<Indices>, type: 'transient' | 'persist' = 'transient') => {
    class Nested {
        @field({ type: 'bool' })
        bool: boolean;

        constructor(bool: boolean) {
            this.bool = bool;
        }
    }

    class NestedBoolQueryDocument {

        @id({ type: 'string' })
        id: string;

        @field({ type: vec(Nested) })
        nested: Nested[]

        constructor(id: string, nested: Nested[]) {
            this.id = id
            this.nested = nested
        }
    }


    const fs = await import('fs');


    const boolIndexPrefilled = await setup({ schema: NestedBoolQueryDocument }, createIndicies, type);

    let docCount = 1e4;
    for (let i = 0; i < docCount; i++) {
        await boolIndexPrefilled.store.put(new NestedBoolQueryDocument(uuid(), [new Nested(i % 2 === 0 ? true : false)]))
    }



    let done = pDefer()
    const suite = new B.Suite({ delay: 100 });
    let fetch = 10;
    suite
        .add("unshaped - " + type, {
            fn: async (deferred: any) => {
                const out = Math.random() > 0.5 ? true : false
                const results = await boolIndexPrefilled.store.query(new SearchRequest({ query: new BoolQuery({ key: ['nested', 'bool'], value: out }), fetch: fetch }))
                if (results.results.length !== fetch) {
                    throw new Error("Missing results")
                }
                deferred.resolve()
            },
            defer: true,
            maxTime: 5,
            async: true,
        })
        .add("shaped - " + type, {
            fn: async (deferred: any) => {
                const out = Math.random() > 0.5 ? true : false
                const results = await boolIndexPrefilled.store.query(new SearchRequest({ query: new BoolQuery({ key: ['nested', 'bool'], value: out }), fetch: fetch }), { shape: { id: true } })
                if (results.results.length !== fetch) {
                    throw new Error("Missing results")
                }
                deferred.resolve()
            },
            defer: true,
            maxTime: 5,
            async: true
        })
        .on("cycle", async (event: any) => {
            console.log(String(event.target));
        })
        .on("error", (err: any) => {
            done.reject(err)
        })
        .on("complete", async () => {
            await boolIndexPrefilled.indices.stop()
            boolIndexPrefilled.directory && fs.rmSync(boolIndexPrefilled.directory, { recursive: true, force: true })
            done.resolve();
        })
        .run();
    return done.promise;


}



export const benchmarks = async (createIndicies: <T> (directory?: string) => Indices | Promise<Indices>, type: 'transient' | 'persist' = 'transient') => {
    await shapedQueryBenchmark(createIndicies, type)
    await boolQueryBenchmark(createIndicies, type)
    await nestedBoolQueryBenchmark(createIndicies, type)
}
