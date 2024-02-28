import { PublicSignKey } from "@peerbit/crypto";
import { IndexEngine, IndexedResult, IndexedResults } from "./index-engine";
import {
	Context,
	SearchRequest,
	CollectNextRequest,
	CloseIteratorRequest
} from "./query.js";
import { IndexKeyPrimitiveType } from "./types.js";
import sqlite3InitModule from "@sqlite.org/sqlite-wasm";
/*
export class SQLLiteEngine<T> extends IndexEngine<T> {
   sqlInstance: any;
   db: any;

   constructor(properties: { canRead?: (value: T, publicKey: PublicSignKey) => Promise<boolean> | boolean; resolve: (hash: string) => T; }) {
       super(properties);

   }
   async start(): Promise<void> {
       this.sqlInstance = await sqlite3InitModule({
           print: console.log.bind(console),
           printErr: console.error.bind(console),
       })
       this.db = new this.sqlInstance.oo1.DB('/mydb.sqlite3', 'ct');
       this.db.run("CREATE TABLE test (id INTEGER, value TEXT)");

   }
   stop(): Promise<void> | void {
       return this.sqlInstance.close();
   }


   put(id: IndexKeyPrimitiveType, value: { value: T | Record<string, any> | Promise<Record<string, any>>; context: Context; reference?: { value: T; size: number; } | undefined; }): Promise<void> {
       this.sqlInstance.run("INSERT INTO test (id, value) VALUES (?, ?)", [id, value]);


   }

  
    get(id: IndexKeyPrimitiveType): IndexedResult<T> | Promise<IndexedResult<T> | undefined> | undefined {
      throw new Error("Method not implemented.");
  } 
   del(id: IndexKeyPrimitiveType): Promise<void> {
       throw new Error("Method not implemented.");
   }
   query(query: SearchRequest, from: PublicSignKey): Promise<IndexedResults<T>> {
       throw new Error("Method not implemented.");
   }
   next(query: CollectNextRequest, from: PublicSignKey): Promise<IndexedResults<T>> {
       throw new Error("Method not implemented.");
   }
   close(query: CloseIteratorRequest, from: PublicSignKey): void | Promise<void> {
       throw new Error("Method not implemented.");
   }
   get size(): number {
       throw new Error("Method not implemented.");
   } 

}*/
