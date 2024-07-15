import { default as sqlite3InitModule, type SAHPoolUtil, type OpfsSAHPoolDatabase } from "@sqlite.org/sqlite-wasm";
import { type Database as SQLDatabase, type PreparedStatement as SQLStatement } from "@sqlite.org/sqlite-wasm";
import { type Statement as IStatement, type StatementGetResult } from "./types.js";
import type { BindableValue } from "./schema.js";
import { BinaryWriter, BinaryReader } from '@dao-xyz/borsh';
import { fromBase64URL, toBase64URL } from "@peerbit/crypto";

/* import { v4 as uuid } from 'uuid';
 */
export const encodeName = (name: string): string => {
    // since "/" and perhaps other characters might not be allowed we do encode
    const writer = new BinaryWriter();
    writer.string(name);
    return toBase64URL(writer.finalize());
};

export const decodeName = (name: string): string => {
    // since "/" and perhaps other characters might not be allowed we do encode
    const writer = new BinaryReader(fromBase64URL(name));
    return writer.string();
};

class Statement implements IStatement {

    constructor(private statement: SQLStatement) { }

    async bind(values: any[]) {
        await this.statement.bind(values);
        return this;
    }

    async finalize() {
        if (await this.statement.finalize() > 0) {
            throw new Error('Error finalizing statement');
        }
    }

    get(values?: BindableValue[]) {
        if (values) {
            this.statement.bind(values);
        }
        let step = this.statement.step();
        if (!step) { // no data available
            this.statement.reset();
            return undefined;
        }
        const results = this.statement.get({})
        this.statement.reset();
        return results as StatementGetResult;
    }

    run(values: BindableValue[]) {
        this.statement.bind(values as any);
        this.statement.stepReset();
    }

    async reset() {
        await this.statement.reset();
        return this;
    }

    all(values: BindableValue[]) {

        if (values && values.length > 0) {
            this.statement.bind(values as any);
        }

        let results = [];
        while (this.statement.step()) {
            results.push(this.statement.get({}));
        }
        this.statement.reset();
        return results
    }

    step() {
        return this.statement.step();
    }

}

/* export class Database implements IDatabase {

    statements: Map<string, Statement> = new Map();
    private db: SQLDatabase
    constructor(private readonly _close?: () => Promise<any> | any) { }

    async exec(sql: string) {
        return this.db.exec(sql);
    }

    async prepare(sql: string) {
        const statement = this.db.prepare(sql);
        const wrappedStatement = new Statement(statement);
        this.statements.set(sql, wrappedStatement)
        return wrappedStatement
    }

    async close() {
        await Promise.all([...this.statements.values()].map(x => x.finalize?.()))
        await this.db.close();
        await this._close?.()
    }

    async get(sql: string) {
        return this.db.exec({ sql, rowMode: 'array' });
    }

    async run(sql: string, bind: any[]) {
        return this.db.exec(sql, { bind, rowMode: 'array' });
    }
}

 */
const log = (...args: any) => console.log(...args);
const error = (...args: any) => console.error(...args);


/* let initOpfsResult: Promise<{ sqlite3: Awaited<ReturnType<typeof sqlite3InitModule>>,poolUtil: }> | undefined = undefined;
const initOpfs = async () => {

    let sqlite3: Awaited<ReturnType<typeof sqlite3InitModule>> = await sqlite3InitModule({
        locateFile: (path, prefix) => {

            return path;
        }, print: log, printErr: error
    });
    let poolUtil = await sqlite3.installOpfsSAHPoolVfs({
        directory: encodeName("helloworld")
    });

    return initOpfsResult || (initOpfsResult = { sqlite3, poolUtil })
} */

let poolUtil: SAHPoolUtil = undefined;
let sqlite3: Awaited<ReturnType<typeof sqlite3InitModule>> | undefined = undefined;

const create = async (directory?: string) => {

    let statements: Map<string, Statement> = new Map();

    sqlite3 = sqlite3 || await sqlite3InitModule({ print: log, printErr: error });
    let sqliteDb: OpfsSAHPoolDatabase | SQLDatabase | undefined = undefined;
    let close: (() => Promise<any> | any) | undefined = async () => {
        await Promise.all([...statements.values()].map(x => x.finalize?.()))
        statements.clear();

        await sqliteDb?.close();
        sqliteDb = undefined
    }
    let open = async () => {
        if (sqliteDb) {
            return sqliteDb
        }
        if (directory) {

            // directory has to be absolute path. Remove leading dot if any
            // TODO show warning if directory is not absolute?
            directory = directory.replace(/^\./, "");

            let dbFileName = `${directory}/db.sqlite`;

            poolUtil = poolUtil || await sqlite3.installOpfsSAHPoolVfs({
                directory: "peerbit/sqlite" // encodeName("peerbit") 
            });

            await poolUtil.reserveMinimumCapacity(100);
            sqliteDb = new poolUtil.OpfsSAHPoolDb(dbFileName);

        }
        else {
            sqliteDb = new sqlite3.oo1.DB(':memory:')
        }

        sqliteDb.exec('PRAGMA journal_mode = WAL');
        sqliteDb.exec('PRAGMA foreign_keys = on');
    }


    return {
        close,
        exec: (sql: string) => {
            return sqliteDb.exec(sql);
        },
        open,
        prepare: (sql: string) => {
            const statement = sqliteDb.prepare(sql);
            const wrappedStatement = new Statement(statement);
            statements.set(sql, wrappedStatement)
            return wrappedStatement
        },
        get(sql: string) {
            return sqliteDb.exec({ sql, rowMode: 'array' });
        },

        run(sql: string, bind: any[]) {
            return sqliteDb.exec(sql, { bind, rowMode: 'array' });
        },
        status: () => sqliteDb?.isOpen() ? 'open' : 'closed',
        statements

    }
}

export { create };
