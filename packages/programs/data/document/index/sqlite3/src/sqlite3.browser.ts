import sqlitePromise from "@sqlite.org/sqlite-wasm";
import { type Database as SQLDatabase, type PreparedStatement as SQLStatement } from "@sqlite.org/sqlite-wasm";
import { type Database as IDatabase, type Statement as IStatement } from "./types.js";
import type { BindableValue } from "./schema.js";

const log = (...args: any) => console.log(...args);
const error = (...args: any) => console.error(...args);
const sqlite = await sqlitePromise({ print: log, printErr: error });



class Statement implements IStatement {

    constructor(private statement: SQLStatement) {
    }

    bind(values: any[]) {
        return this.statement.bind(values);
    }

    finalize() {
        return this.statement.finalize();
    }

    get(values: BindableValue[]) {
        this.statement.bind(values as any);
        let step = this.statement.step();
        if (!step) { // no data available
            this.statement.reset();
            return undefined;
        }
        const results = this.statement.get({})
        this.statement.reset();
        return results;
    }

    run(values: BindableValue[]) {
        this.statement.bind(values as any);
        this.statement.stepReset();
    }

    reset() {
        return this.statement.reset();
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

}

class Database implements IDatabase {

    statements: Map<string, SQLStatement> = new Map();
    constructor(private db: SQLDatabase) {
    }

    async exec(sql: string) {
        return this.db.exec(sql);
    }

    async prepare(sql: string) {
        const statement = this.db.prepare(sql);
        this.statements.set(sql, statement)
        const wrappedStatement = new Statement(statement);
        return wrappedStatement
    }

    async close() {
        return this.db.close();
    }

    async get(sql: string) {
        return this.db.exec({ sql, rowMode: 'array' });
    }

    async run(sql: string, bind: any[]) {
        return this.db.exec(sql, { bind, rowMode: 'array' });
    }
}
const createDatabase = async (directory?: string) => {
    return new Database(new sqlite.oo1.DB(directory ?? ':memory:', 'c'));
}

export default { createDatabase };
