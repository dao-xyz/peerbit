import type { BindableValue, SQLLiteValue } from "./schema.js"

export type SQLite = {
    create: (directory?: string) => Promise<Database> | Database,
}

export type Database = {
    exec: (sql: string) => Promise<any> | any
    prepare: (sql: string, err?: (err: any) => any) => Promise<Statement> | Statement,
    close: (err?: (err: any) => any) => Promise<any> | any
    open(): Promise<any> | any
    status: () => Promise<'open' | 'closed'> | 'open' | 'closed'

}

export type StatementGetResult = { [key: string]: SQLLiteValue }

export type Statement = {

    bind: (values: BindableValue[], err?: (err: any) => any) => Promise<Statement> | Statement
    finalize?: (err?: (err: any) => any) => Promise<void> | void
    get: (values?: BindableValue[], err?: (err: any, row: any) => any) => Promise<StatementGetResult> | StatementGetResult
    run: (values: BindableValue[], err?: (err: any) => any) => Promise<void> | void
    reset?: (err?: (err: any) => any) => Promise<Statement> | Statement,
    all: (values: BindableValue[], err?: (err: any, rows: any[]) => any) => Promise<any> | any

}