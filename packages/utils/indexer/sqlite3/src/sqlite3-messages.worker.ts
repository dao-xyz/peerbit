import { fromBase64, toBase64 } from "@peerbit/crypto"

interface Message { id: string, databaseId: string }

// Database messages
interface CreateDatabase extends Message {
    type: 'create',
    directory?: string
}

interface Exec extends Message {
    type: 'exec'
    sql: string
}

interface Status extends Message {
    type: 'status'
}


interface Close extends Message {
    type: 'close'
}

interface Open extends Message {
    type: 'open'
}


interface Prepare extends Message {
    type: 'prepare'
    sql: string
}

type Uint8ArrayType = { type: 'uint8array', base64: string }

type SimpleType = { type: 'simple', value: any }

export const resolveValue = (value: Uint8ArrayType | SimpleType) => value.type === 'simple' ? value.value : fromBase64(value.base64)
export const encodeValue = (value: any): Uint8ArrayType | SimpleType => {
    if (value instanceof Uint8Array) {
        return { type: 'uint8array', base64: toBase64(value) }
    }
    return { type: 'simple', value }

}

interface Run extends Statement {
    type: 'run'
    sql: string,
    values: (Uint8ArrayType | SimpleType)[]
}




// Statement messages
interface Statement extends Message {
    statementId: string
}


interface Bind extends Statement {
    type: 'bind'
    values: (Uint8ArrayType | SimpleType)[]
}

interface Step extends Statement {
    type: 'step'
}

interface Get extends Statement {
    type: 'get',
    values?: any[]
}

interface Reset extends Statement {
    type: 'reset'
}

interface RunStatement extends Statement {
    type: 'run-statement'
    values: any[]
}



interface All extends Statement {
    type: 'all'
    values: (Uint8ArrayType | SimpleType)[]
}

interface Finalize extends Statement {
    type: 'finalize'

}

// Response messages
interface ErrorResponse {
    type: 'error'
    id: string
    message: string
}

interface Response {
    type: 'response'
    id: string
    result: any
}







export type DatabaseMessages = CreateDatabase | Exec | Prepare | Close | Open | Run | Status
export type StatementMessages = Bind | Step | Get | Reset | All | Finalize | RunStatement
export type ResponseMessages = ErrorResponse | Response

export type IsReady = { type: 'ready' }