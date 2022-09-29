
const assert = require('assert')
export const assertPayload = (value: string | Uint8Array, compare: string | Uint8Array) => {


    if (typeof compare === 'string')
        expect(Buffer.from(value).toString()).toEqual(compare)
    else {
        expect(value).toEqual(compare)
    }
}