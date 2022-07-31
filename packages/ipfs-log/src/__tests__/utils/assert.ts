
const assert = require('assert')
export const assertPayload = (value: string | Uint8Array, compare: string | Uint8Array) => {


    if (typeof compare === 'string')
        assert.strictEqual(Buffer.from(value).toString(), compare)
    else {
        assert.strictEqual(value, compare)
    }
}