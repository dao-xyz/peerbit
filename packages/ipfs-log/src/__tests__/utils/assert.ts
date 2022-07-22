
const assert = require('assert')
export const assertPayload = (binary: Uint8Array, compare: string | Uint8Array) => {

    if (typeof compare === 'string')
        assert.strictEqual(Buffer.from(binary).toString(), compare)
    else {
        assert.strictEqual(binary, compare)
    }
}