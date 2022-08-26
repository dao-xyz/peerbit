
import { U8IntArraySerializer } from '..';
import { field, serialize, deserialize } from '@dao-xyz/borsh';

describe('U8IntArraySerializer', () => {
  it('ser/der', async () => {
    class Test {

      @field(U8IntArraySerializer)
      a: Uint8Array;

      @field(U8IntArraySerializer)
      b: Uint8Array

    }

    const der = deserialize(Buffer.from(serialize(Object.assign(new Test(), {
      a: new Uint8Array([1, 2, 3]),
      b: new Uint8Array([4, 5, 6])
    }))), Test)

    expect(der.a).toStrictEqual(new Uint8Array([1, 2, 3]))
    expect(der.b).toStrictEqual(new Uint8Array([4, 5, 6]))
  })
}) 