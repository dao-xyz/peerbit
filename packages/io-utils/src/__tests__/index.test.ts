
import { arraysCompare, U8IntArraySerializer } from '..';
import { field, serialize, deserialize } from '@dao-xyz/borsh';

describe('U8IntArraySerializer', () => {
  it('ser/der', () => {
    class Test {

      @field(U8IntArraySerializer)
      a: Uint8Array;

      @field(U8IntArraySerializer)
      b: Uint8Array

    }

    const der = deserialize(serialize(Object.assign(new Test(), {
      a: new Uint8Array([1, 2, 3]),
      b: new Uint8Array([4, 5, 6])
    })), Test)

    expect(der.a).toStrictEqual(new Uint8Array([1, 2, 3]))
    expect(der.b).toStrictEqual(new Uint8Array([4, 5, 6]))
  })

  it('array compare', () => {
    const a = new Uint8Array([1]);
    const b = new Uint8Array([1, 2]);
    const c = new Uint8Array([2, 2]);
    const d = new Uint8Array([3]);
    const e = new Uint8Array([1, 1, 1]);
    const arrays = [a, b, c, d, e]
    const expectedSorted = [a, e, b, c, d];
    arrays.sort(arraysCompare);
    expect(arrays).toEqual(expectedSorted)

  })
}) 
