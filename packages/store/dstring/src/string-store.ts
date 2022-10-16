import { PayloadOperation, StringIndex, encoding } from './string-index.js'
import { QueryStore, QueryStoreInitializationOptions } from '@dao-xyz/orbit-db-query-store';
import { QueryRequestV0, RangeCoordinate, RangeCoordinates, Result, ResultWithSource, StringMatchQuery } from '@dao-xyz/query-protocol';
import { StringQueryRequest } from '@dao-xyz/query-protocol';
import { Range } from './range.js';
import { deserialize, field, serialize, variant } from '@dao-xyz/borsh';
import { CustomBinaryPayload } from '@dao-xyz/bpayload';
import { Address, IInitializationOptions, IStoreOptions, load, Store } from '@dao-xyz/peerbit-dstore';
import { IPFS } from 'ipfs-core-types';
import { Identity } from '@dao-xyz/ipfs-log';

export const STRING_STORE_TYPE = 'string_store';
const findAllOccurrences = (str: string, substr: string): number[] => {
  str = str.toLowerCase();

  let result = [];

  let idx = str.indexOf(substr)

  while (idx !== -1) {
    result.push(idx);
    idx = str.indexOf(substr, idx + 1);
  }
  return result;
}


@variant([0, 1])
export class StringStore extends QueryStore<PayloadOperation> {

  _index: StringIndex;
  constructor(properties: { name?: string, queryRegion?: string }) {
    super(properties)
    this._index = new StringIndex();
  }

  init(ipfs: IPFS<{}>, identity: Identity, options: QueryStoreInitializationOptions<PayloadOperation>): Promise<this> {
    return super.init(ipfs, identity, { ...options, encoding, onUpdate: this._index.updateIndex.bind(this._index) })
  }
  add(value: string, index: Range, options = {}) {
    return this._addOperation(new PayloadOperation({
      index,
      value,
    }), options)
  }

  del(index: Range, options = {}) {
    const operation = {
      index
    } as PayloadOperation
    return this._addOperation(operation, options)
  }

  async queryHandler(query: QueryRequestV0): Promise<Result[]> {
    if (query.type instanceof StringQueryRequest == false) {
      return [];
    }
    const stringQuery = query.type as StringQueryRequest;

    const content = this._index.string;
    const relaventQueries = stringQuery.queries.filter(x => x instanceof StringMatchQuery) as StringMatchQuery[]
    if (relaventQueries.length == 0) {
      return [new ResultWithSource({
        source: new StringResultSource({
          string: content
        })
      })]
    }
    let ranges = relaventQueries.map(query => {
      const occurances = findAllOccurrences(query.preprocess(content), query.preprocess(query.value));
      return occurances.map(ix => {
        return new RangeCoordinate({
          offset: BigInt(ix),
          length: BigInt(query.value.length)
        })
      })

    }).flat(1);

    if (ranges.length == 0) {
      return [];
    }

    return [new ResultWithSource({
      source: new StringResultSource({
        string: content,
      }),
      coordinates: new RangeCoordinates({
        coordinates: ranges
      })
    })];
  }

  static async load(ipfs: IPFS, address: Address, options?: {
    timeout?: number;
  }): Promise<StringStore> {
    const instance = await load(ipfs, address, Store, options)
    if (instance instanceof StringStore === false) {
      throw new Error("Unexpected")
    };
    return instance as StringStore;
  }
}

@variant("string")
/* @variant([0, 2]) */
export class StringResultSource extends CustomBinaryPayload {

  @field({ type: 'string' })
  string: string

  constructor(prop?: {
    string: string;
  }) {
    super();
    if (prop) {
      Object.assign(this, prop);
    }
  }
}


