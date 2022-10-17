import { PayloadOperation, StringIndex, encoding } from './string-index.js'
import { DSearch, DSearchInitializationOptions, QueryType } from '@dao-xyz/peerbit-dsearch';
import { RangeCoordinate, RangeCoordinates, Result, ResultWithSource, StringMatchQuery } from '@dao-xyz/peerbit-dsearch';
import { StringQueryRequest } from '@dao-xyz/peerbit-dsearch';
import { Range } from './range.js';
import { deserialize, field, serialize, variant } from '@dao-xyz/borsh';
import { CustomBinaryPayload } from '@dao-xyz/bpayload';
import { Address, IInitializationOptions, IStoreOptions, load, Store } from '@dao-xyz/peerbit-dstore';
import { IPFS } from 'ipfs-core-types';
import { Identity } from '@dao-xyz/ipfs-log';
import { SignatureWithKey } from '@dao-xyz/peerbit-crypto';

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
export class StringStore extends Store<PayloadOperation> {

  @field({ type: DSearch })
  search: DSearch<PayloadOperation>;

  _index: StringIndex;
  constructor(properties: { name?: string, search: DSearch<PayloadOperation> }) {
    super(properties)
    if (properties) {
      this.search = properties.search
    }
    this._index = new StringIndex();
  }

  async init(ipfs: IPFS<{}>, identity: Identity, options: IInitializationOptions<PayloadOperation> & { canRead?(key: SignatureWithKey): Promise<boolean> }): Promise<this> {
    await super.init(ipfs, identity, { ...options, encoding, onUpdate: this._index.updateIndex.bind(this._index) })
    await this.search.init(ipfs, identity, { ...options, context: { address: this.address }, canRead: options.canRead, queryHandler: this.queryHandler.bind(this) })
    return this;

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

  async queryHandler(query: QueryType): Promise<Result[]> {
    if (query instanceof StringQueryRequest == false) {
      return [];
    }
    const stringQuery = query as StringQueryRequest;

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


