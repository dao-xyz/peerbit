import { PayloadOperation, StringIndex, encoding } from './string-index.js'
import { DSearch, QueryType, StoreAddressMatchQuery } from '@dao-xyz/peerbit-dsearch';
import { RangeCoordinate, RangeCoordinates, Result, ResultWithSource, StringMatchQuery } from '@dao-xyz/peerbit-dsearch';
import { StringQueryRequest } from '@dao-xyz/peerbit-dsearch';
import { Range } from './range.js';
import { field, variant } from '@dao-xyz/borsh';
import { CustomBinaryPayload } from '@dao-xyz/peerbit-bpayload';
import { Store } from '@dao-xyz/peerbit-store';
import { BORSH_ENCODING, CanAppend, Identity } from '@dao-xyz/ipfs-log';
import { SignatureWithKey } from '@dao-xyz/peerbit-crypto';
import { Program } from '@dao-xyz/peerbit-program';
import { QueryOptions, CanRead } from '@dao-xyz/peerbit-dquery';
export const STRING_STORE_TYPE = 'string_store';
const findAllOccurrences = (str: string, substr: string): number[] => {
  str = str.toLowerCase();

  let result: number[] = [];

  let idx = str.indexOf(substr)

  while (idx !== -1) {
    result.push(idx);
    idx = str.indexOf(substr, idx + 1);
  }
  return result;
}

export type StringStoreOptions = { canRead?: (key: SignatureWithKey) => Promise<boolean> };

@variant([0, 5])
export class DString extends Program {

  @field({ type: Store })
  store: Store<PayloadOperation>

  @field({ type: DSearch })
  search: DSearch<PayloadOperation>;

  _index: StringIndex;

  constructor(properties: { name?: string, search: DSearch<PayloadOperation> }) {
    super(properties)
    if (properties) {
      this.search = properties.search
      this.store = new Store(properties);
    }
    this._index = new StringIndex();
  }


  async setup(options?: { canRead?: CanRead, canAppend?: CanAppend<PayloadOperation> }) {

    this.store.setup({ encoding, onUpdate: this._index.updateIndex.bind(this._index) });
    if (options?.canAppend) {
      this.store.canAppend = options.canAppend;
    }

    await this.search.setup({ ...options, context: { address: () => this.address }, canRead: options?.canRead, queryHandler: this.queryHandler.bind(this) })


  }

  add(value: string, index: Range, options = {}) {
    return this.store._addOperation(new PayloadOperation({
      index,
      value,
    }), { ...options, encoding })
  }

  del(index: Range, options = {}) {
    const operation = {
      index
    } as PayloadOperation
    return this.store._addOperation(operation, { ...options, encoding })
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

  async toString(options?: { remote: { callback: (string: string) => any, queryOptions: QueryOptions } }): Promise<string | undefined> {
    if (options?.remote) {
      const counter: Map<string, number> = new Map();
      await this.search.query(new StringQueryRequest({
        queries: [new StoreAddressMatchQuery({
          address: this.address.toString()
        })]
      }), (response) => {
        const result = ((response.results[0] as ResultWithSource).source as StringResultSource).string;
        options?.remote.callback && options?.remote.callback(result)
        counter.set(result, (counter.get(result) || 0) + 1)
      }, options.remote.queryOptions);
      let max = -1;
      let ret: string | undefined = undefined;
      counter.forEach((v, k) => {
        if (max < v) {
          max = v
          ret = k;
        }
      })
      return ret;
    }
    else {
      return this._index.string;
    }
  }
}


@variant("string")
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


