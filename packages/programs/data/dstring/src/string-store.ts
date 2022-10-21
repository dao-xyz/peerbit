import { PayloadOperation, StringIndex, encoding } from './string-index.js'
import { DSearch, DSearchInitializationOptions, QueryType } from '@dao-xyz/peerbit-dsearch';
import { RangeCoordinate, RangeCoordinates, Result, ResultWithSource, StringMatchQuery } from '@dao-xyz/peerbit-dsearch';
import { StringQueryRequest } from '@dao-xyz/peerbit-dsearch';
import { Range } from './range.js';
import { deserialize, field, serialize, variant } from '@dao-xyz/borsh';
import { CustomBinaryPayload } from '@dao-xyz/peerbit-bpayload';
import { Address, IInitializationOptions, IStoreOptions, load, Store } from '@dao-xyz/peerbit-store';
import { IPFS } from 'ipfs-core-types';
import { BORSH_ENCODING, CanAppend, Identity } from '@dao-xyz/ipfs-log';
import { SignatureWithKey } from '@dao-xyz/peerbit-crypto';
import { RootProgram, Program, ProgramInitializationOptions } from '@dao-xyz/peerbit-program';
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

const encond = BORSH_ENCODING(PayloadOperation)
export type StringStoreOptions = { canRead?: (key: SignatureWithKey) => Promise<boolean> };

@variant([0, 5])
export class DString extends Program implements RootProgram {

  @field({ type: Store })
  store: Store<PayloadOperation>

  @field({ type: DSearch })
  search: DSearch<PayloadOperation>;

  _index: StringIndex;
  _setup = false;
  constructor(properties: { name?: string, search: DSearch<PayloadOperation> }) {
    super(properties)
    if (properties) {
      this.search = properties.search
      this.store = new Store(properties);
    }
    this._index = new StringIndex();
  }


  async setup(options?: { canRead?(key: SignatureWithKey): Promise<boolean>, canAppend?: CanAppend<PayloadOperation> }) {

    this.store.onUpdate = this._index.updateIndex.bind(this._index)
    if (options?.canAppend) {
      this.store.canAppend = options.canAppend;
    }

    await this.search.setup({ ...options, context: { address: () => this.address }, canRead: options?.canRead, queryHandler: this.queryHandler.bind(this) })

    this._setup = true;

  }

  checkSetup() {
    if (!this._setup) {
      throw new Error(".setup(...) needs to be invoked before use")
    }
  }
  add(value: string, index: Range, options = {}) {
    this.checkSetup();
    return this.store._addOperation(new PayloadOperation({
      index,
      value,
    }), { ...options, encoding })
  }

  del(index: Range, options = {}) {
    this.checkSetup();
    const operation = {
      index
    } as PayloadOperation
    return this.store._addOperation(operation, { ...options, encoding })
  }

  async queryHandler(query: QueryType): Promise<Result[]> {
    this.checkSetup();
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


