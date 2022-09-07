import { PayloadOperation, StringIndex } from './string-index'
import { IPFS as IPFSInstance } from 'ipfs';
import { Identity } from '@dao-xyz/orbit-db-identity-provider';
import { QueryStore } from '@dao-xyz/orbit-db-query-store';
import { QueryRequestV0, RangeCoordinate, RangeCoordinates, Result, ResultWithSource, StringMatchQuery } from '@dao-xyz/query-protocol';
import { StringQueryRequest } from '@dao-xyz/query-protocol';
import { Range } from './range';
import { deserialize, field, serialize, variant } from '@dao-xyz/borsh';

import { BinaryPayload } from '@dao-xyz/bpayload';
import { AccessController, IStoreOptions } from '@dao-xyz/orbit-db-store';

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




const defaultOptions = (options: IStoreOptions<any>): any => {
  if (!options.encoding) {
    options.encoding = {
      decoder: (bytes) => deserialize(Buffer.from(bytes), PayloadOperation),
      encoder: (data) => serialize(data)
    }
  }
  return options;
}

export class StringStore extends QueryStore<PayloadOperation> {

  _index: StringIndex;
  constructor(properties: { accessController: AccessController<PayloadOperation> }) {
    super(properties)
    this._index = new StringIndex();
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

  queryHandler(query: QueryRequestV0): Promise<Result[]> {
    if (query.type instanceof StringQueryRequest == false) {
      return;
    }
    const stringQuery = query.type as StringQueryRequest;

    const content = this._index.string;
    const relaventQueries = stringQuery.queries.filter(x => x instanceof StringMatchQuery) as StringMatchQuery[]
    if (relaventQueries.length == 0) {
      return Promise.resolve([new ResultWithSource({
        source: new StringResultSource({
          string: content
        })
      })])
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
      return;
    }

    return Promise.resolve([new ResultWithSource({
      source: new StringResultSource({
        string: content,
      }),
      coordinates: new RangeCoordinates({
        coordinates: ranges
      })
    })]);
  }
}

@variant("string")
/* @variant([0, 2]) */
export class StringResultSource extends BinaryPayload {

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


