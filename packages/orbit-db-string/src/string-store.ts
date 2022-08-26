import { PayloadOperation, StringIndex } from './string-index'
import { IPFS as IPFSInstance } from 'ipfs';
import { Identity } from '@dao-xyz/orbit-db-identity-provider';
import { QueryStore } from '@dao-xyz/orbit-db-query-store';
import { QueryRequestV0, RangeCoordinate, RangeCoordinates, Result, ResultWithSource, StringMatchQuery } from '@dao-xyz/bquery';
import { StringQueryRequest } from '@dao-xyz/bquery';
import BN from 'bn.js';
import { Range, RangeOptional } from './range';
import { field, variant } from '@dao-xyz/borsh';
import { BStoreOptions } from "@dao-xyz/orbit-db-bstores";
import { IQueryStoreOptions } from '@dao-xyz/orbit-db-query-store';
import { OrbitDB } from '@dao-xyz/orbit-db';
import { BinaryPayload } from '@dao-xyz/bpayload';

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


export type IStringStoreOptions = IQueryStoreOptions<PayloadOperation, string, StringIndex>;


@variant([0, 3])
export class StringStoreOptions extends BStoreOptions<StringStore> {

  constructor() {
    super();
  }
  async newStore(address: string, orbitDB: OrbitDB, options: IStringStoreOptions): Promise<StringStore> {
    return orbitDB.open(address, { ...options, ...{ create: true, type: STRING_STORE_TYPE } })
  }

  get identifier(): string {
    return STRING_STORE_TYPE
  }
}

const defaultOptions = (options: IStringStoreOptions): any => {
  if (!options.Index) Object.assign(options, { Index: StringIndex })
  return options;
}

export class StringStore extends QueryStore<PayloadOperation, string, StringIndex, IStringStoreOptions> {

  _type: string = undefined;
  constructor(ipfs: IPFSInstance, id: Identity, dbname: string, options: IStringStoreOptions) {
    super(ipfs, id, dbname, defaultOptions(options))
    this._type = STRING_STORE_TYPE;
  }

  add(value: string, index: RangeOptional, options = {}) {
    return this._addOperation({
      index,
      value,
    } as PayloadOperation, options)
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
          offset: new BN(ix),
          length: new BN(query.value.length)
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

  @field({ type: 'String' })
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

OrbitDB.addDatabaseType(STRING_STORE_TYPE, StringStore as any)

