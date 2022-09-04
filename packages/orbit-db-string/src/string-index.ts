import { field, option, variant } from '@dao-xyz/borsh';
import { Log } from '@dao-xyz/ipfs-log';
import { Entry, Payload } from '@dao-xyz/ipfs-log-entry';
import { Range } from './range';

@variant(0)
export class PayloadOperation {

  @field({ type: Range })
  index: Range

  @field({ type: option('string') })
  value?: string

  constructor(props?: { index: Range, value?: string }) {
    if (props) {
      this.index = props.index
      this.value = props.value;
    }
  }
}


export class StringIndex {

  _string: string;
  constructor() {
    this._string = '';
  }

  get string(): string {
    return this._string;
  }

  async updateIndex(oplog: Log<PayloadOperation>) {
    this._string = await applyOperations('', oplog.values);
  }
}

export const applyOperations = async (string: string, operations: Entry<PayloadOperation>[]): Promise<string> => {
  await Promise.all(operations.map(operation => operation.getPayload()))
  operations.reduce((handled, item, idx) => {
    if (!handled.includes(item.hash)) {
      handled.push(item.hash)
      string = applyOperation(string, item.payload.value);
    }

    return handled
  }, [])
  return string;
}
export const applyOperation = (s: string, operation: PayloadOperation): string => {
  // TODO check bounds number
  let to = Number(operation.index.offset) + (typeof operation.index.length === 'number' ? operation.index.length : operation.value.length);
  if (operation.value != undefined) {
    s = s.padEnd(to);
    s = s.slice(0, Number(operation.index.offset)) + operation.value + s.slice(to)
    return s;
  } else {
    s = s.slice(0, Number(operation.index.offset)) + s.slice(to)
  }
  return s;
}

