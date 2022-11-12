import { field, option, variant } from '@dao-xyz/borsh';
import { BORSH_ENCODING, Log } from '@dao-xyz/ipfs-log';
import { Entry } from '@dao-xyz/ipfs-log';
import { ComposableProgram } from '@dao-xyz/peerbit-program';
import { Range } from './range.js';

@variant(0)
export class StringOperation {

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
export const encoding = BORSH_ENCODING(StringOperation);


@variant("string_index")
export class StringIndex extends ComposableProgram {

  _string: string;
  constructor() {
    super()
    this._string = '';
  }

  get string(): string {
    return this._string;
  }

  async updateIndex(oplog: Log<StringOperation>) {
    this._string = await applyOperations('', oplog.values);
  }
}

export const applyOperations = async (string: string, operations: Entry<StringOperation>[]): Promise<string> => {
  await Promise.all(operations.map(operation => operation.getPayload()))

  operations.reduce((handled: string[], item: Entry<StringOperation>, _) => {
    if (!handled.includes(item.hash)) {
      handled.push(item.hash)
      string = applyOperation(string, item.payload.getValue(encoding));
    }

    return handled
  }, [])
  return string;
}
export const applyOperation = (s: string, operation: StringOperation): string => {
  // TODO check bounds number
  const to = Number(operation.index.offset) + Number(operation.index.length);
  if (operation.value != undefined) {
    s = s.padEnd(to);
    s = s.slice(0, Number(operation.index.offset)) + operation.value + s.slice(to)
    return s;
  } else {
    s = s.slice(0, Number(operation.index.offset)) + s.slice(to)
  }
  return s;
}

