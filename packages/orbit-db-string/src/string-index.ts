import { Range, RangeOptional } from './range';
export interface PayloadOperation {
  index: RangeOptional,
  value?: string
}

export interface StringLogEntry { hash: string, payload: PayloadOperation };

export class StringIndex {

  _string: string;
  constructor() {
    this._string = '';
  }

  get string(): string {
    return this._string;
  }

  public updateIndex(oplog: any, onProgressCallback?: (item: StringLogEntry, ix: number) => void) {
    this._string = applyOperations('', oplog.values, onProgressCallback);
  }
}

export const applyOperations = (string: string, operations: StringLogEntry[], onProgressCallback?: (item: StringLogEntry, ix: number) => void): string => {
  operations.reduce((handled, item: { hash: string, payload: PayloadOperation }, idx) => {

    if (onProgressCallback) {
      onProgressCallback(item, idx)
    }
    if (!handled.includes(item.hash)) {
      handled.push(item.hash)
      string = applyOperation(string, item.payload);
    }

    return handled
  }, [])
  return string;
}
export const applyOperation = (s: string, operation: PayloadOperation): string => {
  let to = operation.index.offset + (typeof operation.index.length === 'number' ? operation.index.length : operation.value.length);
  if (operation.value != undefined) {
    s = s.padEnd(to);
    s = s.slice(0, operation.index.offset) + operation.value + s.slice(to)
    return s;
  } else {
    s = s.slice(0, operation.index.offset) + s.slice(to)
  }
  return s;
}

