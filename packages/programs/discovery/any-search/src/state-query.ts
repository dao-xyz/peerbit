import { field, option, variant, vec } from "@dao-xyz/borsh";
import { EntryEncryptionTemplate } from "@dao-xyz/ipfs-log";
import { UInt8ArraySerializer } from "@dao-xyz/peerbit-borsh-utils";
import { X25519PublicKey } from "@dao-xyz/peerbit-crypto";
import { Query } from "./query-interface.js";

@variant(2)
export class StateQuery extends Query {}

@variant(1)
export class StateFieldQuery extends StateQuery {
  @field({ type: vec("string") })
  key: string[];

  constructor(props?: { key: string[] | string }) {
    super();
    if (props) {
      this.key = Array.isArray(props.key) ? props.key : [props.key];
    }
  }
}

/* @variant(0)
export class ArrayQuery extends FieldQuery { }

@variant(0)
export class ArrayContainsQuery extends ArrayQuery {

    @field(UInt8ArraySerializer)
    value: Uint8Array

    constructor(props?: { key: string[], value: Uint8Array }) {
        super(props);
        if (props) {
            this.value = props.value;
        }
    }

} */

@variant(1)
export class FieldByteMatchQuery extends StateFieldQuery {
  @field(UInt8ArraySerializer)
  value: Uint8Array;

  constructor(props?: { key: string[]; value: Uint8Array }) {
    super(props);
    if (props) {
      this.value = props.value;
    }
  }
}

@variant(2)
export class FieldStringMatchQuery extends StateFieldQuery {
  @field({ type: "string" })
  value: string;

  constructor(props?: { key: string[] | string; value: string }) {
    super(props);
    if (props) {
      this.value = props.value;
    }
  }
}
export enum Compare {
  Equal = 0,
  Greater = 1,
  GreaterOrEqual = 2,
  Less = 3,
  LessOrEqual = 4,
}

@variant(3)
export class FieldBigIntCompareQuery extends StateFieldQuery {
  @field({ type: "u8" })
  compare: Compare;

  @field({ type: "u64" })
  value: bigint;

  constructor(props?: {
    key: string[] | string;
    value: bigint;
    compare: Compare;
  }) {
    super(props);
    if (props) {
      this.value = props.value;
      this.compare = props.compare;
    }
  }
}
