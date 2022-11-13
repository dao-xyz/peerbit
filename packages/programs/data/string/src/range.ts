import { field, variant } from "@dao-xyz/borsh";

@variant(0)
export class Range {
  @field({ type: "u64" })
  offset: bigint;

  @field({ type: "u64" })
  length: bigint;

  constructor(props?: { offset: bigint | number; length: bigint | number }) {
    if (props) {
      this.offset =
        typeof props.offset === "number" ? BigInt(props.offset) : props.offset;
      this.length =
        typeof props.length === "number" ? BigInt(props.length) : props.length;
    }
  }
}
