import { field, variant } from "@dao-xyz/borsh";
import { AbstractProgram } from "@dao-xyz/peerbit-program";
import { Query } from "./query-interface.js";

@variant(0)
export class ContextMatchQuery extends Query {}

@variant(0)
export class ProgramMatchQuery extends ContextMatchQuery {
  @field({ type: "string" })
  program: string;

  constructor(
    opts?:
      | {
          program: string;
        }
      | AbstractProgram
  ) {
    super();
    if (opts) {
      if (opts instanceof AbstractProgram) {
        this.program = opts.address.toString();
      } else {
        this.program = opts.program;
      }
    }
  }
}
