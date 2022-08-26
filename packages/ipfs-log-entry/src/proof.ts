import { variant } from "@dao-xyz/borsh";

/** Some proof that something is legit */
@variant(0)
export class Proof {

}

@variant(0)
export class NoProof extends Proof {

}