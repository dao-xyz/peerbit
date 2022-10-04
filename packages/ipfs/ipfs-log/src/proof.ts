import { variant } from "@dao-xyz/borsh";

/** Some kind of DDOS resistance proof for unverified identities */
@variant(0)
export class Proof {

}

@variant(0)
export class NoProof extends Proof {

}