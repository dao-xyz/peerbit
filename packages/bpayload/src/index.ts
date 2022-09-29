import { variant } from "@dao-xyz/borsh";

@variant(0)
export class BinaryPayload {

}


/**
 * Reserverd for system things
 */
@variant(0)
export class SystemBinaryPayload extends BinaryPayload {

}

/**
 * Can be used to deliver custom payloads
 */
@variant(1)
export class CustomBinaryPayload extends BinaryPayload {

}