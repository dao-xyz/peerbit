import { variant } from "@dao-xyz/borsh";
import { Message } from "./message.js";

@variant(9)
export abstract class LifeCycleMessage extends Message {}

@variant(0)
export class REQ_Start extends LifeCycleMessage {}

@variant(1)
export class RESP_Start extends LifeCycleMessage {}

@variant(2)
export class REQ_Stop extends LifeCycleMessage {}

@variant(3)
export class RESP_Stop extends LifeCycleMessage {}
