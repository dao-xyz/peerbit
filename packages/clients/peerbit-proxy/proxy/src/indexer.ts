import { variant } from "@dao-xyz/borsh";
import { Message } from "./message.js";

@variant(11)
export abstract class IndexerMessage extends Message {}

// TODO
