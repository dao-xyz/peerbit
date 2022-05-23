import { DaoDB } from "@dao-xyz/social-interface";
import { RecursiveShard } from "@dao-xyz/node";
export declare const createNode: (genesis?: RecursiveShard<any>) => Promise<DaoDB>;
