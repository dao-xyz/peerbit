import { Shard } from "@dao-xyz/node";
import { BinaryDocumentStore } from "@dao-xyz/orbit-db-bdocstore";
import { Post } from "./post";

export type ChannelShard = Shard<BinaryDocumentStore<Post>>