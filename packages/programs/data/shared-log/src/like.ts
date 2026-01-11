import type { PublicSignKey } from "@peerbit/crypto";
import type {
	CountOptions,
	IndexIterator,
	IterateOptions,
} from "@peerbit/indexer-interface";
import type { Entry } from "@peerbit/log";
import type { ShallowEntry } from "@peerbit/log";
import type { ReplicationOptions, ReplicationRangeIndexable } from "./index.js";

export type LogBlocksLike = {
	has: (hash: string) => Promise<boolean> | boolean;
};

export type LogResultsIterator<T> = {
	close: () => void | Promise<void>;
	next: (amount: number) => T[] | Promise<T[]>;
	done: () => boolean | undefined;
	all: () => T[] | Promise<T[]>;
};

export type LogLike<T = any> = {
	idString?: string;
	length: number;
	get: (
		hash: string,
		options?: any,
	) => Promise<Entry<T> | undefined> | Entry<T> | undefined;
	has: (hash: string) => Promise<boolean> | boolean;
	getHeads: (resolve?: boolean) => LogResultsIterator<Entry<T> | ShallowEntry>;
	toArray: () => Promise<Entry<T>[]>;
	blocks?: LogBlocksLike;
};

export type SharedLogReplicationIndexLike<R extends "u32" | "u64" = any> = {
	iterate: (
		request?: IterateOptions,
	) => IndexIterator<ReplicationRangeIndexable<R>, undefined>;
	count: (options?: CountOptions) => Promise<number> | number;
	getSize?: () => Promise<number> | number;
};

export type SharedLogLike<T = any, R extends "u32" | "u64" = any> = {
	closed?: boolean;
	events: EventTarget;
	log: LogLike<T>;
	replicationIndex: SharedLogReplicationIndexLike<R>;
	node?: { identity: { publicKey: PublicSignKey } };
	getReplicators: () => Promise<Set<string>>;
	waitForReplicator: (
		publicKey: PublicSignKey,
		options?: {
			eager?: boolean;
			roleAge?: number;
			timeout?: number;
			signal?: AbortSignal;
		},
	) => Promise<void>;
	waitForReplicators: (options?: {
		timeout?: number;
		roleAge?: number;
		coverageThreshold?: number;
		waitForNewPeers?: boolean;
		signal?: AbortSignal;
	}) => Promise<void>;
	replicate: (
		rangeOrEntry?: ReplicationOptions<R> | any,
		options?: {
			reset?: boolean;
			checkDuplicates?: boolean;
			rebalance?: boolean;
			mergeSegments?: boolean;
		},
	) => Promise<void | ReplicationRangeIndexable<R>[]>;
	unreplicate: (rangeOrEntry?: { id: Uint8Array }[]) => Promise<void>;
	calculateCoverage: (options?: {
		start?: number | bigint;
		end?: number | bigint;
		roleAge?: number;
	}) => Promise<number>;
	getMyReplicationSegments: () => Promise<ReplicationRangeIndexable<R>[]>;
	getAllReplicationSegments: () => Promise<ReplicationRangeIndexable<R>[]>;
	close: () => Promise<void | boolean>;
};
