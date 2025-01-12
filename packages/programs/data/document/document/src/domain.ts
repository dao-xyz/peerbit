import type * as types from "@peerbit/document-interface";
import { Entry, type ShallowEntry } from "@peerbit/log";
import { logger as loggerFn } from "@peerbit/logger";
import {
	type EntryReplicated,
	MAX_U32,
	MAX_U64,
	type NumberFromType,
	type ReplicationDomain,
	type SharedLog,
} from "@peerbit/shared-log";
import { type Operation, isPutOperation } from "./operation.js";
import type { DocumentIndex } from "./search.js";

const logger = loggerFn({ module: "document-domain" });

type InferT<D> = D extends Documents<infer T, any, any> ? T : never;
type InferR<D> =
	D extends Documents<any, ReplicationDomain<any, any, infer R>> ? R : never;

type Documents<
	T,
	D extends ReplicationDomain<any, Operation, R>,
	R extends "u32" | "u64" = D extends ReplicationDomain<any, T, infer I>
		? I
		: "u32",
> = { log: SharedLog<Operation, D, R>; index: DocumentIndex<T, any, D> };

type RangeArgs<R extends "u32" | "u64"> = {
	from: NumberFromType<R>;
	to: NumberFromType<R>;
};
export type CustomDocumentDomain<R extends "u32" | "u64"> = ReplicationDomain<
	RangeArgs<R>,
	Operation,
	R
> & { canProjectToOneSegment: (request: types.SearchRequest) => boolean };

type FromEntry<R extends "u32" | "u64"> = {
	fromEntry?: (
		entry: ShallowEntry | Entry<Operation> | EntryReplicated<R>,
	) => NumberFromType<R>;
};
type FromValue<T, R extends "u32" | "u64"> = {
	fromValue?: (
		value: T | undefined,
		entry: ShallowEntry | Entry<Operation> | EntryReplicated<R>,
	) => NumberFromType<R>;
};

type CreateArgs<
	R extends "u32" | "u64",
	DB extends Documents<any, any, any>,
> = {
	resolution: R;
	canProjectToOneSegment: (request: types.SearchRequest) => boolean;
	mergeSegmentMaxDelta?: number;
} & (FromEntry<R> | FromValue<InferT<DB>, R>);

export const createDocumentDomainFromProperty = <
	R extends "u32" | "u64",
	DB extends Documents<any, any, any>,
>(properties: {
	property: keyof InferT<DB>;
	resolution: R;
	mergeSegmentMaxDelta?: number;
}): ((db: DB) => CustomDocumentDomain<InferR<DB>>) => {
	const coerceNumber = (number: number | bigint): NumberFromType<R> =>
		(properties.resolution === "u32"
			? number
			: BigInt(number)) as NumberFromType<R>;
	return createDocumentDomain({
		resolution: properties.resolution,
		canProjectToOneSegment: (request) =>
			request.sort[0]?.key[0] === properties.property,
		fromValue: (value) => coerceNumber(value![properties.property]),
		mergeSegmentMaxDelta: properties.mergeSegmentMaxDelta,
	});
};

export const createDocumentDomain =
	<R extends "u32" | "u64", DB extends Documents<any, any, any>>(
		args: CreateArgs<R, DB>,
	): ((db: DB) => CustomDocumentDomain<InferR<DB>>) =>
	(db: DB) => {
		let maxValue = args.resolution === "u32" ? MAX_U32 : MAX_U64;
		let fromEntry = (args as FromEntry<InferR<DB>>).fromEntry
			? (args as FromEntry<InferR<DB>>).fromEntry!
			: async (
					entry: ShallowEntry | Entry<Operation> | EntryReplicated<any>,
				) => {
					const item = await (
						entry instanceof Entry ? entry : await db.log.log.get(entry.hash)
					)?.getPayloadValue();

					let document: InferT<DB> | undefined = undefined;
					if (!item) {
						logger.error("Item not found");
					} else if (isPutOperation(item)) {
						document = db.index.valueEncoding.decoder(item.data);
					}
					return (args as FromValue<any, any>).fromValue!(
						document,
						entry,
					) as NumberFromType<InferR<DB>>;
				};
		return {
			type: "custom",
			resolution: args.resolution as InferR<DB>,
			canProjectToOneSegment: args.canProjectToOneSegment,
			fromArgs(args) {
				if (!args) {
					return {
						offset: db.log.node.identity.publicKey,
						length: maxValue as NumberFromType<InferR<DB>>,
					};
				}
				return {
					offset: args.from,
					length: (args.to - args.from) as NumberFromType<InferR<DB>>,
				};
			},
			fromEntry,
			canMerge:
				args.mergeSegmentMaxDelta == null
					? undefined
					: (from, into) => {
							if (
								Math.abs(Number(from.end2 - into.start1)) <=
								args.mergeSegmentMaxDelta!
							) {
								return true;
							}
							if (
								Math.abs(Number(from.start1 - into.end2)) <=
								args.mergeSegmentMaxDelta!
							) {
								return true;
							}
							if (from.overlaps(into)) {
								return true;
							}
							return false;
						},
		};
	};
