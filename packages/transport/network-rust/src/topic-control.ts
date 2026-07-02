// TS adapter for the native topic-control-plane components (`topic_control`
// module of the peerbit_wire crate). Implements the `RustTopicControl`
// surface consumed by `@peerbit/pubsub` in rust-core mode: the PubSubMessage
// codec, the FNV-1a topic hashing behind shard mapping and deterministic
// root selection, the TopicRootDirectory state and the subscribe-state
// convergence rules run in wasm. The observable subscription maps stay
// host-side; PubSubData payload bytes are reported as offsets so the host
// aliases them without a copy.
import type {
	RustDecodedPubSubMessage,
	RustTopicControl,
	RustTopicRootDirectoryState,
} from "@peerbit/stream";

const PUBSUB_VARIANT_DATA = 0;
const PUBSUB_VARIANT_SUBSCRIBE = 1;
const PUBSUB_VARIANT_UNSUBSCRIBE = 2;
const PUBSUB_VARIANT_GET_SUBSCRIBERS = 3;
const PUBSUB_VARIANT_TOPIC_ROOT_CANDIDATES = 4;
const PUBSUB_VARIANT_PEER_UNAVAILABLE = 5;
const PUBSUB_VARIANT_TOPIC_ROOT_QUERY = 6;
const PUBSUB_VARIANT_TOPIC_ROOT_QUERY_RESPONSE = 7;

type WasmDecodedPubSubMessage = {
	variant: number;
	topics: string[];
	flag: boolean;
	data_offset: number;
	data_length: number;
	text: string;
	root?: string;
	request_id: number;
	session: bigint;
	timestamp: bigint;
	free?: () => void;
};

type WasmRootDirectoryInstance = {
	set_root(topic: string, root: string): void;
	delete_root(topic: string): void;
	get_root(topic: string): string | undefined;
	set_default_candidates(candidates: string[]): void;
	get_default_candidates(): string[];
	resolve_deterministic_candidate(topic: string): string | undefined;
};

export type TopicControlWasmExports = {
	TopicControlRootDirectory: new () => WasmRootDirectoryInstance;
	tc_decode_pubsub_message(frame: Uint8Array): WasmDecodedPubSubMessage;
	tc_encode_pubsub_data(
		topics: string[],
		strict: boolean,
		data: Uint8Array,
	): Uint8Array;
	tc_encode_subscribe(
		topics: string[],
		requestSubscribers: boolean,
	): Uint8Array;
	tc_encode_unsubscribe(topics: string[]): Uint8Array;
	tc_encode_get_subscribers(topics: string[]): Uint8Array;
	tc_encode_topic_root_candidates(candidates: string[]): Uint8Array;
	tc_encode_peer_unavailable(
		publicKeyHash: string,
		session: bigint,
		timestamp: bigint,
		topics: string[],
	): Uint8Array;
	tc_encode_topic_root_query(requestId: number, topic: string): Uint8Array;
	tc_encode_topic_root_query_response(
		requestId: number,
		topic: string,
		root?: string,
	): Uint8Array;
	tc_topic_hash32(topic: string): number;
	tc_shard_topic(topic: string, shardCount: number, prefix: string): string;
	tc_normalize_auto_candidates(candidates: string[], me: string): string[];
	tc_subscription_is_latest(
		lasts: BigUint64Array,
		session: bigint,
		timestamp: bigint,
	): boolean;
	tc_subscribe_should_replace(
		existingSession: bigint | undefined,
		session: bigint,
	): boolean;
};

class RustTopicRootDirectoryAdapter implements RustTopicRootDirectoryState {
	private readonly wasm: WasmRootDirectoryInstance;

	constructor(module: TopicControlWasmExports) {
		this.wasm = new module.TopicControlRootDirectory();
	}

	setRoot(topic: string, root: string): void {
		this.wasm.set_root(topic, root);
	}

	deleteRoot(topic: string): void {
		this.wasm.delete_root(topic);
	}

	getRoot(topic: string): string | undefined {
		return this.wasm.get_root(topic);
	}

	setDefaultCandidates(candidates: string[]): void {
		this.wasm.set_default_candidates(candidates);
	}

	getDefaultCandidates(): string[] {
		return this.wasm.get_default_candidates();
	}

	resolveDeterministicCandidate(topic: string): string | undefined {
		return this.wasm.resolve_deterministic_candidate(topic);
	}
}

export const createRustTopicControl = (
	wasm: TopicControlWasmExports,
): RustTopicControl => ({
	encodePubSubData: (topics, strict, data) =>
		wasm.tc_encode_pubsub_data(topics, strict, data),
	encodeSubscribe: (topics, requestSubscribers) =>
		wasm.tc_encode_subscribe(topics, requestSubscribers),
	encodeUnsubscribe: (topics) => wasm.tc_encode_unsubscribe(topics),
	encodeGetSubscribers: (topics) => wasm.tc_encode_get_subscribers(topics),
	encodeTopicRootCandidates: (candidates) =>
		wasm.tc_encode_topic_root_candidates(candidates),
	encodePeerUnavailable: (publicKeyHash, session, timestamp, topics) =>
		wasm.tc_encode_peer_unavailable(publicKeyHash, session, timestamp, topics),
	encodeTopicRootQuery: (requestId, topic) =>
		wasm.tc_encode_topic_root_query(requestId, topic),
	encodeTopicRootQueryResponse: (requestId, topic, root) =>
		wasm.tc_encode_topic_root_query_response(requestId, topic, root),
	decodePubSubMessage: (payload): RustDecodedPubSubMessage => {
		const decoded = wasm.tc_decode_pubsub_message(payload);
		try {
			switch (decoded.variant) {
				case PUBSUB_VARIANT_DATA:
					return {
						type: "data",
						topics: decoded.topics,
						strict: decoded.flag,
						data: payload.subarray(
							decoded.data_offset,
							decoded.data_offset + decoded.data_length,
						),
					};
				case PUBSUB_VARIANT_SUBSCRIBE:
					return {
						type: "subscribe",
						topics: decoded.topics,
						requestSubscribers: decoded.flag,
					};
				case PUBSUB_VARIANT_UNSUBSCRIBE:
					return { type: "unsubscribe", topics: decoded.topics };
				case PUBSUB_VARIANT_GET_SUBSCRIBERS:
					return { type: "get-subscribers", topics: decoded.topics };
				case PUBSUB_VARIANT_TOPIC_ROOT_CANDIDATES:
					return { type: "topic-root-candidates", candidates: decoded.topics };
				case PUBSUB_VARIANT_PEER_UNAVAILABLE:
					return {
						type: "peer-unavailable",
						publicKeyHash: decoded.text,
						session: decoded.session,
						timestamp: decoded.timestamp,
						topics: decoded.topics,
					};
				case PUBSUB_VARIANT_TOPIC_ROOT_QUERY:
					return {
						type: "topic-root-query",
						requestId: decoded.request_id,
						topic: decoded.text,
					};
				case PUBSUB_VARIANT_TOPIC_ROOT_QUERY_RESPONSE:
					return {
						type: "topic-root-query-response",
						requestId: decoded.request_id,
						topic: decoded.text,
						root: decoded.root,
					};
				default:
					throw new Error(
						`unsupported pubsub message variant ${decoded.variant}`,
					);
			}
		} finally {
			decoded.free?.();
		}
	},
	shardTopic: (topic, shardCount, prefix) =>
		wasm.tc_shard_topic(topic, shardCount, prefix),
	normalizeAutoCandidates: (candidates, me) =>
		wasm.tc_normalize_auto_candidates(candidates, me),
	subscriptionIsLatest: (lasts, session, timestamp) =>
		wasm.tc_subscription_is_latest(lasts, session, timestamp),
	subscribeShouldReplace: (existingSession, session) =>
		wasm.tc_subscribe_should_replace(existingSession, session),
	createRootDirectoryState: () => new RustTopicRootDirectoryAdapter(wasm),
});
