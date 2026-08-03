import type { RustTopicRootDirectoryState } from "@peerbit/stream";
import { AbortError } from "@peerbit/time";

const topicHash32 = (topic: string) => {
	let hash = 0x811c9dc5; // FNV-1a
	for (let index = 0; index < topic.length; index++) {
		hash ^= topic.charCodeAt(index);
		hash = (hash * 0x01000193) >>> 0;
	}
	return hash >>> 0;
};

export type TopicRootResolutionOptions = {
	signal?: AbortSignal;
};

export type TopicRootResolver = (
	topic: string,
	options?: TopicRootResolutionOptions,
) => string | undefined | Promise<string | undefined>;

export type TopicRootTracker = {
	resolveRoot(
		topic: string,
		options?: TopicRootResolutionOptions,
	): string | undefined | Promise<string | undefined>;
};

export type TopicRootDirectoryOptions = {
	defaultCandidates?: string[];
	resolver?: TopicRootResolver;
};

const abortReason = (signal: AbortSignal) =>
	signal.reason ?? new AbortError("Topic-root resolution aborted");

const throwIfAborted = (signal?: AbortSignal): void => {
	if (signal?.aborted) throw abortReason(signal);
};

const awaitWithSignal = async <T>(
	value: T | PromiseLike<T>,
	signal?: AbortSignal,
): Promise<T> => {
	if (signal?.aborted) {
		void Promise.resolve(value).catch(() => {});
		throw abortReason(signal);
	}
	if (!signal) return await value;

	return new Promise<T>((resolve, reject) => {
		const onAbort = () => {
			cleanup();
			reject(abortReason(signal));
		};
		const cleanup = () => signal.removeEventListener("abort", onAbort);
		signal.addEventListener("abort", onAbort, { once: true });
		Promise.resolve(value).then(
			(result) => {
				cleanup();
				resolve(result);
			},
			(error) => {
				cleanup();
				reject(error);
			},
		);
	});
};

export class TopicRootDirectory {
	private readonly explicitRootsByTopic = new Map<string, string>();
	private defaultCandidates: string[] = [];
	private resolver?: TopicRootResolver;
	private native?: RustTopicRootDirectoryState;

	constructor(options?: TopicRootDirectoryOptions) {
		if (options?.defaultCandidates) {
			this.setDefaultCandidates(options.defaultCandidates);
		}
		this.resolver = options?.resolver;
	}

	/**
	 * Move the root-resolution state (explicit roots + deterministic
	 * candidates) into the native topic-control core. Current contents are
	 * copied over; a directory that already adopted native state keeps it
	 * (the first adoption owns the state, so directories shared between
	 * co-located planes are only migrated once). The resolver callback and
	 * trackers stay host-side.
	 */
	public adoptNativeState(state: RustTopicRootDirectoryState) {
		if (this.native) return;
		for (const [topic, root] of this.explicitRootsByTopic) {
			state.setRoot(topic, root);
		}
		state.setDefaultCandidates(this.defaultCandidates);
		this.explicitRootsByTopic.clear();
		this.defaultCandidates = [];
		this.native = state;
	}

	public setRoot(topic: string, root: string) {
		if (this.native) {
			this.native.setRoot(topic, root);
			return;
		}
		this.explicitRootsByTopic.set(topic, root);
	}

	public deleteRoot(topic: string) {
		if (this.native) {
			this.native.deleteRoot(topic);
			return;
		}
		this.explicitRootsByTopic.delete(topic);
	}

	public getRoot(topic: string) {
		if (this.native) {
			return this.native.getRoot(topic);
		}
		return this.explicitRootsByTopic.get(topic);
	}

	public setDefaultCandidates(candidates: string[]) {
		if (this.native) {
			this.native.setDefaultCandidates(candidates);
			return;
		}
		const unique = new Set<string>();
		for (const candidate of candidates) {
			if (!candidate) continue;
			unique.add(candidate);
		}
		this.defaultCandidates = [...unique].sort((a, b) =>
			a < b ? -1 : a > b ? 1 : 0,
		);
	}

	public getDefaultCandidates() {
		if (this.native) {
			return this.native.getDefaultCandidates();
		}
		return [...this.defaultCandidates];
	}

	public setResolver(resolver?: TopicRootResolver) {
		this.resolver = resolver;
	}

	public async resolveRoot(
		topic: string,
		options?: TopicRootResolutionOptions,
	): Promise<string | undefined> {
		const local = await this.resolveLocal(topic, options);
		if (local) return local;

		throwIfAborted(options?.signal);
		return this.resolveDeterministicCandidate(topic);
	}

	public async resolveLocal(
		topic: string,
		options?: TopicRootResolutionOptions,
	): Promise<string | undefined> {
		throwIfAborted(options?.signal);
		const explicit = this.getRoot(topic);
		if (explicit) return explicit;

		if (!this.resolver) return undefined;
		return awaitWithSignal(this.resolver(topic, options), options?.signal);
	}

	public resolveDeterministicCandidate(topic: string): string | undefined {
		if (this.native) {
			return this.native.resolveDeterministicCandidate(topic);
		}
		if (this.defaultCandidates.length === 0) return undefined;
		const index = topicHash32(topic) % this.defaultCandidates.length;
		return this.defaultCandidates[index];
	}
}

export type TopicRootControlPlaneOptions = TopicRootDirectoryOptions & {
	directory?: TopicRootDirectory;
	trackers?: TopicRootTracker[];
};

export class TopicRootControlPlane {
	private readonly directory: TopicRootDirectory;
	private trackers: TopicRootTracker[];

	constructor(options?: TopicRootControlPlaneOptions) {
		this.directory =
			options?.directory ||
			new TopicRootDirectory({
				defaultCandidates: options?.defaultCandidates,
				resolver: options?.resolver,
			});
		this.trackers = options?.trackers ? [...options.trackers] : [];
	}

	/** See {@link TopicRootDirectory.adoptNativeState}. */
	public adoptNativeDirectoryState(state: RustTopicRootDirectoryState) {
		this.directory.adoptNativeState(state);
	}

	public setTopicRoot(topic: string, root: string) {
		this.directory.setRoot(topic, root);
	}

	public clearTopicRoot(topic: string) {
		this.directory.deleteRoot(topic);
	}

	public getTopicRoot(topic: string) {
		return this.directory.getRoot(topic);
	}

	public setTopicRootCandidates(candidates: string[]) {
		this.directory.setDefaultCandidates(candidates);
	}

	public getTopicRootCandidates() {
		return this.directory.getDefaultCandidates();
	}

	public setTopicRootResolver(resolver?: TopicRootResolver) {
		this.directory.setResolver(resolver);
	}

	public setTopicRootTrackers(trackers: TopicRootTracker[]) {
		this.trackers = [...trackers];
	}

	public getTopicRootTrackers() {
		return [...this.trackers];
	}

	public resolveLocalTopicRoot(
		topic: string,
		options?: TopicRootResolutionOptions,
	) {
		return this.directory.resolveLocal(topic, options);
	}

	public resolveDeterministicTopicRoot(topic: string) {
		return this.directory.resolveDeterministicCandidate(topic);
	}

	public resolveCanonicalTopicRoot(
		topic: string,
		options?: TopicRootResolutionOptions,
	) {
		return this.directory.resolveRoot(topic, options);
	}

	public resolveTrackedTopicRoot(
		topic: string,
		options?: TopicRootResolutionOptions,
	) {
		return this.resolveWithTrackers(topic, false, options);
	}

	public resolveTopicRoot(
		topic: string,
		options?: TopicRootResolutionOptions,
	) {
		return this.resolveWithTrackers(topic, true, options);
	}

	private async resolveWithTrackers(
		topic: string,
		fallbackToDeterministic = true,
		options?: TopicRootResolutionOptions,
	): Promise<string | undefined> {
		const local = await this.directory.resolveLocal(topic, options);
		if (local) {
			return local;
		}

		for (const tracker of this.trackers) {
			try {
				throwIfAborted(options?.signal);
				const resolved = await awaitWithSignal(
					tracker.resolveRoot(topic, options),
					options?.signal,
				);
				if (resolved) {
					return resolved;
				}
			} catch {
				throwIfAborted(options?.signal);
				// ignore tracker failures and continue with remaining trackers
			}
		}
		throwIfAborted(options?.signal);
		return fallbackToDeterministic
			? this.directory.resolveDeterministicCandidate(topic)
			: undefined;
	}
}
