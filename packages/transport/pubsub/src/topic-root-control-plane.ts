const topicHash32 = (topic: string) => {
	let hash = 0x811c9dc5; // FNV-1a
	for (let index = 0; index < topic.length; index++) {
		hash ^= topic.charCodeAt(index);
		hash = (hash * 0x01000193) >>> 0;
	}
	return hash >>> 0;
};

export type TopicRootResolver = (
	topic: string,
) => string | undefined | Promise<string | undefined>;

export type TopicRootTracker = {
	resolveRoot(topic: string): string | undefined | Promise<string | undefined>;
};

export type TopicRootDirectoryOptions = {
	defaultCandidates?: string[];
	resolver?: TopicRootResolver;
};

export class TopicRootDirectory {
	private readonly explicitRootsByTopic = new Map<string, string>();
	private defaultCandidates: string[] = [];
	private resolver?: TopicRootResolver;

	constructor(options?: TopicRootDirectoryOptions) {
		if (options?.defaultCandidates) {
			this.setDefaultCandidates(options.defaultCandidates);
		}
		this.resolver = options?.resolver;
	}

	public setRoot(topic: string, root: string) {
		this.explicitRootsByTopic.set(topic, root);
	}

	public deleteRoot(topic: string) {
		this.explicitRootsByTopic.delete(topic);
	}

	public getRoot(topic: string) {
		return this.explicitRootsByTopic.get(topic);
	}

	public setDefaultCandidates(candidates: string[]) {
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
		return [...this.defaultCandidates];
	}

	public setResolver(resolver?: TopicRootResolver) {
		this.resolver = resolver;
	}

	public async resolveRoot(topic: string): Promise<string | undefined> {
		const local = await this.resolveLocal(topic);
		if (local) return local;

		return this.resolveDeterministicCandidate(topic);
	}

	public async resolveLocal(topic: string): Promise<string | undefined> {
		const explicit = this.getRoot(topic);
		if (explicit) return explicit;

		return this.resolver?.(topic);
	}

	public resolveDeterministicCandidate(topic: string): string | undefined {
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

	public resolveTopicRoot(topic: string) {
		return this.resolveWithTrackers(topic);
	}

	private async resolveWithTrackers(topic: string): Promise<string | undefined> {
		const local = await this.directory.resolveLocal(topic);
		if (local) {
			return local;
		}

		for (const tracker of this.trackers) {
			try {
				const resolved = await tracker.resolveRoot(topic);
				if (resolved) {
					return resolved;
				}
			} catch {
				// ignore tracker failures and continue with remaining trackers
			}
		}
		return this.directory.resolveDeterministicCandidate(topic);
	}
}
