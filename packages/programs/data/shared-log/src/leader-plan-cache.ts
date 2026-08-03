import { Cache } from "@peerbit/cache";

type LeaderMap = Map<string, { intersecting: boolean }>;

const cloneLeaders = (leaders: LeaderMap): LeaderMap => {
	const cloned: LeaderMap = new Map();
	for (const [hash, meta] of leaders) {
		cloned.set(hash, { intersecting: meta.intersecting });
	}
	return cloned;
};

/**
 * Memoizes leader-election plans between replication-topology changes.
 *
 * Coherence is structural, not temporal: a private monotonic version is
 * bumped (and the store wiped) on every invalidation, and an async fill is
 * only stored when the version captured before the computation still holds —
 * a plan computed across a topology change is returned to its caller but
 * never cached. The TTL bounds wall-clock maturity flips that fire no event
 * (a range re-crossing maturity after the dynamic roleAge regrew has no
 * pending timer), so it is load-bearing for that edge.
 *
 * Callers mutate returned maps and their value objects, so every hit and
 * every store clones.
 */
export class LeaderPlanCache {
	private cache: Cache<{ leaders: LeaderMap }>;
	private version = 0;

	constructor(options: { max: number; ttl: number }) {
		this.cache = new Cache(options);
	}

	invalidate(): void {
		this.version++;
		this.cache.clear();
	}

	get(key: string): LeaderMap | undefined {
		const entry = this.cache.get(key);
		return entry ? cloneLeaders(entry.leaders) : undefined;
	}

	/** Capture the version before an awaited plan computation. */
	capture(): number {
		return this.version;
	}

	/** Store only if no invalidation happened since `capturedVersion`. */
	put(key: string, leaders: LeaderMap, capturedVersion: number): void {
		if (capturedVersion !== this.version) {
			return;
		}
		this.cache.add(
			key,
			{ leaders: cloneLeaders(leaders) },
			Math.max(1, leaders.size),
		);
	}
}
