import type { ReplicationInfoMutation } from "./replication-info-mutation.js";

/**
 * One session per announcement window — rotated at exactly the sites that
 * bumped the legacy retry-generation counter: construction, resetForOpen and
 * the pre-send bump in sendReplicationAnnouncement. Identity comparison
 * replaced every numeric generation compare while the legacy retry/repair
 * workers existed; the workers are gone (B12 stage 3) and the rotation sites
 * are kept-old until stage 5 verifies no consumer pins session identity.
 */
export class AnnouncementWorkerSession {
	readonly createdAt = Date.now(); // diagnostics only
}

export type ReplicationAnnouncementDeps = {
	captureReplicationOwnershipLifecycle: () => AbortController;
	throwIfReplicationOwnershipLifecycleInactive: (
		controller: AbortController,
	) => void;
	// V2 is always current: every committed local mutation feeds the V2
	// sender. The legacy broadcast tail this fed in parallel was deleted in
	// B12 stage 3.
	enqueueReplicationInfoV2: (mutation: ReplicationInfoMutation) => void;
};

export class ReplicationAnnouncementCoordinator {
	_announcementSession!: AnnouncementWorkerSession;
	// Publish local ownership announcements in committed mutation order. This
	// prevents an older Added message with a delayed transport completion from
	// overtaking a newer authoritative empty snapshot.
	_replicationAnnouncementSendTails?: WeakMap<AbortController, Promise<void>>;

	constructor(private readonly deps: ReplicationAnnouncementDeps) {
		this._announcementSession = new AnnouncementWorkerSession();
		this._replicationAnnouncementSendTails = new WeakMap();
	}

	private rotateAnnouncementSession(): AnnouncementWorkerSession {
		return (this._announcementSession = new AnnouncementWorkerSession());
	}

	resetForOpen(): void {
		this.rotateAnnouncementSession();
	}

	/**
	 * KEEP-OLD (B12 stage 3): the repair binding this advanced was deleted
	 * with the legacy repair worker. The pre-send call shape below is
	 * preserved until stage 5 verifies no surviving test pins announcement
	 * session identity, then the rotation/advance pair folds together.
	 */
	private advanceCurrentReplicationStateAnnouncementRepairGeneration(): void {}

	async sendReplicationAnnouncement(
		mutation: ReplicationInfoMutation,
		ownershipLifecycleController = this.deps.captureReplicationOwnershipLifecycle(),
		options?: { shouldSend?: () => boolean },
	): Promise<void> {
		const tails = (this._replicationAnnouncementSendTails ??= new WeakMap<
			AbortController,
			Promise<void>
		>());
		const previous =
			tails.get(ownershipLifecycleController) ?? Promise.resolve();
		const send = previous
			.catch(() => {})
			.then(async () => {
				this.deps.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
				if (options?.shouldSend && !options.shouldSend()) {
					return;
				}
				// Advance before every post-mutation send, including successful ones. An
				// authoritative retry already in flight may have captured the previous
				// local state; the session mismatch forces one more current snapshot
				// after that stale send settles.
				this.rotateAnnouncementSession();
				this.advanceCurrentReplicationStateAnnouncementRepairGeneration();
				this.deps.enqueueReplicationInfoV2(mutation);
			});
		// Keep the ordering barrier usable after a caller-observed send rejection.
		tails.set(
			ownershipLifecycleController,
			send.catch(() => {}),
		);
		return send;
	}
}
