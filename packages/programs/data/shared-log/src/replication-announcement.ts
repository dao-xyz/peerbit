import type { ReplicationInfoMutation } from "./replication-info-mutation.js";

export type ReplicationAnnouncementDeps = {
	captureReplicationOwnershipLifecycle: () => AbortController;
	throwIfReplicationOwnershipLifecycleInactive: (
		controller: AbortController,
	) => void;
	// V2 is always current: every committed local mutation feeds the V2
	// sender. The legacy broadcast tail this fed in parallel was deleted in
	// B12 stage 3, and the announcement-session/repair-generation rotation
	// that ordered the legacy retry/repair workers folded away in stage 5
	// once no consumer pinned session identity.
	enqueueReplicationInfoV2: (mutation: ReplicationInfoMutation) => void;
};

export class ReplicationAnnouncementCoordinator {
	// Publish local ownership announcements in committed mutation order. This
	// prevents an older Added message with a delayed transport completion from
	// overtaking a newer authoritative empty snapshot.
	_replicationAnnouncementSendTails?: WeakMap<AbortController, Promise<void>>;

	constructor(private readonly deps: ReplicationAnnouncementDeps) {
		this._replicationAnnouncementSendTails = new WeakMap();
	}

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
