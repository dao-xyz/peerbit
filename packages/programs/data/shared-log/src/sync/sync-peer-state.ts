// Per-peer sync slot state shared by the simple and rateless synchronizers
// (stage-4 sync unification). One row per peer carries the response/lookup
// slot accounting whose release closures may settle long after the peer is
// gone; both synchronizers use the same registry and the same
// capture-the-row identity pattern.
//
// Per-field lifetime policy (review this table when adding fields):
//
// | field                        | created            | cleared                    |
// |------------------------------|--------------------|----------------------------|
// | lookups / responses / active | first slot acquire | release closure settles;   |
// |                              |                    | row DETACHES at peer       |
// |                              |                    | disconnect (quota returns  |
// |                              |                    | in aggregate; late         |
// |                              |                    | releases settle against    |
// |                              |                    | the detached row)          |
// | pendingResponseHashes        | consume accepts an | lease release settles      |
// | (simple synchronizer only)   | authorization      | while attached; row        |
// |                              | (custody moves     | DETACHES at peer           |
// |                              | batch -> row)      | disconnect (budget returns |
// |                              |                    | in aggregate)              |
// | activeReleases               | consume creates an | release settles (self-     |
// | (simple synchronizer only)   | active-response    | removal); detach invokes   |
// |                              | lease              | the outstanding releases   |
// |                              |                    | so retained work drains    |
//
// Slot accounting deliberately SURVIVES close()/open() generation rotation
// on both synchronizers: the underlying storage resolvers and transport
// sends are not universally abortable, so cross-generation work stays
// charged until it settles. Only a peer disconnect detaches a row.
//
// Deliberately host-side this stage (stage-5 folds them into the host peer
// sessions): dispatch epochs and dispatch-target sets (benchmark-gated hot
// path), pending-claim rows (owned by the pending-sync record store),
// admission per-peer indexes (owned by the record store's reservation
// objects) and the recently-sent exchange-head dedupe rows.

export type SyncPeerSlotRow = {
	peer: string;
	attached: boolean;
	lookups: number;
	responses: number;
	active: number;
	// Portion of the simple synchronizer's global pending-response hash budget
	// held by this peer's ACTIVE (consumed) authorizations. Batch-resident
	// hashes stay charged against the batch; consume moves custody here so a
	// disconnect can return the budget even when the response ship never
	// settles. Always 0 on rateless rows.
	pendingResponseHashes: number;
	// Outstanding active-response lease releases (simple synchronizer only).
	// Releases self-remove when the ship settles; detach invokes the
	// remainder so responseLeases/retainedWork drain and the dispatch
	// lifecycle can dispose — the released-guard makes the eventual late
	// ship-side release inert. Always empty on rateless rows.
	activeReleases: Set<() => void>;
};

export class SyncPeerSlotRegistry {
	readonly rows = new Map<string, SyncPeerSlotRow>();

	ensure(peer: string): SyncPeerSlotRow {
		let row = this.rows.get(peer);
		if (!row) {
			row = {
				peer,
				attached: true,
				lookups: 0,
				responses: 0,
				active: 0,
				pendingResponseHashes: 0,
				activeReleases: new Set(),
			};
			this.rows.set(peer, row);
		}
		return row;
	}

	// Detaches and returns the peer's row (or undefined when the peer holds
	// no slots). The caller subtracts the returned counters from its
	// aggregate quota; release closures that still hold the row settle
	// against it aggregate-neutrally.
	detach(peer: string): SyncPeerSlotRow | undefined {
		const row = this.rows.get(peer);
		if (!row) {
			return undefined;
		}
		row.attached = false;
		this.rows.delete(peer);
		return row;
	}

	// Rows whose slots all settled while attached are dropped eagerly so the
	// registry never accumulates idle peers between disconnects.
	maybeDropIdle(row: SyncPeerSlotRow): void {
		if (
			row.attached &&
			row.lookups === 0 &&
			row.responses === 0 &&
			row.active === 0 &&
			row.pendingResponseHashes === 0 &&
			row.activeReleases.size === 0
		) {
			this.rows.delete(row.peer);
		}
	}
}
