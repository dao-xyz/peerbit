import type {
	NativeBackboneCoordinatePersistenceAdapter,
	ResidentCoordinateEntry,
} from "./index.js";

/**
 * Stage 4.5 (PR-1): the coordinate-persistence state owner.
 *
 * The four coordinate state fields below moved here from SharedLog
 * file-to-file under their historical names (the stage-1
 * ReplicationAnnouncementCoordinator pattern: coordinator-owned public
 * properties, host compat accessors for tests). SharedLog constructs one
 * coordinator per instance (`_coordinates`, created by
 * ensureNativeDurabilityRuntimeState so borsh-created clones that skip class
 * field initializers hydrate it exactly where the legacy `??=` defaults ran)
 * and reaches every field as a direct property hop — never through a getter —
 * so the append and receive hot loops keep their legacy access shape.
 *
 * Reset discipline is unchanged from the legacy host fields: each open/close
 * site resets exactly the subset of fields it always reset (the resident
 * mirror survives openNativeBackbone, the persistence adapter does not, and
 * the mutation-generation ratchet deliberately survives everything but a
 * fresh instance).
 */
export class CoordinatePersistenceCoordinator<R extends "u32" | "u64"> {
	/**
	 * Resident mirror of the coordinate rows (hash -> entry or native
	 * fields). Established by the native hydration paths; `undefined` means
	 * the resident fast path is unavailable.
	 */
	_residentEntryCoordinatesByHash?: Map<string, ResidentCoordinateEntry<R>>;

	/** The (optional) durable native-backbone coordinate journal adapter. */
	_nativeBackboneCoordinatePersistence?: NativeBackboneCoordinatePersistenceAdapter;

	// Moved from SharedLog src/index.ts (same name — the sanctioned
	// file-to-file ratchet move; see scripts/ci/check-fence-ratchet.mjs
	// TARGETS). Per-hash mutation-generation ratchet baseline: rollback
	// snapshots capture the generation current at snapshot time and later
	// roll back only while that generation is still current, so a rollback
	// superseded by a newer mutation is a strict no-op. The map deliberately
	// survives open/close cycles of the same instance.
	_nativeCoordinateMutationGenerations?: Map<string, number>;

	/**
	 * Wall-clock watermark of the last settled native coordinate journal
	 * flush; drives the `flushIntervalMs` on-append flush threshold.
	 */
	_nativeBackboneCoordinateJournalLastFlushMs = 0;
}
