import { calculateRawCid, cidifyString } from "@peerbit/blocks-interface";

export type RangeResolution = "u32" | "u64";

type NativePeerbitBackboneHandle = {
	log_len: () => number;
	block_len: () => number;
	has_log_entry: (hash: string) => boolean;
	has_block: (hash: string) => boolean;
	entry_coordinate_hashes: () => string[];
	get_entry_coordinates: (hash: string) => unknown[] | undefined;
	find_leaders: (
		cursors: string[],
		replicas: number,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => unknown[];
	find_leaders_batch: (
		cursorBatches: string[][],
		replicaCounts: number[],
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => unknown[][];
	get_grid: (from: string, count: number) => unknown[];
	get_gid_coordinates: (gid: string, count: number) => unknown[];
	entry_hashes_for_hash_numbers: (hashNumbers: string[]) => unknown[];
	entry_hashes_for_hash_numbers_u64?: (
		hashNumbers: BigUint64Array,
	) => unknown[];
	entry_hashes_for_hash_numbers_flat_u64?: (
		hashNumbers: BigUint64Array,
	) => string[];
	entry_hash_numbers_in_range: (
		start1: string,
		end1: string,
		start2: string,
		end2: string,
	) => unknown[];
	entry_hash_numbers_in_range_u64?: (
		start1: string,
		end1: string,
		start2: string,
		end2: string,
	) => BigUint64Array;
	count_entry_coordinates_in_ranges: (
		start1: string[],
		end1: string[],
		start2: string[],
		end2: string[],
		includeAssignedToRangeBoundary: boolean,
	) => number;
	entry_coordinate_fields: () => unknown[];
	coordinate_index_len: () => number;
	coordinate_value_len: () => number;
	coordinate_index_has_hash: (hash: string) => boolean;
	configure_document_schema_ir: (
		schemaIr: Uint8Array,
	) => [number, number, number];
	set_document_byte_element_index_limit?: (limit: number) => void;
	set_document_context_head_field: (field: number) => void;
	set_document_context_fields: (
		created: number,
		modified: number,
		head: number,
		gid: number,
		size: number,
	) => void;
	register_document_projection_plan: (
		plan: NativeBackboneSimpleDocumentProjectionPlan,
	) => number;
	project_document_index_simple: (
		encodedDocument: Uint8Array,
		plan: NativeBackboneSimpleDocumentProjectionPlan,
		created: string,
		modified: string,
		head: string,
		gid: string,
		size: number,
		signer?: Uint8Array,
	) => Uint8Array;
	set_append_profile_enabled: (enabled: boolean) => void;
	reset_append_profile: () => void;
	append_profile: () => number[];
	document_index_len: () => number;
	document_value_len: () => number;
	document_exact_string_first_key: (
		field: number,
		value: string,
	) => string | undefined;
	document_value_bytes: (key: string) => Uint8Array | undefined;
	document_entry: (key: string) => [string, Uint8Array] | undefined;
	document_keys_exist?: (keys: string[]) => Uint8Array;
	document_field_value: (
		key: string,
		field: number,
	) => NativeBackboneDocumentFieldValue | undefined;
	document_context: (
		key: string,
	) => [string, string, string, string, number] | undefined;
	document_context_batch?: (
		keys: string[],
	) => Array<[string, string, string, string, number] | undefined>;
	document_previous_signature_public_key?: (
		key: string,
	) => [boolean, Uint8Array | undefined];
	document_context_previous_signature_public_key_batch?: (
		keys: string[],
	) => Array<
		[
			[string, string, string, string, number] | undefined,
			Uint8Array | undefined,
		]
	>;
	document_query: (
		queryBytes: Uint8Array,
		sortBytes: Uint8Array,
	) => Array<[string, Uint8Array]>;
	document_query_page: (
		queryBytes: Uint8Array,
		sortBytes: Uint8Array,
		offset: number,
		limit: number,
	) => Array<[string, Uint8Array]>;
	document_count: (queryBytes: Uint8Array) => number;
	document_sum: (
		queryBytes: Uint8Array,
		field: number,
	) => ["none" | "i64" | "u64", string];
	put_document_encoded_parts_stored: (
		key: string,
		valuePrefixBytes: Uint8Array,
		valueSuffixBytes: Uint8Array,
		byteElementIndexLimit: number,
	) => void;
	put_document_encoded_parts_stored_batch: (
		keys: string[],
		valuePrefixBytes: Uint8Array[],
		valueSuffixBytes: Uint8Array[],
		byteElementIndexLimit: number,
	) => void;
	delete_document: (key: string) => boolean;
	delete_documents: (keys: string[]) => number;
	delete_documents_result: (keys: string[]) => Uint8Array;
	clear_document_index: () => void;
	document_journal_header: () => Uint8Array;
	document_pending_journal_len: () => number;
	document_pending_journal_byte_len: () => number;
	document_journal_enabled: () => boolean;
	set_document_journal_enabled: (enabled: boolean) => void;
	document_journal: () => Uint8Array;
	clear_document_journal: () => void;
	document_snapshot: () => Uint8Array;
	load_document_snapshot_and_journal: (
		snapshot: Uint8Array,
		journal: Uint8Array,
	) => number;
	coordinate_journal_header: () => Uint8Array;
	coordinate_pending_journal_len: () => number;
	coordinate_pending_journal_byte_len: () => number;
	coordinate_journal_enabled: () => boolean;
	set_coordinate_journal_enabled: (enabled: boolean) => void;
	coordinate_journal: () => Uint8Array;
	clear_coordinate_journal: () => void;
	coordinate_snapshot: () => Uint8Array;
	load_coordinate_snapshot_and_journal: (
		snapshot: Uint8Array,
		journal: Uint8Array,
	) => number;
	document_signer_journal_header: () => Uint8Array;
	document_signer_pending_journal_len: () => number;
	document_signer_pending_journal_byte_len: () => number;
	document_signer_journal_enabled: () => boolean;
	set_document_signer_journal_enabled: (enabled: boolean) => void;
	document_signer_journal: () => Uint8Array;
	clear_document_signer_journal: () => void;
	document_signer_snapshot: () => Uint8Array;
	load_document_signer_snapshot_and_journal: (
		snapshot: Uint8Array,
		journal: Uint8Array,
	) => number;
	graph_has_many: (hashes: string[]) => string[];
	graph_put: (
		hash: string,
		gid: string,
		next: string[],
		type: number,
		wallTime: bigint,
		logical: number,
		payloadSize: number,
		head: boolean,
		data?: Uint8Array,
	) => void;
	graph_put_batch: (
		hashes: string[],
		gids: string[],
		nexts: string[][],
		entryTypes: Uint8Array,
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		payloadSizes: Uint32Array,
		heads: Uint8Array,
		datas: Array<Uint8Array | undefined>,
	) => void;
	graph_put_append_chain: (
		hashes: string[],
		gid: string,
		initialNext: string[],
		entryType: number,
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		payloadSizes: Uint32Array,
		datas: Array<Uint8Array | undefined>,
	) => void;
	graph_clear: () => void;
	commit_log_blocks_and_graph_batch: (
		hashes: string[],
		blockBytes: Uint8Array[],
		gids: string[],
		nexts: string[][],
		entryTypes: Uint8Array,
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		payloadSizes: Uint32Array,
		heads: Uint8Array,
		datas: Array<Uint8Array | undefined>,
	) => void;
	commit_log_blocks_graph_and_coordinates_batch: (
		hashes: string[],
		blockBytes: Uint8Array[],
		gids: string[],
		nexts: string[][],
		entryTypes: Uint8Array,
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		payloadSizes: Uint32Array,
		heads: Uint8Array,
		datas: Array<Uint8Array | undefined>,
		coordinateHashes: string[],
		coordinateGids: string[],
		coordinateHashNumbers: string[],
		coordinateBatches: string[][],
		coordinateNextHashBatches: string[][],
		coordinateAssignedToRangeBoundaries: Uint8Array,
		coordinateRequestedReplicas: number[],
	) => void;
	prepare_raw_receive_batch: (
		blocks: Uint8Array[],
	) => NativeBackboneRawReceivePreparedFactsRow[];
	prepare_raw_receive_columns_batch?: (
		blocks: Uint8Array[],
	) => NativeBackboneRawReceivePreparedFactsColumns;
	prepare_raw_receive_unverified_columns_batch?: (
		blocks: Uint8Array[],
	) => NativeBackboneRawReceivePreparedFactsColumns;
	prepare_raw_receive_expected_columns_batch?: (
		blocks: Uint8Array[],
		hashes: string[],
	) => NativeBackboneRawReceivePreparedFactsColumns;
	prepare_raw_receive_expected_compact_columns_batch?: (
		blocks: Uint8Array[],
		hashes: string[],
	) => NativeBackboneRawReceivePreparedFactsColumns;
	prepare_raw_receive_unverified_expected_columns_batch?: (
		blocks: Uint8Array[],
		hashes: string[],
	) => NativeBackboneRawReceivePreparedFactsColumns;
	prepare_raw_receive_unverified_expected_compact_columns_batch?: (
		blocks: Uint8Array[],
		hashes: string[],
	) => NativeBackboneRawReceivePreparedFactsColumns;
	prepare_raw_receive_unverified_expected_compact_columns_and_selection_batch?: (
		blocks: Uint8Array[],
		hashes: string[],
		minReplicas: number,
		maxReplicas: number | undefined,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
		fromHash: string,
	) => [
		NativeBackboneRawReceivePreparedFactsColumns,
		NativeBackboneRawReceiveSelectionRow | undefined,
	];
	plan_prepared_raw_receive_groups?: (
		hashes: string[],
		minReplicas: number,
		maxReplicas?: number,
	) => NativeBackboneRawReceiveGroupPlanRow[] | undefined;
	plan_prepared_raw_receive_group_indexes?: (
		hashes: string[],
		minReplicas: number,
		maxReplicas?: number,
	) => NativeBackboneRawReceiveGroupIndexPlanRow[] | undefined;
	plan_prepared_raw_receive_group_leaders?: (
		hashes: string[],
		minReplicas: number,
		maxReplicas: number | undefined,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => NativeBackboneRawReceiveGroupLeaderPlanRow[] | undefined;
	plan_prepared_raw_receive_group_assignments?: (
		hashes: string[],
		minReplicas: number,
		maxReplicas: number | undefined,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
		fromHash: string,
	) => NativeBackboneRawReceiveGroupAssignmentPlanRow[] | undefined;
	plan_prepared_raw_receive_fast_drop?: (
		hashes: string[],
		minReplicas: number,
		maxReplicas: number | undefined,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
		fromHash: string,
	) => [boolean, number, number] | undefined;
	plan_prepared_raw_receive_selection?: (
		hashes: string[],
		minReplicas: number,
		maxReplicas: number | undefined,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
		fromHash: string,
	) => NativeBackboneRawReceiveSelectionRow | undefined;
	select_prepared_raw_receive_hashes?: (
		hashes: string[],
		minReplicas: number,
		maxReplicas: number | undefined,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
		fromHash: string,
	) => NativeBackboneRawReceiveSelectionRow | undefined;
	verify_prepared_raw_receive_entries?: (
		hashes: string[],
	) => Uint8Array | undefined;
	commit_prepared_raw_receive_batch: (
		hashes: string[],
		heads: Uint8Array,
		coordinateHashes: string[],
		coordinateGids: string[],
		coordinateHashNumbers: string[],
		coordinateBatches: string[][],
		coordinateNextHashBatches: string[][],
		coordinateAssignedToRangeBoundaries: Uint8Array,
		coordinateRequestedReplicas: number[],
	) => boolean;
	commit_prepared_raw_receive_batch_u64?: (
		hashes: string[],
		heads: Uint8Array,
		coordinateHashes: string[],
		coordinateGids: string[],
		coordinateHashNumbers: BigUint64Array,
		coordinateCounts: Uint32Array,
		coordinates: BigUint64Array,
		coordinateNextHashBatches: string[][],
		coordinateAssignedToRangeBoundaries: Uint8Array,
		coordinateRequestedReplicas: Uint32Array,
	) => boolean;
	commit_prepared_raw_receive_join_batch?: (
		hashes: string[],
		heads: Uint8Array,
		coordinateHashes: string[],
		coordinateGids: string[],
		coordinateHashNumbers: string[],
		coordinateBatches: string[][],
		coordinateNextHashBatches: string[][],
		coordinateAssignedToRangeBoundaries: Uint8Array,
		coordinateRequestedReplicas: number[],
	) => boolean;
	commit_prepared_raw_receive_join_batch_u64?: (
		hashes: string[],
		heads: Uint8Array,
		coordinateHashes: string[],
		coordinateGids: string[],
		coordinateHashNumbers: BigUint64Array,
		coordinateCounts: Uint32Array,
		coordinates: BigUint64Array,
		coordinateNextHashBatches: string[][],
		coordinateAssignedToRangeBoundaries: Uint8Array,
		coordinateRequestedReplicas: Uint32Array,
	) => boolean;
	commit_verified_prepared_raw_receive_join_batch?: (
		hashes: string[],
		heads: Uint8Array,
		verifyHashes: string[],
		coordinateHashes: string[],
		coordinateGids: string[],
		coordinateHashNumbers: string[],
		coordinateBatches: string[][],
		coordinateNextHashBatches: string[][],
		coordinateAssignedToRangeBoundaries: Uint8Array,
		coordinateRequestedReplicas: number[],
	) => boolean;
	commit_verified_prepared_raw_receive_join_batch_u64?: (
		hashes: string[],
		heads: Uint8Array,
		verifyHashes: string[],
		coordinateHashes: string[],
		coordinateGids: string[],
		coordinateHashNumbers: BigUint64Array,
		coordinateCounts: Uint32Array,
		coordinates: BigUint64Array,
		coordinateNextHashBatches: string[][],
		coordinateAssignedToRangeBoundaries: Uint8Array,
		coordinateRequestedReplicas: Uint32Array,
	) => boolean;
	commit_verified_all_prepared_raw_receive_join_batch?: (
		hashes: string[],
		heads: Uint8Array,
		coordinateHashes: string[],
		coordinateGids: string[],
		coordinateHashNumbers: string[],
		coordinateBatches: string[][],
		coordinateNextHashBatches: string[][],
		coordinateAssignedToRangeBoundaries: Uint8Array,
		coordinateRequestedReplicas: number[],
	) => boolean;
	commit_verified_all_prepared_raw_receive_join_batch_u64?: (
		hashes: string[],
		heads: Uint8Array,
		coordinateHashes: string[],
		coordinateGids: string[],
		coordinateHashNumbers: BigUint64Array,
		coordinateCounts: Uint32Array,
		coordinates: BigUint64Array,
		coordinateNextHashBatches: string[][],
		coordinateAssignedToRangeBoundaries: Uint8Array,
		coordinateRequestedReplicas: Uint32Array,
	) => boolean;
	clear_prepared_raw_receive_entries: (hashes: string[]) => number;
	graph_delete: (hash: string) => boolean;
	graph_delete_many: (hashes: string[]) => number;
	graph_oldest_entries: (limit: number) => unknown[];
	graph_heads: (gid?: string) => string[];
	graph_has_head: (gid?: string) => boolean;
	graph_has_any_head: (gids: string[]) => boolean;
	graph_has_any_head_batch: (gidSets: string[][]) => boolean[];
	graph_head_entries: (gid?: string) => unknown[];
	graph_head_data_entries: (gid?: string) => unknown[];
	graph_max_head_data_u32: (gid?: string) => number | undefined;
	graph_max_head_data_u32_batch: (gids: string[]) => Array<number | undefined>;
	graph_join_head_entries: (gid?: string) => unknown[];
	graph_child_join_entries: (hash: string) => unknown[];
	graph_entry_metadata_batch: (hashes: string[]) => unknown[];
	graph_entry_metadata_hints_batch?: (hashes: string[]) => unknown[];
	graph_entry_signature_public_key_batch?: (
		hashes: string[],
	) => Array<Uint8Array | undefined>;
	graph_unique_reference_gids: (hash: string) => string[] | undefined;
	graph_unique_reference_gid_rows_batch: (hashes: string[]) => unknown[];
	graph_unique_reference_gid_rows_flat_batch?: (
		hashes: string[],
	) => Array<[number, string, string]> | undefined;
	graph_plan_delete_recursively: (
		hashes: string[],
		skipFirst: boolean,
	) => string[];
	graph_payload_size_sum: () => number;
	graph_oldest_hash: () => string | undefined;
	graph_newest_hash: () => string | undefined;
	graph_count_has_next: (next: string, excludeHash?: string) => number;
	graph_shadowed_gids: (
		gid: string,
		next: string[],
		excludeHash?: string,
	) => string[];
	graph_plan_join: (
		hash: string,
		next: string[],
		type: number,
		reset: boolean,
		gid?: string,
		wallTime?: bigint,
		logical?: number,
	) => [boolean, string[], boolean, boolean];
	graph_plan_join_batch: (
		hashes: string[],
		nexts: string[][],
		types: Uint8Array,
		reset: boolean,
		gids: string[],
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		cutCheck: boolean,
	) => Array<[boolean, string[], boolean, boolean]>;
	block_get: (key: string) => Uint8Array | undefined;
	block_get_many: (keys: string[]) => Array<Uint8Array | undefined>;
	block_has_many: (keys: string[]) => boolean[];
	block_put: (key: string, value: Uint8Array) => void;
	block_put_many: (keys: string[], values: Uint8Array[]) => void;
	block_delete: (key: string) => boolean;
	block_delete_many: (keys: string[]) => number;
	block_entries: () => Array<[string, Uint8Array]>;
	block_size: () => number;
	clear: () => void;
	clear_shared_log: () => void;
	clear_entry_coordinates: () => void;
	put_range: (
		id: string,
		hash: string,
		timestamp: string,
		start1: string,
		end1: string,
		start2: string,
		end2: string,
		width: string,
		mode: number,
	) => void;
	delete_range: (id: string) => boolean;
	put_entry_coordinates: (
		hash: string,
		gid: string,
		hashNumber: string,
		coordinates: string[],
		assignedToRangeBoundary: boolean,
		requestedReplicas: number,
	) => void;
	delete_entry_coordinates: (hash: string) => boolean;
	delete_entry_coordinates_batch: (hashes: string[]) => void;
	commit_entry_coordinates: (
		hash: string,
		gid: string,
		hashNumber: string,
		coordinates: string[],
		nextHashes: string[],
		assignedToRangeBoundary: boolean,
		requestedReplicas: number,
	) => void;
	commit_entry_coordinates_batch?: (
		hashes: string[],
		gids: string[],
		hashNumbers: string[],
		coordinateBatches: string[][],
		nextHashBatches: string[][],
		assignedToRangeBoundaries: Uint8Array,
		requestedReplicas: number[],
	) => void;
	commit_entry_coordinates_batch_u64?: (
		hashes: string[],
		gids: string[],
		hashNumbers: BigUint64Array,
		coordinateCounts: Uint32Array,
		coordinates: BigUint64Array,
		nextHashBatches: string[][],
		assignedToRangeBoundaries: Uint8Array,
		requestedReplicas: Uint32Array,
	) => void;
	add_gid_peers: (gid: string, peers: string[], reset: boolean) => number;
	remove_gid_peer: (peer: string, gid?: string) => void;
	remove_gid_peers?: (peer: string, gids: string[]) => void;
	delete_gid_peers: (gid: string) => boolean;
	clear_gid_peers: () => void;
	mark_entries_known_by_peer: (hashes: string[], peer: string) => void;
	remove_entries_known_by_peer: (hashes: string[], peer: string) => void;
	remove_peer_from_entry_known_peers: (peer: string) => void;
	clear_entry_known_peers: () => void;
	plan_entry_leaders_for_gid: (
		gid: string,
		replicas: number,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => [unknown[], unknown[]];
	plan_leaders_for_gids_batch: (
		gids: string[],
		replicaCounts: number[],
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => Array<[unknown[], unknown[]]>;
	plan_leader_samples_for_gids_batch?: (
		gids: string[],
		replicaCounts: number[],
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => unknown[];
	plan_request_prune_leader_hints?: (
		hashes: string[],
		skipHashes: string[],
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => unknown[];
	plan_request_prune_leader_hint_columns?: (
		hashes: string[],
		skipHashes: string[],
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => unknown[];
	plan_request_prune_all_confirmed?: (
		hashes: string[],
		prunePeer: string,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => unknown[];
	plan_request_prune_all_confirmed_no_gid_return?: (
		hashes: string[],
		prunePeer: string,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => boolean;
	plan_entry_assignment_for_gid: (
		gid: string,
		replicas: number,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => [unknown[], unknown[], boolean];
	plan_repair_dispatch_for_entries: (
		entryHashes: string[],
		entryGids: string[],
		entryRequestedReplicas: number[],
		entryCoordinateBatches: string[][],
		pendingModes: string[],
		pendingPeersByMode: string[][],
		optimisticPeersByMode: string[][][],
		fullReplicaRepairCandidates: string[],
		fullReplicaRepairCandidateCount: number,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => unknown[];
	plan_repair_dispatch_for_resident_entries: (
		pendingModes: string[],
		pendingPeersByMode: string[][],
		optimisticGidsByMode: string[][],
		optimisticPeersByGidByMode: string[][][],
		fullReplicaRepairCandidates: string[],
		fullReplicaRepairCandidateCount: number,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => unknown[];
	plan_local_append_for_gid_compact: (
		entryHash: string,
		gid: string,
		hashNumber: string,
		nextHashes: string[],
		replicas: number,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => [unknown[] | undefined, boolean, boolean, unknown[]];
	commit_local_append_for_gid_compact: (
		entryHash: string,
		gid: string,
		hashNumber: string,
		nextHashes: string[],
		deleteHashes: string[],
		replicas: number,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => [unknown[] | undefined, boolean, boolean, unknown[]];
	plan_append_for_gid: (
		entryHash: string,
		gid: string,
		hashNumber: string,
		nextHashes: string[],
		replicas: number,
		fullReplicaCandidates: string[],
		fallbackRecipients: string[],
		deliverySelfHash: string,
		deliveryEnabled: boolean,
		reliabilityAck: boolean,
		minAcks: number | undefined,
		requireRecipients: boolean,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => [
		unknown[],
		unknown[],
		boolean,
		boolean,
		[
			boolean,
			boolean,
			boolean,
			string[],
			string[],
			string[],
			string[],
			string[],
		],
		unknown[],
	];
	plan_append_for_gids_batch: (
		entryHashes: string[],
		gids: string[],
		hashNumbers: string[],
		nextHashBatches: string[][],
		replicaCounts: number[],
		fullReplicaCandidates: string[],
		fallbackRecipients: string[],
		deliverySelfHash: string,
		deliveryEnabled: boolean,
		reliabilityAck: boolean,
		minAcks: number | undefined,
		requireRecipients: boolean,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => Array<
		[
			unknown[],
			unknown[],
			boolean,
			boolean,
			[
				boolean,
				boolean,
				boolean,
				string[],
				string[],
				string[],
				string[],
				string[],
			],
			unknown[],
		]
	>;
	plan_receive_coordinates_for_gids_batch: (
		entryHashes: string[],
		gids: string[],
		hashNumbers: string[],
		nextHashBatches: string[][],
		replicaCounts: number[],
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => Array<[unknown[], unknown[], boolean, boolean, unknown[]]>;
	prepare_plain_entry_commit_facts: (
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		trimLengthTo: number | undefined,
	) => unknown[];
	prepare_plain_entry_commit_facts_document_index: (
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		trimLengthTo: number | undefined,
		documentKey: string,
		documentValuePrefixBytes: Uint8Array,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlan:
			| NativeBackboneSimpleDocumentProjectionPlan
			| undefined,
		documentProjectionEncodedDocument: Uint8Array | undefined,
		documentProjectionSigner: Uint8Array | undefined,
	) => unknown[];
	prepare_plain_entry_commit_facts_document_index_cached_plan: (
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		trimLengthTo: number | undefined,
		documentKey: string,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlanId: number,
		documentProjectionEncodedDocument: Uint8Array,
		documentProjectionSigner: Uint8Array | undefined,
	) => unknown[];
	prepare_plain_entry_commit_no_next_facts_document_index_compact: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		documentKey: string,
		documentValuePrefixBytes: Uint8Array,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlan:
			| NativeBackboneSimpleDocumentProjectionPlan
			| undefined,
		documentProjectionEncodedDocument: Uint8Array | undefined,
		documentProjectionSigner: Uint8Array | undefined,
	) => unknown[];
	prepare_plain_entry_commit_no_next_facts_document_index_cached_plan_compact: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		documentKey: string,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlanId: number,
		documentProjectionEncodedDocument: Uint8Array,
		documentProjectionSigner: Uint8Array | undefined,
	) => unknown[];
	prepare_plain_entry_commit_no_next_facts_document_index_cached_plan_compact_plain_put_payload?: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		documentKey: string,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlanId: number,
		documentProjectionSigner: Uint8Array | undefined,
	) => unknown[];
	prepare_plain_entry_commit_no_next_facts_document_index_trim_hashes: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		trimLengthTo: number,
		documentKey: string,
		documentValuePrefixBytes: Uint8Array,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlan:
			| NativeBackboneSimpleDocumentProjectionPlan
			| undefined,
		documentProjectionEncodedDocument: Uint8Array | undefined,
		documentProjectionSigner: Uint8Array | undefined,
	) => unknown[];
	prepare_plain_entry_commit_no_next_facts_document_index_compact_trim_hashes: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		trimLengthTo: number,
		documentKey: string,
		documentValuePrefixBytes: Uint8Array,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlan:
			| NativeBackboneSimpleDocumentProjectionPlan
			| undefined,
		documentProjectionEncodedDocument: Uint8Array | undefined,
		documentProjectionSigner: Uint8Array | undefined,
	) => unknown[];
	prepare_plain_entry_commit_no_next_facts_document_index_cached_plan_trim_hashes: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		trimLengthTo: number,
		documentKey: string,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlanId: number,
		documentProjectionEncodedDocument: Uint8Array,
		documentProjectionSigner: Uint8Array | undefined,
	) => unknown[];
	prepare_plain_entry_commit_no_next_facts_document_index_cached_plan_compact_trim_hashes: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		trimLengthTo: number,
		documentKey: string,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlanId: number,
		documentProjectionEncodedDocument: Uint8Array,
		documentProjectionSigner: Uint8Array | undefined,
	) => unknown[];
	prepare_plain_entry_commit_no_next_facts_document_index_cached_plan_compact_trim_hashes_plain_put_payload?: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		trimLengthTo: number,
		documentKey: string,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlanId: number,
		documentProjectionSigner: Uint8Array | undefined,
	) => unknown[];
	prepare_plain_entry_commit_latest_facts_document_index_trim_hashes: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		trimLengthTo: number | undefined,
		documentKey: string,
		documentValuePrefixBytes: Uint8Array,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlan:
			| NativeBackboneSimpleDocumentProjectionPlan
			| undefined,
		documentProjectionEncodedDocument: Uint8Array | undefined,
		documentProjectionSigner: Uint8Array | undefined,
	) => unknown[];
	prepare_plain_entry_commit_latest_facts_document_index_cached_plan_trim_hashes: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		trimLengthTo: number | undefined,
		documentKey: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlanId: number,
		documentProjectionEncodedDocument: Uint8Array,
		documentProjectionSigner: Uint8Array | undefined,
	) => unknown[];
	prepare_plain_entry_storage_facts_and_put: (
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
	) => unknown[];
	prepare_plain_entry_storage_facts_trim_and_put: (
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		trimLengthTo: number,
	) => unknown[];
	prepare_plain_no_next_storage_append_transaction: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
	) => unknown[];
	prepare_plain_no_next_storage_append_transaction_trim: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
		trimLengthTo: number,
	) => unknown[];
	prepare_plain_no_next_storage_append_document_index_transaction: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
		documentKey: string,
		documentValuePrefixBytes: Uint8Array,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlan:
			| NativeBackboneSimpleDocumentProjectionPlan
			| undefined,
		documentProjectionEncodedDocument: Uint8Array | undefined,
		documentProjectionSigner: Uint8Array | undefined,
	) => unknown[];
	prepare_plain_no_next_storage_append_document_index_transaction_trim: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
		documentKey: string,
		documentValuePrefixBytes: Uint8Array,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlan:
			| NativeBackboneSimpleDocumentProjectionPlan
			| undefined,
		documentProjectionEncodedDocument: Uint8Array | undefined,
		documentProjectionSigner: Uint8Array | undefined,
		trimLengthTo: number,
	) => unknown[];
	prepare_plain_committed_no_next_storage_append_transaction: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
	) => unknown[];
	prepare_plain_committed_no_next_storage_append_document_index_transaction: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
		documentKey: string,
		documentValuePrefixBytes: Uint8Array,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlan:
			| NativeBackboneSimpleDocumentProjectionPlan
			| undefined,
		documentProjectionEncodedDocument: Uint8Array | undefined,
		documentProjectionSigner: Uint8Array | undefined,
	) => unknown[];
	prepare_plain_committed_no_next_storage_append_document_index_cached_plan_transaction: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
		documentKey: string,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlanId: number,
		documentProjectionEncodedDocument: Uint8Array,
		documentProjectionSigner: Uint8Array | undefined,
	) => unknown[];
		prepare_plain_committed_no_next_storage_append_document_index_compact_transaction: (
			wallTime: bigint,
			logical: number,
			gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		documentKey: string,
		documentValuePrefixBytes: Uint8Array,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
			documentDeleteTrimmedHeads: boolean,
			trimLengthTo: number | undefined,
		) => unknown[];
		prepare_plain_committed_no_next_storage_append_document_index_compact_plain_put_payload_transaction?: (
			wallTime: bigint,
			logical: number,
			gid: string,
			type: number,
			metaData: Uint8Array | undefined,
			payloadData: Uint8Array,
			replicas: number,
			roleAgeMs: number,
			now: string,
			selfHash: string,
			selfReplicating: boolean,
			documentKey: string,
			documentExistingCreated: string,
			documentByteElementIndexLimit: number,
			documentDeleteTrimmedHeads: boolean,
			trimLengthTo: number | undefined,
		) => unknown[];
		prepare_plain_committed_no_next_storage_append_document_index_compact_batch_transaction?: (
			wallTimes: BigUint64Array,
			logicals: Uint32Array,
			gids: string[],
		type: number,
		metaDatas: Array<Uint8Array | undefined>,
		payloadDatas: Uint8Array[],
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		documentKeys: string[],
		documentValuePrefixBytes: Uint8Array[],
		documentExistingCreated: string[],
		documentByteElementIndexLimit: number,
			documentDeleteTrimmedHeads: boolean,
			trimLengthTo: number | undefined,
		) => unknown[][];
		prepare_plain_committed_no_next_storage_append_document_index_compact_plain_put_payload_batch_transaction?: (
			wallTimes: BigUint64Array,
			logicals: Uint32Array,
			gids: string[],
			type: number,
			metaDatas: Array<Uint8Array | undefined>,
			payloadDatas: Uint8Array[],
			replicas: number,
			roleAgeMs: number,
			now: string,
			selfHash: string,
			selfReplicating: boolean,
			documentKeys: string[],
			documentExistingCreated: string[],
			documentByteElementIndexLimit: number,
			documentDeleteTrimmedHeads: boolean,
			trimLengthTo: number | undefined,
		) => unknown[][];
	prepare_plain_committed_no_next_storage_append_document_index_cached_plan_compact_batch_transaction?: (
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		gids: string[],
		type: number,
		metaDatas: Array<Uint8Array | undefined>,
		payloadDatas: Uint8Array[],
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		documentKeys: string[],
		documentExistingCreated: string[],
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlanIds: Uint32Array,
		documentProjectionEncodedDocuments: Uint8Array[],
		documentProjectionSigners: Array<Uint8Array | undefined>,
		trimLengthTo: number | undefined,
	) => unknown[][];
	prepare_plain_committed_no_next_storage_append_document_index_cached_plan_compact_plain_put_payload_batch_transaction?: (
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		gids: string[],
		type: number,
		metaDatas: Array<Uint8Array | undefined>,
		payloadDatas: Uint8Array[],
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		documentKeys: string[],
		documentExistingCreated: string[],
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlanIds: Uint32Array,
		documentProjectionSigners: Array<Uint8Array | undefined>,
		trimLengthTo: number | undefined,
	) => unknown[][];
	prepare_plain_committed_no_next_storage_append_document_index_cached_plan_compact_transaction: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		documentKey: string,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlanId: number,
		documentProjectionEncodedDocument: Uint8Array,
		documentProjectionSigner: Uint8Array | undefined,
		trimLengthTo: number | undefined,
	) => unknown[];
	prepare_plain_committed_no_next_storage_append_document_index_cached_plan_compact_plain_put_payload_transaction?: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		documentKey: string,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlanId: number,
		documentProjectionSigner: Uint8Array | undefined,
		trimLengthTo: number | undefined,
	) => unknown[];
	prepare_plain_committed_no_next_storage_append_transaction_trim: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
		trimLengthTo: number,
	) => unknown[];
	benchmark_plain_committed_no_next_storage_append_transaction_loop: (
		iterations: number,
		wallTimeStart: bigint,
		payloadData: Uint8Array,
		replicas: number,
		selfHash: string,
		useDocumentIndex: boolean,
		documentByteElementIndexLimit: number,
		trimLengthTo: number | undefined,
	) => number[];
	prepare_plain_committed_no_next_storage_append_document_index_transaction_trim: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
		documentKey: string,
		documentValuePrefixBytes: Uint8Array,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlan:
			| NativeBackboneSimpleDocumentProjectionPlan
			| undefined,
		documentProjectionEncodedDocument: Uint8Array | undefined,
		documentProjectionSigner: Uint8Array | undefined,
		trimLengthTo: number,
	) => unknown[];
	prepare_plain_committed_no_next_storage_append_document_index_cached_plan_transaction_trim: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
		documentKey: string,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlanId: number,
		documentProjectionEncodedDocument: Uint8Array,
		documentProjectionSigner: Uint8Array | undefined,
		trimLengthTo: number,
	) => unknown[];
	prepare_plain_storage_append_transaction: (
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
	) => unknown[];
	prepare_plain_storage_append_transaction_trim: (
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
		trimLengthTo: number,
	) => unknown[];
	prepare_plain_storage_append_document_index_transaction: (
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
		documentKey: string,
		documentValuePrefixBytes: Uint8Array,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlan:
			| NativeBackboneSimpleDocumentProjectionPlan
			| undefined,
		documentProjectionEncodedDocument: Uint8Array | undefined,
		documentProjectionSigner: Uint8Array | undefined,
	) => unknown[];
	prepare_plain_storage_append_document_index_transaction_trim: (
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
		documentKey: string,
		documentValuePrefixBytes: Uint8Array,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlan:
			| NativeBackboneSimpleDocumentProjectionPlan
			| undefined,
		documentProjectionEncodedDocument: Uint8Array | undefined,
		documentProjectionSigner: Uint8Array | undefined,
		trimLengthTo: number,
	) => unknown[];
		prepare_plain_committed_storage_append_transaction: (
			wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
			selfReplicating: boolean,
			resolveTrimmedEntries: boolean,
		) => unknown[];
		prepare_plain_committed_storage_append_document_delete_transaction: (
			wallTime: bigint,
			logical: number,
			gid: string,
			next: string[],
			type: number,
			metaData: Uint8Array | undefined,
			payloadData: Uint8Array,
			replicas: number,
			roleAgeMs: number,
			now: string,
			selfHash: string,
			selfReplicating: boolean,
			resolveTrimmedEntries: boolean,
			documentKey: string,
		) => unknown[];
	prepare_plain_committed_storage_append_document_index_transaction: (
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
		documentKey: string,
		documentValuePrefixBytes: Uint8Array,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlan:
			| NativeBackboneSimpleDocumentProjectionPlan
			| undefined,
		documentProjectionEncodedDocument: Uint8Array | undefined,
		documentProjectionSigner: Uint8Array | undefined,
	) => unknown[];
	prepare_plain_committed_storage_append_document_index_latest_transaction: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
		documentKey: string,
		documentValuePrefixBytes: Uint8Array,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlan:
			| NativeBackboneSimpleDocumentProjectionPlan
			| undefined,
		documentProjectionEncodedDocument: Uint8Array | undefined,
		documentProjectionSigner: Uint8Array | undefined,
		trimLengthTo: number | undefined,
	) => unknown[];
		prepare_plain_committed_storage_append_document_index_latest_required_previous_signer_transaction?: (
			wallTime: bigint,
			logical: number,
			gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
		documentKey: string,
		documentValuePrefixBytes: Uint8Array,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlan:
			| NativeBackboneSimpleDocumentProjectionPlan
			| undefined,
		documentProjectionEncodedDocument: Uint8Array | undefined,
		documentProjectionSigner: Uint8Array | undefined,
			requiredPreviousSignerPublicKey: Uint8Array,
			trimLengthTo: number | undefined,
		) => unknown[];
			prepare_plain_committed_storage_append_document_index_latest_compact_transaction?: (
				wallTime: bigint,
				logical: number,
				gid: string,
			type: number,
			metaData: Uint8Array | undefined,
			payloadData: Uint8Array,
			replicas: number,
			roleAgeMs: number,
			now: string,
			selfHash: string,
			selfReplicating: boolean,
			documentKey: string,
			documentValuePrefixBytes: Uint8Array,
			documentByteElementIndexLimit: number,
			documentDeleteTrimmedHeads: boolean,
			documentProjectionPlan:
				| NativeBackboneSimpleDocumentProjectionPlan
				| undefined,
			documentProjectionEncodedDocument: Uint8Array | undefined,
				documentProjectionSigner: Uint8Array | undefined,
				trimLengthTo: number | undefined,
			) => unknown[];
			prepare_plain_committed_storage_append_document_index_latest_compact_plain_put_payload_transaction?: (
				wallTime: bigint,
				logical: number,
				gid: string,
				type: number,
				metaData: Uint8Array | undefined,
				payloadData: Uint8Array,
				replicas: number,
				roleAgeMs: number,
				now: string,
				selfHash: string,
				selfReplicating: boolean,
				documentKey: string,
				documentByteElementIndexLimit: number,
				documentDeleteTrimmedHeads: boolean,
				trimLengthTo: number | undefined,
			) => unknown[];
			prepare_plain_committed_storage_append_document_index_latest_required_previous_signer_compact_transaction?: (
				wallTime: bigint,
				logical: number,
				gid: string,
			type: number,
			metaData: Uint8Array | undefined,
			payloadData: Uint8Array,
			replicas: number,
			roleAgeMs: number,
			now: string,
			selfHash: string,
			selfReplicating: boolean,
			documentKey: string,
			documentValuePrefixBytes: Uint8Array,
			documentByteElementIndexLimit: number,
			documentDeleteTrimmedHeads: boolean,
			documentProjectionPlan:
				| NativeBackboneSimpleDocumentProjectionPlan
				| undefined,
			documentProjectionEncodedDocument: Uint8Array | undefined,
			documentProjectionSigner: Uint8Array | undefined,
			requiredPreviousSignerPublicKey: Uint8Array,
			trimLengthTo: number | undefined,
		) => unknown[];
		prepare_plain_committed_storage_append_document_index_latest_cached_plan_compact_transaction?: (
			wallTime: bigint,
			logical: number,
			gid: string,
			type: number,
			metaData: Uint8Array | undefined,
			payloadData: Uint8Array,
			replicas: number,
			roleAgeMs: number,
			now: string,
			selfHash: string,
			selfReplicating: boolean,
			documentKey: string,
			documentByteElementIndexLimit: number,
			documentDeleteTrimmedHeads: boolean,
			documentProjectionPlanId: number,
			documentProjectionEncodedDocument: Uint8Array,
			documentProjectionSigner: Uint8Array | undefined,
			trimLengthTo: number | undefined,
		) => unknown[];
		prepare_plain_committed_storage_append_document_index_latest_cached_plan_compact_plain_put_payload_transaction?: (
			wallTime: bigint,
			logical: number,
			gid: string,
			type: number,
			metaData: Uint8Array | undefined,
			payloadData: Uint8Array,
			replicas: number,
			roleAgeMs: number,
			now: string,
			selfHash: string,
			selfReplicating: boolean,
			documentKey: string,
			documentByteElementIndexLimit: number,
			documentDeleteTrimmedHeads: boolean,
			documentProjectionPlanId: number,
			documentProjectionSigner: Uint8Array | undefined,
			trimLengthTo: number | undefined,
		) => unknown[];
		prepare_plain_committed_storage_append_document_index_latest_cached_plan_transaction: (
			wallTime: bigint,
			logical: number,
			gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
		documentKey: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlanId: number,
		documentProjectionEncodedDocument: Uint8Array,
		documentProjectionSigner: Uint8Array | undefined,
		trimLengthTo: number | undefined,
	) => unknown[];
	prepare_plain_committed_storage_append_document_index_latest_batch_transaction?: (
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		gids: string[],
		type: number,
		metaDatas: Array<Uint8Array | undefined>,
		payloadDatas: Uint8Array[],
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
		documentKeys: string[],
		documentValuePrefixBytes: Uint8Array[],
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		trimLengthTo: number | undefined,
	) => unknown[][];
	prepare_plain_committed_storage_append_document_index_latest_required_previous_signer_batch_transaction?: (
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		gids: string[],
		type: number,
		metaDatas: Array<Uint8Array | undefined>,
		payloadDatas: Uint8Array[],
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
		documentKeys: string[],
		documentValuePrefixBytes: Uint8Array[],
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		requiredPreviousSignerPublicKey: Uint8Array,
		trimLengthTo: number | undefined,
	) => unknown[][];
			prepare_plain_committed_storage_append_document_index_latest_compact_batch_transaction?: (
				wallTimes: BigUint64Array,
				logicals: Uint32Array,
				gids: string[],
		type: number,
		metaDatas: Array<Uint8Array | undefined>,
		payloadDatas: Uint8Array[],
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		documentKeys: string[],
		documentValuePrefixBytes: Uint8Array[],
		documentByteElementIndexLimit: number,
				documentDeleteTrimmedHeads: boolean,
				trimLengthTo: number | undefined,
			) => unknown[][];
			prepare_plain_committed_storage_append_document_index_latest_compact_plain_put_payload_batch_transaction?: (
				wallTimes: BigUint64Array,
				logicals: Uint32Array,
				gids: string[],
				type: number,
				metaDatas: Array<Uint8Array | undefined>,
				payloadDatas: Uint8Array[],
				replicas: number,
				roleAgeMs: number,
				now: string,
				selfHash: string,
				selfReplicating: boolean,
				documentKeys: string[],
				documentByteElementIndexLimit: number,
				documentDeleteTrimmedHeads: boolean,
				trimLengthTo: number | undefined,
			) => unknown[][];
	prepare_plain_committed_storage_append_document_index_latest_required_previous_signer_compact_batch_transaction?: (
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		gids: string[],
		type: number,
		metaDatas: Array<Uint8Array | undefined>,
		payloadDatas: Uint8Array[],
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		documentKeys: string[],
		documentValuePrefixBytes: Uint8Array[],
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		requiredPreviousSignerPublicKey: Uint8Array,
		trimLengthTo: number | undefined,
	) => unknown[][];
		prepare_plain_committed_storage_append_document_index_latest_cached_plan_batch_transaction?: (
			wallTimes: BigUint64Array,
			logicals: Uint32Array,
		gids: string[],
		type: number,
		metaDatas: Array<Uint8Array | undefined>,
		payloadDatas: Uint8Array[],
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
		documentKeys: string[],
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlanIds: Uint32Array,
		documentProjectionEncodedDocuments: Uint8Array[],
			documentProjectionSigners: Array<Uint8Array | undefined>,
			trimLengthTo: number | undefined,
		) => unknown[][];
		prepare_plain_committed_storage_append_document_index_latest_required_previous_signer_cached_plan_batch_transaction?: (
			wallTimes: BigUint64Array,
			logicals: Uint32Array,
			gids: string[],
			type: number,
			metaDatas: Array<Uint8Array | undefined>,
			payloadDatas: Uint8Array[],
			replicas: number,
			roleAgeMs: number,
			now: string,
			selfHash: string,
			selfReplicating: boolean,
			resolveTrimmedEntries: boolean,
			documentKeys: string[],
			documentByteElementIndexLimit: number,
			documentDeleteTrimmedHeads: boolean,
			documentProjectionPlanIds: Uint32Array,
			documentProjectionEncodedDocuments: Uint8Array[],
			documentProjectionSigners: Array<Uint8Array | undefined>,
			requiredPreviousSignerPublicKey: Uint8Array,
			trimLengthTo: number | undefined,
		) => unknown[][];
		prepare_plain_committed_storage_append_document_index_latest_cached_plan_compact_batch_transaction?: (
			wallTimes: BigUint64Array,
			logicals: Uint32Array,
		gids: string[],
		type: number,
		metaDatas: Array<Uint8Array | undefined>,
		payloadDatas: Uint8Array[],
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		documentKeys: string[],
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlanIds: Uint32Array,
		documentProjectionEncodedDocuments: Uint8Array[],
			documentProjectionSigners: Array<Uint8Array | undefined>,
			trimLengthTo: number | undefined,
		) => unknown[][];
		prepare_plain_committed_storage_append_document_index_latest_required_previous_signer_cached_plan_compact_batch_transaction?: (
			wallTimes: BigUint64Array,
			logicals: Uint32Array,
			gids: string[],
			type: number,
			metaDatas: Array<Uint8Array | undefined>,
			payloadDatas: Uint8Array[],
			replicas: number,
			roleAgeMs: number,
			now: string,
			selfHash: string,
			selfReplicating: boolean,
			documentKeys: string[],
			documentByteElementIndexLimit: number,
			documentDeleteTrimmedHeads: boolean,
			documentProjectionPlanIds: Uint32Array,
			documentProjectionEncodedDocuments: Uint8Array[],
			documentProjectionSigners: Array<Uint8Array | undefined>,
			requiredPreviousSignerPublicKey: Uint8Array,
			trimLengthTo: number | undefined,
		) => unknown[][];
		prepare_plain_committed_storage_append_document_index_latest_cached_plan_compact_plain_put_payload_batch_transaction?: (
			wallTimes: BigUint64Array,
			logicals: Uint32Array,
		gids: string[],
		type: number,
		metaDatas: Array<Uint8Array | undefined>,
		payloadDatas: Uint8Array[],
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		documentKeys: string[],
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlanIds: Uint32Array,
		documentProjectionSigners: Array<Uint8Array | undefined>,
		trimLengthTo: number | undefined,
	) => unknown[][];
		prepare_plain_committed_storage_append_transaction_trim: (
			wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
			resolveTrimmedEntries: boolean,
			trimLengthTo: number,
		) => unknown[];
		prepare_plain_committed_storage_append_document_delete_transaction_trim: (
			wallTime: bigint,
			logical: number,
			gid: string,
			next: string[],
			type: number,
			metaData: Uint8Array | undefined,
			payloadData: Uint8Array,
			replicas: number,
			roleAgeMs: number,
			now: string,
			selfHash: string,
			selfReplicating: boolean,
			resolveTrimmedEntries: boolean,
			documentKey: string,
			trimLengthTo: number,
		) => unknown[];
	prepare_plain_committed_storage_append_document_index_transaction_trim: (
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		resolveTrimmedEntries: boolean,
		documentKey: string,
		documentValuePrefixBytes: Uint8Array,
		documentExistingCreated: string,
		documentByteElementIndexLimit: number,
		documentDeleteTrimmedHeads: boolean,
		documentProjectionPlan:
			| NativeBackboneSimpleDocumentProjectionPlan
			| undefined,
		documentProjectionEncodedDocument: Uint8Array | undefined,
		documentProjectionSigner: Uint8Array | undefined,
		trimLengthTo: number,
	) => unknown[];
};

type NativeBackboneHeadFlags = boolean[] | Uint8Array;

const nativeBackboneHeadFlagsToBytes = (
	headFlags: NativeBackboneHeadFlags,
): Uint8Array =>
	headFlags instanceof Uint8Array
		? headFlags
		: new Uint8Array(headFlags.map((head) => (head ? 1 : 0)));

type WasmModule = {
	default: (input?: unknown) => Promise<unknown>;
	initSync: (input?: unknown) => unknown;
	NativePeerbitBackbone: new (
		resolution: string,
		clockId: Uint8Array,
		privateKey: Uint8Array,
		publicKey: Uint8Array,
	) => NativePeerbitBackboneHandle;
};

let wasmModulePromise: Promise<WasmModule> | undefined;
let wasmInitialized = false;

const loadWasm = async (): Promise<WasmModule> => {
	if (!wasmModulePromise) {
		const wasmModulePath = "../wasm/native_backbone.js";
		wasmModulePromise = import(
			/* @vite-ignore */ wasmModulePath
		) as Promise<WasmModule>;
	}

	const wasm = await wasmModulePromise;
	if (!wasmInitialized) {
		const processLike = (
			globalThis as { process?: { versions?: { node?: string } } }
		).process;
		if (processLike?.versions?.node) {
			const fsPromises = "fs/promises";
			const { readFile } = (await import(
				/* @vite-ignore */ fsPromises
			)) as typeof import("fs/promises");
			const bytes = await readFile(
				new URL("../wasm/native_backbone_bg.wasm", import.meta.url),
			);
			wasm.initSync({ module: bytes });
		} else {
			await wasm.default({
				module_or_path: new URL(
					"../wasm/native_backbone_bg.wasm",
					import.meta.url,
				),
			});
		}
		wasmInitialized = true;
	}

	return wasm;
};

type NativeBackboneLeaderSample = {
	intersecting: boolean;
};

type NativeBackboneFindLeaderOptions = {
	roleAge?: number;
	now?: bigint | number | string;
	peerFilter?: Iterable<string>;
	expandPeerFilter?: boolean;
	selfHash?: string;
	selfReplicating?: boolean;
	fullReplicaFallback?: boolean;
	includeStrictFullReplica?: boolean;
};

type NativeBackboneAppendDeliveryPlan = {
	hasRemoteRecipients: boolean;
	noPeerError: boolean;
	defaultSendSilent: boolean;
	sendTo: string[];
	ackTo: string[];
	silentTo: string[];
	repairTargets: string[];
	authoritativeRecipients: string[];
};

type NativeBackboneCoordinatePlan = {
	hash: string;
	hashNumber: number | bigint;
	hashNumberString: string;
	gid: string;
	coordinates: Array<number | bigint>;
	coordinateStrings: string[];
	assignedToRangeBoundary: boolean;
	requestedReplicas: number;
};

type NativeBackboneDocumentContextFacts = {
	created: bigint;
	modified: bigint;
	head: string;
	gid: string;
	size: number;
};

export type NativeBackboneCoordinateFields = NativeBackboneCoordinatePlan & {
	wallTime: bigint;
	wallTimeString: string;
	metaBytes: Uint8Array;
};

type NativeBackboneLeaderPlan = {
	coordinates: Array<number | bigint>;
	coordinateStrings?: string[];
	leaders: Map<string, NativeBackboneLeaderSample>;
};

type NativeBackboneLeaderGidBatchInput = {
	gid: string;
	replicas: number;
};

type NativeBackboneRequestPruneHints = {
	entries: Map<
		string,
		{ hash: string; gid: string; data?: Uint8Array; replicas?: number }
	>;
	presentBlockHashes: Set<string>;
	localLeaderHashes: Set<string>;
	replicaCounts: Map<string, number>;
	peerHistoryGids: string[];
	peerHistoryRemovedHashes: Set<string>;
};

export type NativeBackboneRequestPruneHintColumns = {
	gids: Array<string | undefined>;
	data: Array<Uint8Array | undefined>;
	presentBlockFlags: Uint8Array;
	localLeaderFlags: Uint8Array;
	replicaCounts: Uint32Array;
	peerHistoryGids: string[];
	peerHistoryRemovedFlags: Uint8Array;
};

type NativeBackboneRequestPruneAllConfirmed = {
	allConfirmed: boolean;
	peerHistoryGids: string[];
};

type NativeBackboneLeaderCursorBatchInput = {
	cursors: Iterable<bigint | number | string>;
	replicas: number;
};

type NativeBackboneEntryAssignmentPlan = NativeBackboneLeaderPlan & {
	assignedToRangeBoundary: boolean;
};

type NativeBackboneRepairDispatchEntry = {
	hash: string;
	gid: string;
	requestedReplicas: number;
	coordinates: Iterable<bigint | number | string>;
};

type NativeBackboneRepairDispatchInput = {
	entries: Iterable<NativeBackboneRepairDispatchEntry>;
	pendingModes: Iterable<string>;
	pendingPeersByMode: ReadonlyMap<string, Iterable<string>>;
	optimisticPeersByMode?: ReadonlyMap<
		string,
		ReadonlyMap<string, Iterable<string>>
	>;
	fullReplicaRepairCandidates?: Iterable<string>;
	fullReplicaRepairCandidateCount: number;
	selfHash: string;
};

type NativeBackboneResidentRepairDispatchInput = Omit<
	NativeBackboneRepairDispatchInput,
	"entries"
>;

type NativeBackboneRepairDispatchPlan = Map<
	string,
	Map<string, string[]>
>;

type NativeBackboneCommittedEntry = {
	cid: string;
	hash: string;
	next: string[];
	bytes?: Uint8Array;
	metaBytes?: Uint8Array;
	byteLength: number;
	signature?: Uint8Array;
	payloadBytes?: Uint8Array;
	signatureBytes?: Uint8Array;
	hashDigestBytes?: Uint8Array;
};

type NativeBackboneStorageBackedEntry = NativeBackboneCommittedEntry & {
	bytes: Uint8Array;
};

type NativeBackboneLogEntry = {
	hash: string;
	gid: string;
	next: string[];
	type: number;
	head?: boolean;
	payloadSize?: number;
	data?: Uint8Array;
	clock: {
		timestamp: {
			wallTime: bigint | number | string;
			logical?: number;
		};
	};
};

export type NativeBackboneLogCommitEntry = NativeBackboneLogEntry & {
	bytes: Uint8Array;
};

export type NativeBackboneCoordinateCommitColumns = {
	hashes: string[];
	gids: string[];
	hashNumbers?: string[];
	hashNumberValues?: BigUint64Array;
	coordinateBatches?: string[][];
	coordinateCounts?: Uint32Array;
	coordinateValues?: BigUint64Array;
	nextHashBatches: string[][];
	assignedToRangeBoundaries: Uint8Array;
	requestedReplicas?: number[];
	requestedReplicaValues?: Uint32Array;
};

type NativeBackboneRawReceivePreparedFacts = {
	cid: string;
	hashDigestBytes: Uint8Array;
	byteLength: number;
	clockId: Uint8Array;
	wallTime: bigint;
	logical: number;
	gid: string;
	next: string[];
	type: number;
	metaBytes: Uint8Array;
	metaData?: Uint8Array;
	payloadByteLength: number;
	signatureVerified: boolean;
	requestedReplicas?: number;
	hashNumber?: string;
};

export type NativeBackboneRawReceiveGroupPlan = {
	gid: string;
	hashes: string[];
	requestedReplicas: number[];
	latestHash: string;
	maxReplicasFromHead: number;
	maxReplicasFromNewEntries: number;
	maxMaxReplicas: number;
};

export type NativeBackboneRawReceiveGroupIndexPlan = {
	gid: string;
	indexes: Uint32Array;
	requestedReplicas: number[];
	latestIndex: number;
	maxReplicasFromHead: number;
	maxReplicasFromNewEntries: number;
	maxMaxReplicas: number;
};

export type NativeBackboneRawReceiveGroupLeaderPlan =
	NativeBackboneRawReceiveGroupIndexPlan & NativeBackboneLeaderPlan;

export type NativeBackboneRawReceiveGroupAssignmentPlan =
	NativeBackboneRawReceiveGroupIndexPlan & {
		coordinates: Array<number | bigint>;
		coordinateStrings: string[];
		isLeader: boolean;
		fromIsLeader: boolean;
		assignedToRangeBoundary: boolean;
	};

type NativeBackboneRawReceiveFastDropPlan = {
	canDrop: boolean;
	groupCount: number;
	plannedHashCount: number;
};

export type NativeBackboneRawReceiveSelectionPlan = {
	retainedHashes: string[];
	droppedHashes: string[];
	retainedIndexes?: Uint32Array;
	droppedIndexes?: Uint32Array;
	groupCount: number;
	plannedHashCount: number;
	usedNativeFastDropPlan: boolean;
	usedLeaderSamplePlans: boolean;
	retainedGroupLeaderPlans?: NativeBackboneRawReceiveGroupLeaderPlan[];
};

type NativeBackbonePreparedRawReceiveColumnsAndSelection = {
	columns: NativeBackboneRawReceivePreparedFactsColumns;
	selection?: NativeBackboneRawReceiveSelectionPlan;
};

type NativeBackboneRawReceivePreparedFactsColumns = [
	string[],
	Array<Uint8Array | undefined>,
	Uint32Array,
	Uint8Array[],
	BigUint64Array,
	Uint32Array,
	string[],
	string[][],
	Uint8Array,
	Uint8Array[],
	Array<Uint8Array | undefined>,
	Uint32Array,
	Uint8Array,
	Uint32Array,
	string[] | BigUint64Array,
];

type NativeBackboneRawReceivePreparedFactsRow = [
	string,
	Uint8Array,
	number,
	Uint8Array,
	string,
	number,
	string,
	string[],
	number,
	Uint8Array,
	Uint8Array | undefined,
	number,
	boolean,
	number | undefined,
	string | undefined,
];

type NativeBackboneRawReceiveGroupPlanRow = [
	string,
	string[],
	Uint32Array,
	string,
	number,
	number,
	number,
];

type NativeBackboneRawReceiveGroupIndexPlanRow = [
	string,
	Uint32Array,
	Uint32Array,
	number,
	number,
	number,
	number,
];

type NativeBackboneRawReceiveGroupLeaderPlanRow = [
	string,
	Uint32Array,
	Uint32Array,
	number,
	number,
	number,
	number,
	unknown[],
	unknown[],
];

type NativeBackboneRawReceiveGroupAssignmentPlanRow = [
	string,
	Uint32Array,
	Uint32Array,
	number,
	number,
	number,
	number,
	unknown[],
	boolean,
	boolean,
	boolean,
];

type NativeBackboneRawReceiveSelectionRow = [
	string[],
	string[],
	number,
	number,
	boolean,
	boolean,
	NativeBackboneRawReceiveGroupLeaderPlanRow[] | undefined,
	Uint32Array?,
	Uint32Array?,
];

type NativeBackboneTrimmedEntry = {
	hash: string;
	gid: string;
	next: string[];
	type: number;
	payloadSize: number;
	data?: Uint8Array;
	clock: {
		timestamp: {
			wallTime: bigint;
			logical: number;
		};
	};
};

type NativeBackboneAppendInput = {
	wallTime: bigint | number | string;
	logical?: number;
	gid: string;
	type?: number;
	metaData?: Uint8Array;
	payloadData: Uint8Array;
	replicas: number;
	roleAgeMs?: number;
	now?: bigint | number | string;
	selfHash?: string;
	selfReplicating?: boolean;
	trimLengthTo?: number;
	resolveTrimmedEntries?: boolean;
	documentIndex?: {
		key: string;
		valuePrefixBytes?: Uint8Array;
		usePlainPutPayload?: boolean;
		projection?: {
			encodedDocument: Uint8Array;
			plan: NativeBackboneSimpleDocumentProjectionPlan;
			signer?: Uint8Array;
		};
		existingCreated?: bigint | number | string;
		byteElementIndexLimit?: number;
		deleteTrimmedHeads?: boolean;
		useLatestContext?: boolean;
		requiredPreviousSignerPublicKey?: Uint8Array;
	};
	documentDeleteKey?: string;
};

type NativeBackboneCommittedNoNextDocumentIndexBatchInput = {
	entries: Array<{
		wallTime: bigint | number | string;
		logical?: number;
		gid: string;
		metaData?: Uint8Array;
		payloadData: Uint8Array;
		documentIndex: NonNullable<NativeBackboneAppendInput["documentIndex"]>;
	}>;
	type?: number;
	replicas: number;
	roleAgeMs?: number;
	now?: bigint | number | string;
	selfHash?: string;
	selfReplicating?: boolean;
	trimLengthTo?: number;
	documentByteElementIndexLimit?: number;
	documentDeleteTrimmedHeads?: boolean;
};

type NativeBackboneCommittedLatestDocumentIndexBatchInput = {
	entries: Array<{
		wallTime: bigint | number | string;
		logical?: number;
		gid: string;
		metaData?: Uint8Array;
		payloadData: Uint8Array;
		documentIndex: NonNullable<NativeBackboneAppendInput["documentIndex"]>;
	}>;
	type?: number;
	replicas: number;
	roleAgeMs?: number;
	now?: bigint | number | string;
	selfHash?: string;
	selfReplicating?: boolean;
	resolveTrimmedEntries?: boolean;
	trimLengthTo?: number;
	documentByteElementIndexLimit?: number;
	documentDeleteTrimmedHeads?: boolean;
};

type NativeBackboneStorageAppendInput = NativeBackboneAppendInput & {
	next?: Iterable<string>;
};

export type NativeBackboneAppendResult = {
	entry: NativeBackboneCommittedEntry;
	coordinate: NativeBackboneCoordinatePlan;
	leaders?: Map<string, NativeBackboneLeaderSample>;
	isLeader: boolean;
	assignedToRangeBoundary: boolean;
	trimmed: NativeBackboneTrimmedEntry[];
	trimmedHashes?: string[];
	documentTrimmedHeadsProcessed?: boolean;
	documentPreviousContext?: NativeBackboneDocumentContextFacts;
};

type NativeBackboneStorageAppendResult = {
	entry: NativeBackboneStorageBackedEntry;
	coordinate: NativeBackboneCoordinatePlan;
	leaders?: Map<string, NativeBackboneLeaderSample>;
	isLeader: boolean;
	assignedToRangeBoundary: boolean;
	trimmed: NativeBackboneTrimmedEntry[];
	trimmedHashes?: string[];
	documentTrimmedHeadsProcessed?: boolean;
	documentPreviousContext?: NativeBackboneDocumentContextFacts;
};

type NativeBackboneLogGraphOptions = {
	commitBlocks?: boolean;
	documentProjectionPlanId?: (
		plan: NativeBackboneSimpleDocumentProjectionPlan,
	) => number;
};

type NativeBackboneLoopBenchmark = {
	totalMs: number;
	logLength: number;
	blockLength: number;
	coordinateLength: number;
	documentLength: number;
};

type NativeBackboneAppendPlan = {
	coordinates: Array<number | bigint>;
	leaders?: Map<string, NativeBackboneLeaderSample>;
	isLeader: boolean;
	assignedToRangeBoundary: boolean;
	delivery?: NativeBackboneAppendDeliveryPlan;
	coordinate: NativeBackboneCoordinatePlan;
};

type NativeBackboneReceiveCoordinatePlan = {
	coordinates: Array<number | bigint>;
	leaders?: Map<string, NativeBackboneLeaderSample>;
	isLeader: boolean;
	assignedToRangeBoundary: boolean;
	coordinate: NativeBackboneCoordinatePlan;
};

type NativeBackboneAppendEntryBatchInput = {
	entryHash: string;
	gid: string;
	hashNumber?: bigint | number | string;
	nextHashes?: Iterable<string>;
	replicas: number;
};

type NativeBackboneRangeInput = {
	id: string;
	hash: string;
	timestamp: bigint | number | string;
	start1: bigint | number | string;
	end1: bigint | number | string;
	start2: bigint | number | string;
	end2: bigint | number | string;
	width: bigint | number | string;
	mode: number;
};

type NativeBackboneDocumentSchemaStats = {
	rootFields: number;
	nodeCount: number;
	genericNodes: number;
};

export type NativeBackboneSimpleDocumentProjectionPlan = {
	documentVariantType?: "u8" | "string";
	documentVariantValue?: string;
	documentFieldNames: string[];
	documentFieldTypes: string[];
	outputVariantType?: "u8" | "string";
	outputVariantValue?: string;
	outputFieldTypes: string[];
	sourceKinds: string[];
	sourceValues: string[];
};

type NativeBackboneSimpleDocumentProjectionContext = {
	created: bigint | number | string;
	modified: bigint | number | string;
	head?: string;
	gid: string;
	size: number;
	signer?: Uint8Array;
};

export type NativeBackboneAppendProfile = {
	nativeBackboneStorageAppendInnerMs: number;
	nativeBackboneInputCopyMs: number;
	nativeBackboneLogTotalMs: number;
	nativeBackboneLogNextCloneMs: number;
	nativeBackboneLogEntryCoreMs: number;
	nativeBackboneLogEncodeMetaMs: number;
	nativeBackboneLogEncodePayloadMs: number;
	nativeBackboneLogEncodeSignableMs: number;
	nativeBackboneLogSignMs: number;
	nativeBackboneLogEncodeSignatureMs: number;
	nativeBackboneLogEncodeStorageMs: number;
	nativeBackboneLogCidMs: number;
	nativeBackboneLogCidHashMs: number;
	nativeBackboneLogCidStringMs: number;
	nativeBackboneLogIndexEntryMs: number;
	nativeBackboneLogFactsMs: number;
	nativeBackboneLogBlockPutMs: number;
	nativeBackboneLogGraphPutMs: number;
	nativeBackboneLogTrimMs: number;
	nativeBackboneEntryRowMs: number;
	nativeBackboneTrimRowsMs: number;
	nativeBackboneHashNumberMs: number;
	nativeBackboneCoordinatePlanMs: number;
	nativeBackboneCoordinateCoreMs: number;
	nativeBackboneCoordinateFieldsBuildMs: number;
	nativeBackboneCoordinateValueEncodeMs: number;
	nativeBackboneCoordinateJournalPutMs: number;
	nativeBackboneCoordinateIndexPutMs: number;
	nativeBackboneCoordinateValuePutMs: number;
	nativeBackboneCoordinateDeleteMs: number;
	nativeBackboneDocumentIndexCommitMs: number;
	nativeBackboneDocumentIndexContextEncodeMs: number;
	nativeBackboneDocumentIndexExtractMs: number;
	nativeBackboneDocumentIndexValueBuildMs: number;
	nativeBackboneDocumentIndexPutMs: number;
	nativeBackboneDocumentValuePutMs: number;
	nativeBackboneDocumentIndexTrimDeleteMs: number;
	nativeBackboneResultRowMs: number;
	nativeBackboneRawReceiveInputCopyMs: number;
	nativeBackboneRawReceivePrepareMs: number;
	nativeBackboneRawReceiveDigestMs: number;
	nativeBackboneRawReceiveCidStringMs: number;
	nativeBackboneRawReceiveExpectedCidMs: number;
	nativeBackboneRawReceiveStorageParseMs: number;
	nativeBackboneRawReceiveMetaParseMs: number;
	nativeBackboneRawReceivePayloadParseMs: number;
	nativeBackboneRawReceiveSignatureParseMs: number;
	nativeBackboneRawReceiveSignableMs: number;
	nativeBackboneRawReceiveVerifyBatchMs: number;
	nativeBackboneRawReceiveVerifyFallbackMs: number;
	nativeBackboneRawReceivePrepareColumnsMs: number;
	nativeBackboneRawReceivePendingCheckMs: number;
	nativeBackboneRawReceiveVerifyMs: number;
	nativeBackboneRawReceiveVerifyStatusMs: number;
	nativeBackboneRawReceiveJoinPlanMs: number;
	nativeBackboneRawReceiveRemoveMs: number;
	nativeBackboneRawReceiveBlockPutMs: number;
	nativeBackboneRawReceiveGraphPutMs: number;
	nativeBackboneRawReceiveCoordinateCommitMs: number;
};

const nativeBackboneAppendProfileKeys = [
	"nativeBackboneStorageAppendInnerMs",
	"nativeBackboneInputCopyMs",
	"nativeBackboneLogTotalMs",
	"nativeBackboneLogNextCloneMs",
	"nativeBackboneLogEntryCoreMs",
	"nativeBackboneLogEncodeMetaMs",
	"nativeBackboneLogEncodePayloadMs",
	"nativeBackboneLogEncodeSignableMs",
	"nativeBackboneLogSignMs",
	"nativeBackboneLogEncodeSignatureMs",
	"nativeBackboneLogEncodeStorageMs",
	"nativeBackboneLogCidMs",
	"nativeBackboneLogCidHashMs",
	"nativeBackboneLogCidStringMs",
	"nativeBackboneLogIndexEntryMs",
	"nativeBackboneLogFactsMs",
	"nativeBackboneLogBlockPutMs",
	"nativeBackboneLogGraphPutMs",
	"nativeBackboneLogTrimMs",
	"nativeBackboneEntryRowMs",
	"nativeBackboneTrimRowsMs",
	"nativeBackboneHashNumberMs",
	"nativeBackboneCoordinatePlanMs",
	"nativeBackboneCoordinateCoreMs",
	"nativeBackboneCoordinateFieldsBuildMs",
	"nativeBackboneCoordinateValueEncodeMs",
	"nativeBackboneCoordinateJournalPutMs",
	"nativeBackboneCoordinateIndexPutMs",
	"nativeBackboneCoordinateValuePutMs",
	"nativeBackboneCoordinateDeleteMs",
	"nativeBackboneDocumentIndexCommitMs",
	"nativeBackboneDocumentIndexContextEncodeMs",
	"nativeBackboneDocumentIndexExtractMs",
	"nativeBackboneDocumentIndexValueBuildMs",
	"nativeBackboneDocumentIndexPutMs",
	"nativeBackboneDocumentValuePutMs",
	"nativeBackboneDocumentIndexTrimDeleteMs",
	"nativeBackboneResultRowMs",
	"nativeBackboneRawReceiveInputCopyMs",
	"nativeBackboneRawReceivePrepareMs",
	"nativeBackboneRawReceiveDigestMs",
	"nativeBackboneRawReceiveCidStringMs",
	"nativeBackboneRawReceiveExpectedCidMs",
	"nativeBackboneRawReceiveStorageParseMs",
	"nativeBackboneRawReceiveMetaParseMs",
	"nativeBackboneRawReceivePayloadParseMs",
	"nativeBackboneRawReceiveSignatureParseMs",
	"nativeBackboneRawReceiveSignableMs",
	"nativeBackboneRawReceiveVerifyBatchMs",
	"nativeBackboneRawReceiveVerifyFallbackMs",
	"nativeBackboneRawReceivePrepareColumnsMs",
	"nativeBackboneRawReceivePendingCheckMs",
	"nativeBackboneRawReceiveVerifyMs",
	"nativeBackboneRawReceiveVerifyStatusMs",
	"nativeBackboneRawReceiveJoinPlanMs",
	"nativeBackboneRawReceiveRemoveMs",
	"nativeBackboneRawReceiveBlockPutMs",
	"nativeBackboneRawReceiveGraphPutMs",
	"nativeBackboneRawReceiveCoordinateCommitMs",
] as const satisfies readonly (keyof NativeBackboneAppendProfile)[];

type NativeBackboneDocumentEntry = [key: string, value: Uint8Array];
type NativeBackboneDocumentFieldValue =
	| ["bool", boolean]
	| ["i64", string]
	| ["u64", string]
	| ["string", string]
	| ["bytes", Uint8Array];

export type NativeBackboneOptions = {
	resolution?: RangeResolution;
	clockId: Uint8Array;
	privateKey: Uint8Array;
	publicKey: Uint8Array;
};

type NativeBackboneCoordinatePersistenceFiles = {
	snapshot?: string;
	journal?: string;
	documentSnapshot?: string;
	documentJournal?: string;
	documentSignerSnapshot?: string;
	documentSignerJournal?: string;
};

export type NativeBackboneCoordinatePersistenceOptions =
	NativeBackboneCoordinatePersistenceFiles & {
		flushOnAppend?: boolean;
		flushMaxPendingBytes?: number;
		flushIntervalMs?: number;
		compactMaxJournalBytes?: number;
		compactMaxJournalRecords?: number;
	};

export type NativeBackboneCoordinatePersistenceStore = {
	read(name: string): Promise<Uint8Array | undefined>;
	write(name: string, bytes: Uint8Array): Promise<void>;
	append(name: string, bytes: Uint8Array): Promise<void>;
	remove?(name: string): Promise<void>;
	flush?(): Promise<void>;
	close?(): Promise<void>;
};

export type NativeBackboneCoordinatePersistenceAdapter = {
	flushOnAppend?: boolean;
	flushMaxPendingBytes?: number;
	flushIntervalMs?: number;
	compactMaxJournalBytes?: number;
	compactMaxJournalRecords?: number;
	hydrate(backbone: NativePeerbitBackbone): Promise<number>;
	flushJournal(backbone: NativePeerbitBackbone): Promise<number>;
	flushJournalOnAppend?(
		backbone: NativePeerbitBackbone,
	): number | Promise<number>;
	compact?(backbone: NativePeerbitBackbone): Promise<void>;
	close?(): Promise<void>;
};

export type NativeBackboneCoordinatePersistenceConfig =
	| NativeBackboneCoordinatePersistenceAdapter
	| (NativeBackboneCoordinatePersistenceOptions & {
			store: NativeBackboneCoordinatePersistenceStore;
			buffered?: boolean | { maxBufferedBytes?: number };
	  });

export type NativeBackboneBufferedCoordinatePersistenceOptions =
	NativeBackboneCoordinatePersistenceFiles & {
		flushMaxPendingBytes?: number;
		flushIntervalMs?: number;
		maxBufferedBytes?: number;
		compactMaxJournalBytes?: number;
		compactMaxJournalRecords?: number;
	};

export type NativeBackboneNodeCoordinatePersistenceOptions =
	NativeBackboneCoordinatePersistenceOptions & {
		fs?: NativeBackboneNodeFs;
		writeBufferMaxBytes?: number;
	};

const nativeBackboneCoordinatePersistenceFiles = {
	snapshot: "coordinates.bin",
	journal: "coordinates.wal",
	documentSnapshot: "document-values.bin",
	documentJournal: "document-values.wal",
	documentSignerSnapshot: "document-signers.bin",
	documentSignerJournal: "document-signers.wal",
} as const;

export const defaultNativeBackboneCoordinateFlushMaxPendingBytes = 1024 * 1024;
export const defaultNativeBackboneCoordinateCompactMaxJournalBytes =
	64 * 1024 * 1024;

type NativeBackboneNodeFs = {
	mkdir(path: string, options?: { recursive?: boolean }): Promise<unknown>;
	readFile(path: string): Promise<Uint8Array>;
	writeFile(path: string, data: Uint8Array): Promise<unknown>;
	appendFile(path: string, data: Uint8Array): Promise<unknown>;
	open?(
		path: string,
		flags: string,
	): Promise<NativeBackboneNodeAppendFileHandle>;
	rm(path: string, options?: { force?: boolean }): Promise<unknown>;
};

type NativeBackboneNodeAppendFileHandle = {
	write(data: Uint8Array): Promise<unknown>;
	close(): Promise<unknown>;
};

type NativeBackboneOPFSFile = {
	arrayBuffer(): Promise<ArrayBuffer>;
	size: number;
};

type NativeBackboneOPFSSyncAccessHandle = {
	getSize(): number;
	write(buffer: Uint8Array, options?: { at?: number }): number;
	flush?(): void;
	close(): void;
};

type NativeBackboneOPFSWritable = {
	seek(position: number): Promise<void>;
	write(data: Uint8Array): Promise<void>;
	close(): Promise<void>;
};

export type NativeBackboneOPFSFileHandle = {
	getFile(): Promise<NativeBackboneOPFSFile>;
	createWritable(options?: {
		keepExistingData?: boolean;
	}): Promise<NativeBackboneOPFSWritable>;
	createSyncAccessHandle?: () => Promise<NativeBackboneOPFSSyncAccessHandle>;
};

export type NativeBackboneOPFSDirectoryHandle = {
	getDirectoryHandle(
		name: string,
		options?: { create?: boolean },
	): Promise<NativeBackboneOPFSDirectoryHandle>;
	getFileHandle(
		name: string,
		options?: { create?: boolean },
	): Promise<NativeBackboneOPFSFileHandle>;
	removeEntry(name: string): Promise<void>;
};

const isNotFoundError = (error: unknown): boolean => {
	const maybeError = error as { code?: string; name?: string } | undefined;
	return maybeError?.code === "ENOENT" || maybeError?.name === "NotFoundError";
};

const validateCoordinatePersistenceName = (name: string): string => {
	if (
		name.length === 0 ||
		name === "." ||
		name === ".." ||
		name.includes("/") ||
		name.includes("\\")
	) {
		throw new Error(
			`Invalid native backbone coordinate persistence file: ${name}`,
		);
	}
	return name;
};

const concatBytes = (chunks: Uint8Array[]): Uint8Array => {
	const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
	const out = new Uint8Array(total);
	let offset = 0;
	for (const chunk of chunks) {
		out.set(chunk, offset);
		offset += chunk.byteLength;
	}
	return out;
};

const copyBytes = (bytes: Uint8Array): Uint8Array => bytes.slice();

const rowsToNumbers = (
	resolution: RangeResolution,
	rows: unknown[],
): Array<number | bigint> =>
	rows.map((row) => {
		const value = row as string;
		return resolution === "u64" ? BigInt(value) : Number(value);
	});

const rowsToHashNumberMap = (rows: unknown[]): Map<bigint, string[]> => {
	const out = new Map<bigint, string[]>();
	for (const row of rows) {
		const [hashNumber, hashes] = row as [string, string[]];
		out.set(BigInt(hashNumber), hashes);
	}
	return out;
};

const rowsToSamples = (
	rows: unknown[] | undefined,
): Map<string, NativeBackboneLeaderSample> | undefined => {
	if (!rows) {
		return undefined;
	}
	const out = new Map<string, NativeBackboneLeaderSample>();
	for (const row of rows) {
		const [hash, intersecting] = row as [string, boolean];
		out.set(hash, { intersecting });
	}
	return out;
};

const rowsToRepairDispatchPlan = (
	rows: unknown[],
): NativeBackboneRepairDispatchPlan => {
	const plan: NativeBackboneRepairDispatchPlan = new Map();
	for (const row of rows) {
		const [mode, target, hashes] = row as [string, string, string[]];
		let targets = plan.get(mode);
		if (!targets) {
			targets = new Map();
			plan.set(mode, targets);
		}
		targets.set(target, hashes);
	}
	return plan;
};

const appendCoordinatePlanFromRow = (
	resolution: RangeResolution,
	row: unknown[],
): NativeBackboneCoordinatePlan => {
	const [
		hash,
		hashNumber,
		gid,
		coordinateRows,
		assignedToRangeBoundary,
		requestedReplicas,
	] = row as [string, unknown, string, unknown[], boolean, number];
	const coordinateStrings = coordinateRows.map((coordinate) =>
		String(coordinate),
	);
	return {
		hash,
		hashNumber: rowsToNumbers(resolution, [hashNumber])[0]!,
		hashNumberString:
			typeof hashNumber === "string" ? hashNumber : String(hashNumber),
		gid,
		coordinates: rowsToNumbers(resolution, coordinateStrings),
		coordinateStrings,
		assignedToRangeBoundary,
		requestedReplicas,
	};
};

const appendCoordinatePlanFromCompactNoNextRow = (
	resolution: RangeResolution,
	hash: string,
	row: unknown[],
): NativeBackboneCoordinatePlan => {
	const [
		hashNumber,
		gid,
		coordinateRows,
		assignedToRangeBoundary,
		requestedReplicas,
	] = row as [unknown, string, unknown[], boolean, number];
	const coordinateStrings = coordinateRows.map((coordinate) =>
		String(coordinate),
	);
	return {
		hash,
		hashNumber: rowsToNumbers(resolution, [hashNumber])[0]!,
		hashNumberString:
			typeof hashNumber === "string" ? hashNumber : String(hashNumber),
		gid,
		coordinates: rowsToNumbers(resolution, coordinateStrings),
		coordinateStrings,
		assignedToRangeBoundary,
		requestedReplicas,
	};
};

const documentContextFactsFromRow = (
	row: unknown[] | undefined,
): NativeBackboneDocumentContextFacts | undefined => {
	if (!row) {
		return undefined;
	}
	const [created, modified, head, gid, size] = row as [
		string,
		string,
		string,
		string,
		number,
	];
	return {
		created: BigInt(created),
		modified: BigInt(modified),
		head,
		gid,
		size,
	};
};

const coordinateFieldsFromRow = (
	resolution: RangeResolution,
	row: unknown[],
): NativeBackboneCoordinateFields => {
	const [
		hash,
		hashNumber,
		gid,
		coordinateRows,
		assignedToRangeBoundary,
		requestedReplicas,
		wallTime,
		metaBytes,
	] = row as [
		string,
		unknown,
		string,
		unknown[],
		boolean,
		number,
		string,
		Uint8Array,
	];
	const coordinate = appendCoordinatePlanFromRow(resolution, [
		hash,
		hashNumber,
		gid,
		coordinateRows,
		assignedToRangeBoundary,
		requestedReplicas,
	]);
	const wallTimeString =
		typeof wallTime === "string" ? wallTime : String(wallTime);
	return {
		...coordinate,
		wallTime: BigInt(wallTimeString),
		wallTimeString,
		metaBytes,
	};
};

const appendDeliveryPlanFromRow = (
	row: [
		boolean,
		boolean,
		boolean,
		string[],
		string[],
		string[],
		string[],
		string[],
	],
): NativeBackboneAppendDeliveryPlan => ({
	hasRemoteRecipients: row[0],
	noPeerError: row[1],
	defaultSendSilent: row[2],
	sendTo: row[3],
	ackTo: row[4],
	silentTo: row[5],
	repairTargets: row[6],
	authoritativeRecipients: row[7],
});

const committedEntryFromRow = (
	row: unknown[],
): NativeBackboneCommittedEntry => {
	const [hash, metaBytes, byteLength, hashDigestBytes] = row as [
		string,
		Uint8Array | undefined,
		number,
		Uint8Array | undefined,
	];
	return {
		cid: hash,
		hash,
		next: [],
		metaBytes,
		byteLength,
		hashDigestBytes,
	};
};

const committedStorageFactsEntryFromRow = (
	row: unknown[],
): NativeBackboneCommittedEntry => {
	if (row.length === 4) {
		return committedEntryFromRow(row);
	}
	const [hash, next, metaBytes, byteLength, hashDigestBytes] = row as [
		string,
		string[],
		Uint8Array | undefined,
		number,
		Uint8Array | undefined,
	];
	return {
		cid: hash,
		hash,
		next,
		metaBytes,
		byteLength,
		hashDigestBytes,
	};
};

const storageFactsEntryFromRow = (
	row: unknown[],
): NativeBackboneStorageBackedEntry => {
	const [bytes, cid, next, byteLength, metaBytes, hashDigestBytes] = row as [
		Uint8Array,
		string,
		string[],
		number,
		Uint8Array | undefined,
		Uint8Array | undefined,
	];
	return {
		cid,
		hash: cid,
		next,
		bytes,
		byteLength,
		metaBytes,
		hashDigestBytes,
	};
};

const trimmedEntryFromRow = (row: unknown): NativeBackboneTrimmedEntry => {
	const [hash, gid, next, type, wallTime, logical, payloadSize, data] = row as [
		string,
		string,
		string[],
		number,
		string,
		number,
		number,
		Uint8Array | undefined,
	];
	return {
		hash,
		gid,
		next,
		type,
		payloadSize,
		data,
		clock: {
			timestamp: {
				wallTime: BigInt(wallTime),
				logical,
			},
		},
	};
};

const trimmedHashFromRow = (row: unknown): string => (row as [string])[0];

const trimmedRowsResult = (
	rows: unknown[],
): {
	readonly trimmed: NativeBackboneTrimmedEntry[];
	readonly trimmedHashes: string[];
} => {
	let trimmed: NativeBackboneTrimmedEntry[] | undefined;
	const trimmedHashes = rows.map(trimmedHashFromRow);
	return {
		get trimmed() {
			return (trimmed ??= rows.map(trimmedEntryFromRow));
		},
		trimmedHashes,
	};
};

const trimmedRowsAndHashesResult = (
	rows: unknown[],
	hashRows?: unknown[],
): {
	readonly trimmed: NativeBackboneTrimmedEntry[];
	readonly trimmedHashes: string[];
} => {
	if (!hashRows) {
		return trimmedRowsResult(rows);
	}
	let trimmed: NativeBackboneTrimmedEntry[] | undefined;
	const trimmedHashes = hashRows as string[];
	return {
		get trimmed() {
			return (trimmed ??= rows.map(trimmedEntryFromRow));
		},
		trimmedHashes,
	};
};

const nativeLogEntryFromTrimRow = (row: unknown): NativeBackboneLogEntry => {
	const entry = trimmedEntryFromRow(row);
	return {
		...entry,
		clock: {
			timestamp: {
				wallTime: entry.clock.timestamp.wallTime,
				logical: entry.clock.timestamp.logical,
			},
		},
	};
};

const headEntryFromRow = (row: unknown) => {
	const [hash, gid, wallTime, logical] = row as [
		string,
		string,
		string,
		number,
	];
	return {
		hash,
		meta: {
			gid,
			clock: { timestamp: { wallTime: BigInt(wallTime), logical } },
		},
	};
};

const joinHeadEntryFromRow = (row: unknown) => {
	const [hash, gid, wallTime, logical, type, next] = row as [
		string,
		string,
		string,
		number,
		number,
		string[],
	];
	return {
		hash,
		meta: {
			gid,
			type,
			next,
			clock: { timestamp: { wallTime: BigInt(wallTime), logical } },
		},
	};
};

const headDataEntryFromRow = (row: unknown) => {
	const [hash, data] = row as [string, Uint8Array | undefined];
	return { hash, meta: { data } };
};

const metadataEntryFromRow = (row: unknown) => {
	if (row == null) {
		return undefined;
	}
	const [hash, gid, data, replicas] = row as [
		string,
		string,
		Uint8Array | undefined,
		number | undefined,
	];
	const entry: { hash: string; gid: string; data?: Uint8Array; replicas?: number } =
		{ hash, gid, data };
	if (replicas != null) {
		entry.replicas = replicas;
	}
	return entry;
};

const requestPruneEntryFromRow = (
	row: unknown,
): { hash: string; gid: string; data?: Uint8Array; replicas?: number } => {
	const [hash, gid, replicas, data] = row as [
		string,
		string,
		number | undefined,
		Uint8Array | undefined,
	];
	const entry: { hash: string; gid: string; data?: Uint8Array; replicas?: number } =
		{ hash, gid };
	if (replicas != null) {
		entry.replicas = replicas;
	}
	if (data != null) {
		entry.data = data;
	}
	return entry;
};

const storageAppendResultFromRow = (
	resolution: RangeResolution,
	row: unknown[],
): NativeBackboneStorageAppendResult => {
	const [
		entryRow,
		leaderRows,
		isLeader,
		assignedToRangeBoundary,
		coordinateRow,
		trimRows,
		trimHashRows,
		documentTrimmedHeadsProcessed,
		documentPreviousContextRow,
	] = row as [
		unknown[],
		unknown[] | undefined,
		boolean,
		boolean,
		unknown[],
		unknown[],
		unknown[] | undefined,
		boolean | undefined,
		unknown[] | undefined,
	];
	return {
		entry: storageFactsEntryFromRow(entryRow),
		leaders: rowsToSamples(leaderRows),
		isLeader,
		assignedToRangeBoundary,
		coordinate: appendCoordinatePlanFromRow(resolution, coordinateRow),
		...trimmedRowsAndHashesResult(trimRows, trimHashRows),
		documentTrimmedHeadsProcessed,
		documentPreviousContext: documentContextFactsFromRow(
			documentPreviousContextRow,
		),
	};
};

const committedStorageAppendResultFromRow = (
	resolution: RangeResolution,
	row: unknown[],
): NativeBackboneAppendResult => {
	const [
		entryRow,
		leaderRows,
		isLeader,
		assignedToRangeBoundary,
		coordinateRow,
		trimRows,
		trimHashRows,
		documentTrimmedHeadsProcessed,
		documentPreviousContextRow,
	] = row as [
		unknown[],
		unknown[] | undefined,
		boolean,
		boolean,
		unknown[],
		unknown[],
		unknown[] | undefined,
		boolean | undefined,
		unknown[] | undefined,
	];
	return {
		entry: committedStorageFactsEntryFromRow(entryRow),
		leaders: rowsToSamples(leaderRows),
		isLeader,
		assignedToRangeBoundary,
		coordinate: appendCoordinatePlanFromRow(resolution, coordinateRow),
		...trimmedRowsAndHashesResult(trimRows, trimHashRows),
		documentTrimmedHeadsProcessed,
		documentPreviousContext: documentContextFactsFromRow(
			documentPreviousContextRow,
		),
	};
};

const compactCommittedNoNextStorageAppendResultFromRow = (
	resolution: RangeResolution,
	row: unknown[],
): NativeBackboneAppendResult => {
	const [hash, byteLength, metaBytes, fourth] = row as [
		string,
		number,
		Uint8Array | undefined,
		Uint8Array | unknown[] | string | number,
	];
	const hasDigestRow = fourth instanceof Uint8Array;
	const hashDigestBytes = hasDigestRow ? (fourth as Uint8Array) : undefined;
	const rest = row.slice(hasDigestRow ? 4 : 3);
	const usesNestedCoordinateRow = Array.isArray(rest[0]);
	let coordinate: NativeBackboneCoordinatePlan;
	let leaderRows: unknown[] | undefined;
	let isLeader: boolean;
	let trimHashRows: string[] | undefined;
	let documentTrimmedHeadsProcessed: boolean | undefined;
	let bytes: Uint8Array | undefined;
	if (usesNestedCoordinateRow) {
		const [
			coordinateRow,
			nestedLeaderRows,
			nestedIsLeader,
			nestedTrimHashRows,
			nestedDocumentTrimmedHeadsProcessed,
			nestedBytes,
		] = rest as [
			unknown[],
			unknown[] | undefined,
			boolean,
			string[] | undefined,
			boolean | undefined,
			Uint8Array | undefined,
		];
		coordinate = appendCoordinatePlanFromRow(resolution, coordinateRow);
		leaderRows = nestedLeaderRows;
		isLeader = nestedIsLeader;
		trimHashRows = nestedTrimHashRows;
		documentTrimmedHeadsProcessed = nestedDocumentTrimmedHeadsProcessed;
		bytes = nestedBytes instanceof Uint8Array ? nestedBytes : undefined;
	} else {
		const [
			hashNumber,
			gid,
			coordinateRows,
			assignedToRangeBoundary,
			requestedReplicas,
			flatLeaderRows,
			flatIsLeader,
			flatTrimHashRows,
			flatDocumentTrimmedHeadsProcessed,
			flatBytes,
		] = rest as [
			unknown,
			string,
			unknown[],
			boolean,
			number,
			unknown[] | undefined,
			boolean,
			string[] | undefined,
			boolean | undefined,
			Uint8Array | undefined,
		];
		coordinate = appendCoordinatePlanFromCompactNoNextRow(resolution, hash, [
			hashNumber,
			gid,
			coordinateRows,
			assignedToRangeBoundary,
			requestedReplicas,
		]);
		leaderRows = flatLeaderRows;
		isLeader = flatIsLeader;
		trimHashRows = flatTrimHashRows;
		documentTrimmedHeadsProcessed = flatDocumentTrimmedHeadsProcessed;
		bytes = flatBytes instanceof Uint8Array ? flatBytes : undefined;
	}
	return {
		entry: {
			cid: hash,
			hash,
			next: [],
			bytes,
			metaBytes,
			byteLength,
			hashDigestBytes,
		},
		leaders: rowsToSamples(leaderRows),
		isLeader,
		assignedToRangeBoundary: coordinate.assignedToRangeBoundary,
		coordinate,
		trimmed: [],
		trimmedHashes: trimHashRows ?? [],
		documentTrimmedHeadsProcessed,
	};
};

const compactCommittedLatestStorageAppendResultFromRow = (
	resolution: RangeResolution,
	row: unknown[],
): NativeBackboneAppendResult => {
	const [
		hash,
		byteLength,
		metaBytes,
		fourth,
		fifth,
		sixth,
		seventh,
		eighth,
		ninth,
		tenth,
		eleventh,
		twelfth,
	] = row as [
		string,
		number,
		Uint8Array | undefined,
		Uint8Array | string[],
		string[] | unknown[],
		unknown[] | undefined,
		unknown[] | boolean,
		boolean | string[] | undefined,
		string[] | boolean | undefined,
		boolean | unknown[] | undefined,
		unknown[] | undefined,
		Uint8Array | undefined,
	];
	const hasDigestRow = fourth instanceof Uint8Array;
	const hashDigestBytes = hasDigestRow ? fourth : undefined;
	const next = (hasDigestRow ? fifth : fourth) as string[];
	const coordinateRow = (hasDigestRow ? sixth : fifth) as unknown[];
	const leaderRows = (hasDigestRow ? seventh : sixth) as unknown[] | undefined;
	const isLeader = (hasDigestRow ? eighth : seventh) as boolean;
	const trimHashRows = (hasDigestRow ? ninth : eighth) as string[] | undefined;
	const documentTrimmedHeadsProcessed = (hasDigestRow ? tenth : ninth) as
		| boolean
		| undefined;
	const documentPreviousContextRow = (hasDigestRow ? eleventh : tenth) as
		| unknown[]
		| undefined;
	const bytes = (hasDigestRow ? twelfth : eleventh) as Uint8Array | undefined;
	const coordinate = appendCoordinatePlanFromRow(resolution, coordinateRow);
	return {
		entry: {
			cid: hash,
			hash,
			next,
			bytes: bytes instanceof Uint8Array ? bytes : undefined,
			metaBytes,
			byteLength,
			hashDigestBytes,
		},
		leaders: rowsToSamples(leaderRows),
		isLeader,
		assignedToRangeBoundary: coordinate.assignedToRangeBoundary,
		coordinate,
		trimmed: [],
		trimmedHashes: trimHashRows ?? [],
		documentTrimmedHeadsProcessed,
		documentPreviousContext: documentContextFactsFromRow(
			documentPreviousContextRow,
		),
	};
};

const preparedCommitFactsFromRow = (
	row: unknown[],
): NativeBackboneCommittedEntry & {
	trimmedEntries?: NativeBackboneLogEntry[];
} => {
	const isTrimRow =
		Array.isArray(row) &&
		row.length === 2 &&
		Array.isArray(row[0]) &&
		Array.isArray(row[1]);
	const entryRow = (isTrimRow ? row[0] : row) as unknown[];
	const prepared = committedStorageFactsEntryFromRow(entryRow);
	if (isTrimRow) {
		return {
			...prepared,
			trimmedEntries: (row[1] as unknown[]).map(nativeLogEntryFromTrimRow),
		};
	}
	return prepared;
};

const preparedCommitFactsWithLatestDocumentContextFromRow = (
	row: unknown[],
): NativeBackboneCommittedEntry & {
	trimmedEntryHashes?: string[];
	documentTrimmedHeadsProcessed?: boolean;
	documentPreviousContext?: NativeBackboneDocumentContextFacts;
} => {
	const [entryRow, trimHashRows, documentTrimmedHeadsProcessed, contextRow] =
		row as [
			unknown[],
			string[] | undefined,
			boolean | undefined,
			unknown[] | undefined,
		];
	return {
		...committedStorageFactsEntryFromRow(entryRow),
		trimmedEntryHashes: trimHashRows ?? [],
		documentTrimmedHeadsProcessed,
		documentPreviousContext: documentContextFactsFromRow(contextRow),
	};
};

const compactPreparedCommitFactsWithTrimHashesFromRow = (
	row: unknown[],
): NativeBackboneCommittedEntry & {
	trimmedEntryHashes?: string[];
	documentTrimmedHeadsProcessed?: boolean;
} => {
	const [hash, byteLength, metaBytes, fourth] = row as [
		string,
		number,
		Uint8Array | undefined,
		Uint8Array | string[] | undefined,
	];
	const hasDigestRow = fourth instanceof Uint8Array;
	const hashDigestBytes = hasDigestRow ? fourth : undefined;
	const trimHashOffset = hasDigestRow ? 4 : 3;
	const trimHashRows = row[trimHashOffset] as string[] | undefined;
	const documentTrimmedHeadsProcessed = row[trimHashOffset + 1] as
		| boolean
		| undefined;
	return {
		cid: hash,
		hash,
		next: [],
		metaBytes,
		byteLength,
		hashDigestBytes,
		trimmedEntryHashes: trimHashRows ?? [],
		documentTrimmedHeadsProcessed,
	};
};

const nativeLogCommitEntryColumns = (
	entries: NativeBackboneLogCommitEntry[],
) => {
	const hashes = new Array<string>(entries.length);
	const blockBytes = new Array<Uint8Array>(entries.length);
	const gids = new Array<string>(entries.length);
	const nexts = new Array<string[]>(entries.length);
	const entryTypes = new Uint8Array(entries.length);
	const wallTimes = new BigUint64Array(entries.length);
	const logicals = new Uint32Array(entries.length);
	const payloadSizes = new Uint32Array(entries.length);
	const heads = new Uint8Array(entries.length);
	const datas = new Array<Uint8Array | undefined>(entries.length);
	for (let i = 0; i < entries.length; i++) {
		const entry = entries[i]!;
		hashes[i] = entry.hash;
		blockBytes[i] = entry.bytes;
		gids[i] = entry.gid;
		nexts[i] = entry.next;
		entryTypes[i] = entry.type;
		wallTimes[i] = BigInt(entry.clock.timestamp.wallTime);
		logicals[i] = entry.clock.timestamp.logical ?? 0;
		payloadSizes[i] = entry.payloadSize ?? 0;
		heads[i] = (entry.head ?? true) ? 1 : 0;
		datas[i] = entry.data;
	}
	return {
		hashes,
		blockBytes,
		gids,
		nexts,
		entryTypes,
		wallTimes,
		logicals,
		payloadSizes,
		heads,
		datas,
	};
};

const validateNativeBackboneCoordinateCommitColumns = (
	columns: NativeBackboneCoordinateCommitColumns,
): void => {
	const length = columns.hashes.length;
	if (
		columns.gids.length !== length ||
		columns.nextHashBatches.length !== length ||
		columns.assignedToRangeBoundaries.length !== length
	) {
		throw new Error("Expected equal native coordinate commit column lengths");
	}
	if (columns.hashNumberValues || columns.coordinateCounts || columns.coordinateValues) {
		if (
			!columns.hashNumberValues ||
			!columns.coordinateCounts ||
			!columns.coordinateValues ||
			columns.hashNumberValues.length !== length ||
			columns.coordinateCounts.length !== length
		) {
			throw new Error("Expected equal native coordinate numeric column lengths");
		}
		let coordinateCount = 0;
		for (const count of columns.coordinateCounts) {
			coordinateCount += count;
		}
		if (coordinateCount !== columns.coordinateValues.length) {
			throw new Error("Expected equal native coordinate value lengths");
		}
	}
	if (
		columns.requestedReplicaValues &&
		columns.requestedReplicaValues.length !== length
	) {
		throw new Error("Expected equal native coordinate replica column lengths");
	}
	if (columns.hashNumbers || columns.coordinateBatches || columns.requestedReplicas) {
		if (
			!columns.hashNumbers ||
			!columns.coordinateBatches ||
			!columns.requestedReplicas ||
			columns.hashNumbers.length !== length ||
			columns.coordinateBatches.length !== length ||
			columns.requestedReplicas.length !== length
		) {
			throw new Error("Expected equal native coordinate string column lengths");
		}
	}
};

const emptyNativeBackboneCoordinateCommitColumns =
	(): NativeBackboneCoordinateCommitColumns => ({
		hashes: [],
		gids: [],
		nextHashBatches: [],
		assignedToRangeBoundaries: new Uint8Array(0),
		hashNumberValues: new BigUint64Array(0),
		coordinateCounts: new Uint32Array(0),
		coordinateValues: new BigUint64Array(0),
		requestedReplicaValues: new Uint32Array(0),
	});

const hasNativeBackboneNumericCoordinateCommitColumns = (
	columns: NativeBackboneCoordinateCommitColumns,
): columns is NativeBackboneCoordinateCommitColumns &
	Required<
		Pick<
			NativeBackboneCoordinateCommitColumns,
			| "hashNumberValues"
			| "coordinateCounts"
			| "coordinateValues"
			| "requestedReplicaValues"
		>
	> =>
	!!columns.hashNumberValues &&
	!!columns.coordinateCounts &&
	!!columns.coordinateValues &&
	!!columns.requestedReplicaValues;

const nativeBackboneCoordinateCommitStringColumns = (
	columns: NativeBackboneCoordinateCommitColumns,
): Required<
	Pick<
		NativeBackboneCoordinateCommitColumns,
		"hashNumbers" | "coordinateBatches" | "requestedReplicas"
	>
> => {
	if (columns.hashNumbers && columns.coordinateBatches && columns.requestedReplicas) {
		return {
			hashNumbers: columns.hashNumbers,
			coordinateBatches: columns.coordinateBatches,
			requestedReplicas: columns.requestedReplicas,
		};
	}
	if (!hasNativeBackboneNumericCoordinateCommitColumns(columns)) {
		throw new Error("Missing native coordinate commit columns");
	}
	const hashNumbers = Array.from(columns.hashNumberValues, (value) =>
		value.toString(),
	);
	const coordinateBatches = new Array<string[]>(columns.hashes.length);
	let coordinateOffset = 0;
	for (let i = 0; i < columns.hashes.length; i++) {
		const count = columns.coordinateCounts[i]!;
		const coordinates = new Array<string>(count);
		for (let j = 0; j < count; j++) {
			coordinates[j] = columns.coordinateValues[coordinateOffset++]!.toString();
		}
		coordinateBatches[i] = coordinates;
	}
	return {
		hashNumbers,
		coordinateBatches,
		requestedReplicas: Array.from(columns.requestedReplicaValues),
	};
};

const rawReceivePreparedFactsFromRow = ([
	cid,
	hashDigestBytes,
	byteLength,
	clockId,
	wallTime,
	logical,
	gid,
	next,
	type,
	metaBytes,
	metaData,
	payloadByteLength,
	signatureVerified,
	requestedReplicas,
	hashNumber,
]: NativeBackboneRawReceivePreparedFactsRow): NativeBackboneRawReceivePreparedFacts => ({
	cid,
	hashDigestBytes,
	byteLength,
	clockId,
	wallTime: BigInt(wallTime),
	logical,
	gid,
	next,
	type,
	metaBytes,
	metaData,
	payloadByteLength,
	signatureVerified,
	requestedReplicas,
	hashNumber,
});

const rawReceivePreparedFactsFromColumns = ([
	cids,
	hashDigestBytes,
	byteLengths,
	clockIds,
	wallTimes,
	logicals,
	gids,
	nexts,
	types,
	metaBytes,
	metaDatas,
	payloadByteLengths,
	signatureVerified,
	requestedReplicas,
	hashNumbers,
]: NativeBackboneRawReceivePreparedFactsColumns): NativeBackboneRawReceivePreparedFacts[] => {
	const length = cids.length;
	if (
		hashDigestBytes.length !== length ||
		byteLengths.length !== length ||
		clockIds.length !== length ||
		wallTimes.length !== length ||
		logicals.length !== length ||
		gids.length !== length ||
		nexts.length !== length ||
		types.length !== length ||
		metaBytes.length !== length ||
		metaDatas.length !== length ||
		payloadByteLengths.length !== length ||
		signatureVerified.length !== length ||
		requestedReplicas.length !== length ||
		hashNumbers.length !== length
	) {
		throw new Error("Expected equal raw receive prepared column lengths");
	}
	const out = new Array<NativeBackboneRawReceivePreparedFacts>(length);
	for (let i = 0; i < length; i++) {
		out[i] = {
			cid: cids[i]!,
			hashDigestBytes:
				hashDigestBytes[i] ?? cidifyString(cids[i]!).multihash.digest,
			byteLength: byteLengths[i]!,
			clockId: clockIds[i]!,
			wallTime: wallTimes[i]!,
			logical: logicals[i]!,
			gid: gids[i]!,
			next: nexts[i]!,
			type: types[i]!,
			metaBytes: metaBytes[i]!,
			metaData: metaDatas[i],
			payloadByteLength: payloadByteLengths[i]!,
			signatureVerified: signatureVerified[i] !== 0,
			requestedReplicas:
				requestedReplicas[i] && requestedReplicas[i] > 0
					? requestedReplicas[i]
					: undefined,
			hashNumber:
				hashNumbers[i] == null ? undefined : String(hashNumbers[i]),
		};
	}
	return out;
};

const rawReceiveGroupPlanFromRow = ([
	gid,
	hashes,
	requestedReplicas,
	latestHash,
	maxReplicasFromHead,
	maxReplicasFromNewEntries,
	maxMaxReplicas,
]: NativeBackboneRawReceiveGroupPlanRow): NativeBackboneRawReceiveGroupPlan => ({
	gid,
	hashes,
	requestedReplicas: Array.from(requestedReplicas),
	latestHash,
	maxReplicasFromHead,
	maxReplicasFromNewEntries,
	maxMaxReplicas,
});

const rawReceiveGroupIndexPlanFromRow = ([
	gid,
	indexes,
	requestedReplicas,
	latestIndex,
	maxReplicasFromHead,
	maxReplicasFromNewEntries,
	maxMaxReplicas,
]: NativeBackboneRawReceiveGroupIndexPlanRow): NativeBackboneRawReceiveGroupIndexPlan => ({
	gid,
	indexes,
	requestedReplicas: Array.from(requestedReplicas),
	latestIndex,
	maxReplicasFromHead,
	maxReplicasFromNewEntries,
	maxMaxReplicas,
});

const rawReceiveGroupLeaderPlanFromRow = (
	resolution: "u32" | "u64",
	[
		gid,
		indexes,
		requestedReplicas,
		latestIndex,
		maxReplicasFromHead,
		maxReplicasFromNewEntries,
		maxMaxReplicas,
		coordinateRows,
		leaderRows,
	]: NativeBackboneRawReceiveGroupLeaderPlanRow,
): NativeBackboneRawReceiveGroupLeaderPlan => {
	const coordinateStrings = coordinateRows.map((coordinate) =>
		String(coordinate),
	);
	return {
		gid,
		indexes,
		requestedReplicas: Array.from(requestedReplicas),
		latestIndex,
		maxReplicasFromHead,
		maxReplicasFromNewEntries,
		maxMaxReplicas,
		coordinates: rowsToNumbers(resolution, coordinateStrings),
		coordinateStrings,
		leaders: rowsToSamples(leaderRows) ?? new Map(),
	};
};

const rawReceiveGroupAssignmentPlanFromRow = (
	resolution: "u32" | "u64",
	[
		gid,
		indexes,
		requestedReplicas,
		latestIndex,
		maxReplicasFromHead,
		maxReplicasFromNewEntries,
		maxMaxReplicas,
		coordinateRows,
		isLeader,
		fromIsLeader,
		assignedToRangeBoundary,
	]: NativeBackboneRawReceiveGroupAssignmentPlanRow,
): NativeBackboneRawReceiveGroupAssignmentPlan => {
	const coordinateStrings = coordinateRows.map((coordinate) =>
		String(coordinate),
	);
	return {
		gid,
		indexes,
		requestedReplicas: Array.from(requestedReplicas),
		latestIndex,
		maxReplicasFromHead,
		maxReplicasFromNewEntries,
		maxMaxReplicas,
		coordinates: rowsToNumbers(resolution, coordinateStrings),
		coordinateStrings,
		isLeader,
		fromIsLeader,
		assignedToRangeBoundary,
	};
};

const rawReceiveSelectionFromRow = (
	resolution: "u32" | "u64",
	[
		retainedHashes,
		droppedHashes,
		groupCount,
		plannedHashCount,
		usedNativeFastDropPlan,
		usedLeaderSamplePlans,
		retainedGroupLeaderPlanRows,
		retainedIndexes,
		droppedIndexes,
	]: NativeBackboneRawReceiveSelectionRow,
): NativeBackboneRawReceiveSelectionPlan => {
	const plan: NativeBackboneRawReceiveSelectionPlan = {
		retainedHashes,
		droppedHashes,
		groupCount,
		plannedHashCount,
		usedNativeFastDropPlan,
		usedLeaderSamplePlans,
	};
	if (retainedIndexes) {
		plan.retainedIndexes = retainedIndexes;
	}
	if (droppedIndexes) {
		plan.droppedIndexes = droppedIndexes;
	}
	if (retainedGroupLeaderPlanRows) {
		plan.retainedGroupLeaderPlans = retainedGroupLeaderPlanRows.map((row) =>
			rawReceiveGroupLeaderPlanFromRow(resolution, row),
		);
	}
	return plan;
};

export class NativeBackboneLogGraph {
	constructor(
		private readonly native: NativePeerbitBackboneHandle,
		private readonly options?: NativeBackboneLogGraphOptions,
	) {}

	get length(): number {
		return this.native.log_len();
	}

	has(hash: string): boolean {
		return this.native.has_log_entry(hash);
	}

	hasMany(hashes: Iterable<string>): Set<string> {
		return new Set(this.native.graph_has_many([...hashes]));
	}

	put(entry: NativeBackboneLogEntry): void {
		this.native.graph_put(
			entry.hash,
			entry.gid,
			entry.next,
			entry.type,
			BigInt(entry.clock.timestamp.wallTime),
			entry.clock.timestamp.logical ?? 0,
			entry.payloadSize ?? 0,
			entry.head ?? true,
			entry.data,
		);
	}

	putBatch(entries: NativeBackboneLogEntry[]): void {
		if (entries.length === 0) {
			return;
		}
		const hashes = new Array<string>(entries.length);
		const gids = new Array<string>(entries.length);
		const nexts = new Array<string[]>(entries.length);
		const entryTypes = new Uint8Array(entries.length);
		const wallTimes = new BigUint64Array(entries.length);
		const logicals = new Uint32Array(entries.length);
		const payloadSizes = new Uint32Array(entries.length);
		const heads = new Uint8Array(entries.length);
		const datas = new Array<Uint8Array | undefined>(entries.length);
		for (let i = 0; i < entries.length; i++) {
			const entry = entries[i]!;
			hashes[i] = entry.hash;
			gids[i] = entry.gid;
			nexts[i] = entry.next;
			entryTypes[i] = entry.type;
			wallTimes[i] = BigInt(entry.clock.timestamp.wallTime);
			logicals[i] = entry.clock.timestamp.logical ?? 0;
			payloadSizes[i] = entry.payloadSize ?? 0;
			heads[i] = (entry.head ?? true) ? 1 : 0;
			datas[i] = entry.data;
		}
		this.native.graph_put_batch(
			hashes,
			gids,
			nexts,
			entryTypes,
			wallTimes,
			logicals,
			payloadSizes,
			heads,
			datas,
		);
	}

	putAppendChain(entries: NativeBackboneLogEntry[]): void {
		if (entries.length === 0) {
			return;
		}
		const first = entries[0]!;
		const hashes = new Array<string>(entries.length);
		const wallTimes = new BigUint64Array(entries.length);
		const logicals = new Uint32Array(entries.length);
		const payloadSizes = new Uint32Array(entries.length);
		const datas = new Array<Uint8Array | undefined>(entries.length);
		for (let i = 0; i < entries.length; i++) {
			const entry = entries[i]!;
			hashes[i] = entry.hash;
			wallTimes[i] = BigInt(entry.clock.timestamp.wallTime);
			logicals[i] = entry.clock.timestamp.logical ?? 0;
			payloadSizes[i] = entry.payloadSize ?? 0;
			datas[i] = entry.data;
		}
		this.native.graph_put_append_chain(
			hashes,
			first.gid,
			first.next,
			first.type,
			wallTimes,
			logicals,
			payloadSizes,
			datas,
		);
	}

	commitBlocksAndGraphBatch(entries: NativeBackboneLogCommitEntry[]): void {
		if (entries.length === 0) {
			return;
		}
		const columns = nativeLogCommitEntryColumns(entries);
		this.native.commit_log_blocks_and_graph_batch(
			columns.hashes,
			columns.blockBytes,
			columns.gids,
			columns.nexts,
			columns.entryTypes,
			columns.wallTimes,
			columns.logicals,
			columns.payloadSizes,
			columns.heads,
			columns.datas,
		);
	}

	commitBlocksGraphAndCoordinatesBatch(
		entries: NativeBackboneLogCommitEntry[],
		coordinates: NativeBackboneCoordinateCommitColumns,
	): void {
		if (entries.length === 0) {
			return;
		}
		validateNativeBackboneCoordinateCommitColumns(coordinates);
		const coordinateStringColumns =
			nativeBackboneCoordinateCommitStringColumns(coordinates);
		const columns = nativeLogCommitEntryColumns(entries);
		this.native.commit_log_blocks_graph_and_coordinates_batch(
			columns.hashes,
			columns.blockBytes,
			columns.gids,
			columns.nexts,
			columns.entryTypes,
			columns.wallTimes,
			columns.logicals,
			columns.payloadSizes,
			columns.heads,
			columns.datas,
			coordinates.hashes,
			coordinates.gids,
			coordinateStringColumns.hashNumbers,
			coordinateStringColumns.coordinateBatches,
			coordinates.nextHashBatches,
			coordinates.assignedToRangeBoundaries,
			coordinateStringColumns.requestedReplicas,
		);
	}

	commitPreparedRawReceiveBatch(
		hashes: string[],
		headFlags: NativeBackboneHeadFlags,
		coordinates?: NativeBackboneCoordinateCommitColumns,
	): boolean {
		if (hashes.length === 0) {
			return true;
		}
		if (hashes.length !== headFlags.length) {
			throw new Error("Expected equal raw receive hash and head lengths");
		}
		const coordinateColumns =
			coordinates ?? emptyNativeBackboneCoordinateCommitColumns();
		validateNativeBackboneCoordinateCommitColumns(coordinateColumns);
		if (
			this.native.commit_prepared_raw_receive_batch_u64 &&
			hasNativeBackboneNumericCoordinateCommitColumns(coordinateColumns)
		) {
			return this.native.commit_prepared_raw_receive_batch_u64(
				hashes,
				nativeBackboneHeadFlagsToBytes(headFlags),
				coordinateColumns.hashes,
				coordinateColumns.gids,
				coordinateColumns.hashNumberValues,
				coordinateColumns.coordinateCounts,
				coordinateColumns.coordinateValues,
				coordinateColumns.nextHashBatches,
				coordinateColumns.assignedToRangeBoundaries,
				coordinateColumns.requestedReplicaValues,
			);
		}
		const coordinateStringColumns =
			nativeBackboneCoordinateCommitStringColumns(coordinateColumns);
		return this.native.commit_prepared_raw_receive_batch(
			hashes,
			nativeBackboneHeadFlagsToBytes(headFlags),
			coordinateColumns.hashes,
			coordinateColumns.gids,
			coordinateStringColumns.hashNumbers,
			coordinateStringColumns.coordinateBatches,
			coordinateColumns.nextHashBatches,
			coordinateColumns.assignedToRangeBoundaries,
			coordinateStringColumns.requestedReplicas,
		);
	}

	commitPreparedRawReceiveJoinBatch(
		hashes: string[],
		headFlags: NativeBackboneHeadFlags,
		coordinates?: NativeBackboneCoordinateCommitColumns,
	): boolean | undefined {
		if (hashes.length === 0) {
			return true;
		}
		if (hashes.length !== headFlags.length) {
			throw new Error("Expected equal raw receive hash and head lengths");
		}
		if (!this.native.commit_prepared_raw_receive_join_batch) {
			return undefined;
		}
		const coordinateColumns =
			coordinates ?? emptyNativeBackboneCoordinateCommitColumns();
		validateNativeBackboneCoordinateCommitColumns(coordinateColumns);
		if (
			this.native.commit_prepared_raw_receive_join_batch_u64 &&
			hasNativeBackboneNumericCoordinateCommitColumns(coordinateColumns)
		) {
			return this.native.commit_prepared_raw_receive_join_batch_u64(
				hashes,
				nativeBackboneHeadFlagsToBytes(headFlags),
				coordinateColumns.hashes,
				coordinateColumns.gids,
				coordinateColumns.hashNumberValues,
				coordinateColumns.coordinateCounts,
				coordinateColumns.coordinateValues,
				coordinateColumns.nextHashBatches,
				coordinateColumns.assignedToRangeBoundaries,
				coordinateColumns.requestedReplicaValues,
			);
		}
		const coordinateStringColumns =
			nativeBackboneCoordinateCommitStringColumns(coordinateColumns);
		return this.native.commit_prepared_raw_receive_join_batch(
			hashes,
			nativeBackboneHeadFlagsToBytes(headFlags),
			coordinateColumns.hashes,
			coordinateColumns.gids,
			coordinateStringColumns.hashNumbers,
			coordinateStringColumns.coordinateBatches,
			coordinateColumns.nextHashBatches,
			coordinateColumns.assignedToRangeBoundaries,
			coordinateStringColumns.requestedReplicas,
		);
	}

	commitVerifiedPreparedRawReceiveJoinBatch(
		hashes: string[],
		headFlags: NativeBackboneHeadFlags,
		verifyHashes: string[],
		coordinates?: NativeBackboneCoordinateCommitColumns,
	): boolean | undefined {
		if (hashes.length === 0) {
			return true;
		}
		if (hashes.length !== headFlags.length) {
			throw new Error("Expected equal raw receive hash and head lengths");
		}
		if (!this.native.commit_verified_prepared_raw_receive_join_batch) {
			return undefined;
		}
		const coordinateColumns =
			coordinates ?? emptyNativeBackboneCoordinateCommitColumns();
		validateNativeBackboneCoordinateCommitColumns(coordinateColumns);
		if (
			this.native.commit_verified_prepared_raw_receive_join_batch_u64 &&
			hasNativeBackboneNumericCoordinateCommitColumns(coordinateColumns)
		) {
			return this.native.commit_verified_prepared_raw_receive_join_batch_u64(
				hashes,
				nativeBackboneHeadFlagsToBytes(headFlags),
				verifyHashes,
				coordinateColumns.hashes,
				coordinateColumns.gids,
				coordinateColumns.hashNumberValues,
				coordinateColumns.coordinateCounts,
				coordinateColumns.coordinateValues,
				coordinateColumns.nextHashBatches,
				coordinateColumns.assignedToRangeBoundaries,
				coordinateColumns.requestedReplicaValues,
			);
		}
		const coordinateStringColumns =
			nativeBackboneCoordinateCommitStringColumns(coordinateColumns);
		return this.native.commit_verified_prepared_raw_receive_join_batch(
			hashes,
			nativeBackboneHeadFlagsToBytes(headFlags),
			verifyHashes,
			coordinateColumns.hashes,
			coordinateColumns.gids,
			coordinateStringColumns.hashNumbers,
			coordinateStringColumns.coordinateBatches,
			coordinateColumns.nextHashBatches,
			coordinateColumns.assignedToRangeBoundaries,
			coordinateStringColumns.requestedReplicas,
		);
	}

	commitVerifiedAllPreparedRawReceiveJoinBatch(
		hashes: string[],
		headFlags: NativeBackboneHeadFlags,
		coordinates?: NativeBackboneCoordinateCommitColumns,
	): boolean | undefined {
		if (hashes.length === 0) {
			return true;
		}
		if (hashes.length !== headFlags.length) {
			throw new Error("Expected equal raw receive hash and head lengths");
		}
		if (!this.native.commit_verified_all_prepared_raw_receive_join_batch) {
			return undefined;
		}
		const coordinateColumns =
			coordinates ?? emptyNativeBackboneCoordinateCommitColumns();
		validateNativeBackboneCoordinateCommitColumns(coordinateColumns);
		if (
			this.native.commit_verified_all_prepared_raw_receive_join_batch_u64 &&
			hasNativeBackboneNumericCoordinateCommitColumns(coordinateColumns)
		) {
			return this.native.commit_verified_all_prepared_raw_receive_join_batch_u64(
				hashes,
				nativeBackboneHeadFlagsToBytes(headFlags),
				coordinateColumns.hashes,
				coordinateColumns.gids,
				coordinateColumns.hashNumberValues,
				coordinateColumns.coordinateCounts,
				coordinateColumns.coordinateValues,
				coordinateColumns.nextHashBatches,
				coordinateColumns.assignedToRangeBoundaries,
				coordinateColumns.requestedReplicaValues,
			);
		}
		const coordinateStringColumns =
			nativeBackboneCoordinateCommitStringColumns(coordinateColumns);
		return this.native.commit_verified_all_prepared_raw_receive_join_batch(
			hashes,
			nativeBackboneHeadFlagsToBytes(headFlags),
			coordinateColumns.hashes,
			coordinateColumns.gids,
			coordinateStringColumns.hashNumbers,
			coordinateStringColumns.coordinateBatches,
			coordinateColumns.nextHashBatches,
			coordinateColumns.assignedToRangeBoundaries,
			coordinateStringColumns.requestedReplicas,
		);
	}

	clearPreparedRawReceiveEntries(hashes: Iterable<string>): number {
		return this.native.clear_prepared_raw_receive_entries(
			iterableToArray(hashes),
		);
	}

	verifyPreparedRawReceiveEntries(
		hashes: Iterable<string>,
	): boolean[] | undefined {
		const normalized = iterableToArray(hashes);
		if (normalized.length === 0) {
			return [];
		}
		const verified =
			this.native.verify_prepared_raw_receive_entries?.(normalized);
		return verified ? Array.from(verified, (value) => value !== 0) : undefined;
	}

	prepareEntryV0PlainEntryCommit(
		input: {
			clockId: Uint8Array;
			privateKey: Uint8Array;
			publicKey: Uint8Array;
			wallTime: bigint | number | string;
			logical?: number;
			gid: string;
			next?: string[];
			type?: number;
			metaData?: Uint8Array;
			payloadData: Uint8Array;
			resolveTrimmedEntries?: boolean;
			includeMaterializationBytes?: boolean;
			includeAppendFactsBytes?: boolean;
			trimLengthTo?: number;
			documentIndex?: {
				key: string;
				valuePrefixBytes?: Uint8Array;
				projection?: {
					encodedDocument: Uint8Array;
					plan: NativeBackboneSimpleDocumentProjectionPlan;
					signer?: Uint8Array;
				};
				existingCreated?: bigint | number | string;
				byteElementIndexLimit?: number;
				deleteTrimmedHeads?: boolean;
				useLatestContext?: boolean;
			};
		},
		_blockStore: unknown,
	):
		| (NativeBackboneCommittedEntry & {
				trimmedEntries?: NativeBackboneLogEntry[];
				trimmedEntryHashes?: string[];
				documentTrimmedHeadsProcessed?: boolean;
		  })
		| undefined {
		if (this.options?.commitBlocks === false) {
			return undefined;
		}
		if (
			input.includeMaterializationBytes !== false ||
			input.includeAppendFactsBytes !== true
		) {
			return undefined;
		}
		const wallTime = BigInt(input.wallTime);
		const logical = input.logical ?? 0;
		const entryType = input.type ?? 0;
		const hasNoNext = input.next == null || input.next.length === 0;
		const documentIndex = input.documentIndex;
		const documentIndexArgs = nativeDocumentIndexArgs(documentIndex);
		const projection = documentIndex?.projection;
		if (
			documentIndex?.useLatestContext &&
			documentIndexArgs &&
			input.resolveTrimmedEntries === false
		) {
			if (projection && this.options?.documentProjectionPlanId) {
				return preparedCommitFactsWithLatestDocumentContextFromRow(
					this.native.prepare_plain_entry_commit_latest_facts_document_index_cached_plan_trim_hashes(
						wallTime,
						logical,
						input.gid,
						entryType,
						input.metaData,
						input.payloadData,
						input.trimLengthTo,
						documentIndex.key,
						documentIndex.byteElementIndexLimit ?? 0,
						documentIndex.deleteTrimmedHeads === true,
						this.options.documentProjectionPlanId(projection.plan),
						projection.encodedDocument,
						projection.signer,
					),
				);
			}
			return preparedCommitFactsWithLatestDocumentContextFromRow(
				this.native.prepare_plain_entry_commit_latest_facts_document_index_trim_hashes(
					wallTime,
					logical,
					input.gid,
					entryType,
					input.metaData,
					input.payloadData,
					input.trimLengthTo,
					documentIndex.key,
					documentIndex.valuePrefixBytes ?? EMPTY_UINT8_ARRAY,
					documentIndex.byteElementIndexLimit ?? 0,
					documentIndex.deleteTrimmedHeads === true,
					projection?.plan,
					projection?.encodedDocument,
					projection?.signer,
				),
			);
		}
		if (
			documentIndexArgs &&
			projection &&
			this.options?.documentProjectionPlanId &&
			input.resolveTrimmedEntries === false &&
			input.trimLengthTo != null &&
			hasNoNext
		) {
			const projectionPlanId = this.options.documentProjectionPlanId(
				projection.plan,
			);
			const plainPutPayloadCommit =
				this.native
					.prepare_plain_entry_commit_no_next_facts_document_index_cached_plan_compact_trim_hashes_plain_put_payload;
			if (plainPutPayloadCommit) {
				return compactPreparedCommitFactsWithTrimHashesFromRow(
					plainPutPayloadCommit.call(
						this.native,
						wallTime,
						logical,
						input.gid,
						entryType,
						input.metaData,
						input.payloadData,
						input.trimLengthTo,
						documentIndex.key,
						documentIndex.existingCreated == null
							? ""
							: integerString(documentIndex.existingCreated),
						documentIndex.byteElementIndexLimit ?? 0,
						documentIndex.deleteTrimmedHeads === true,
						projectionPlanId,
						projection.signer,
					),
				);
			}
			return compactPreparedCommitFactsWithTrimHashesFromRow(
				this.native.prepare_plain_entry_commit_no_next_facts_document_index_cached_plan_compact_trim_hashes(
					wallTime,
					logical,
					input.gid,
					entryType,
					input.metaData,
					input.payloadData,
					input.trimLengthTo,
					documentIndex.key,
					documentIndex.existingCreated == null
						? ""
						: integerString(documentIndex.existingCreated),
					documentIndex.byteElementIndexLimit ?? 0,
					documentIndex.deleteTrimmedHeads === true,
					projectionPlanId,
					projection.encodedDocument,
					projection.signer,
				),
			);
		}
		if (
			documentIndexArgs &&
			projection &&
			this.options?.documentProjectionPlanId &&
			input.trimLengthTo == null &&
			hasNoNext
		) {
			const projectionPlanId = this.options.documentProjectionPlanId(
				projection.plan,
			);
			const plainPutPayloadCommit =
				this.native
					.prepare_plain_entry_commit_no_next_facts_document_index_cached_plan_compact_plain_put_payload;
			if (plainPutPayloadCommit) {
				return preparedCommitFactsFromRow(
					plainPutPayloadCommit.call(
						this.native,
						wallTime,
						logical,
						input.gid,
						entryType,
						input.metaData,
						input.payloadData,
						documentIndex.key,
						documentIndex.existingCreated == null
							? ""
							: integerString(documentIndex.existingCreated),
						documentIndex.byteElementIndexLimit ?? 0,
						documentIndex.deleteTrimmedHeads === true,
						projectionPlanId,
						projection.signer,
					),
				);
			}
			return preparedCommitFactsFromRow(
				this.native.prepare_plain_entry_commit_no_next_facts_document_index_cached_plan_compact(
					wallTime,
					logical,
					input.gid,
					entryType,
					input.metaData,
					input.payloadData,
					documentIndex.key,
					documentIndex.existingCreated == null
						? ""
						: integerString(documentIndex.existingCreated),
					documentIndex.byteElementIndexLimit ?? 0,
					documentIndex.deleteTrimmedHeads === true,
					projectionPlanId,
					projection.encodedDocument,
					projection.signer,
				),
			);
		}
		if (
			documentIndexArgs &&
			projection &&
			this.options?.documentProjectionPlanId
		) {
			const baseArgs = [
				wallTime,
				logical,
				input.gid,
				input.next ?? [],
				entryType,
				input.metaData,
				input.payloadData,
				input.trimLengthTo,
			] as const;
			return preparedCommitFactsFromRow(
				this.native.prepare_plain_entry_commit_facts_document_index_cached_plan(
					...baseArgs,
					documentIndex.key,
					documentIndex.existingCreated == null
						? ""
						: integerString(documentIndex.existingCreated),
					documentIndex.byteElementIndexLimit ?? 0,
					documentIndex.deleteTrimmedHeads === true,
					this.options.documentProjectionPlanId(projection.plan),
					projection.encodedDocument,
					projection.signer,
				),
			);
		}
		if (
			documentIndexArgs &&
			input.trimLengthTo == null &&
			hasNoNext
		) {
			return preparedCommitFactsFromRow(
				this.native.prepare_plain_entry_commit_no_next_facts_document_index_compact(
					wallTime,
					logical,
					input.gid,
					entryType,
					input.metaData,
					input.payloadData,
					...documentIndexArgs,
				),
			);
		}
		if (
			documentIndexArgs &&
			input.resolveTrimmedEntries === false &&
			input.trimLengthTo != null &&
			hasNoNext
		) {
			return compactPreparedCommitFactsWithTrimHashesFromRow(
				this.native.prepare_plain_entry_commit_no_next_facts_document_index_compact_trim_hashes(
					wallTime,
					logical,
					input.gid,
					entryType,
					input.metaData,
					input.payloadData,
					input.trimLengthTo,
					...documentIndexArgs,
				),
			);
		}
		const baseArgs = [
			wallTime,
			logical,
			input.gid,
			input.next ?? [],
			entryType,
			input.metaData,
			input.payloadData,
			input.trimLengthTo,
		] as const;
		return preparedCommitFactsFromRow(
			documentIndexArgs
				? this.native.prepare_plain_entry_commit_facts_document_index(
						...baseArgs,
						...documentIndexArgs,
					)
				: this.native.prepare_plain_entry_commit_facts(...baseArgs),
		);
	}

	prepareEntryV0PlainEntryAndPut(input: {
		clockId: Uint8Array;
		privateKey: Uint8Array;
		publicKey: Uint8Array;
		wallTime: bigint | number | string;
		logical?: number;
		gid: string;
		next?: string[];
		type?: number;
		metaData?: Uint8Array;
		payloadData: Uint8Array;
		includeMaterializationBytes?: boolean;
		includeAppendFactsBytes?: boolean;
		trimLengthTo?: number;
	}): NativeBackboneStorageBackedEntry & {
		trimmedEntries?: NativeBackboneLogEntry[];
	} {
		const row =
			input.trimLengthTo == null
				? this.native.prepare_plain_entry_storage_facts_and_put(
						BigInt(input.wallTime),
						input.logical ?? 0,
						input.gid,
						input.next ?? [],
						input.type ?? 0,
						input.metaData,
						input.payloadData,
					)
				: this.native.prepare_plain_entry_storage_facts_trim_and_put(
						BigInt(input.wallTime),
						input.logical ?? 0,
						input.gid,
						input.next ?? [],
						input.type ?? 0,
						input.metaData,
						input.payloadData,
						input.trimLengthTo,
					);
		const isTrimRow =
			Array.isArray(row) &&
			row.length === 2 &&
			Array.isArray(row[0]) &&
			Array.isArray(row[1]);
		const entry = storageFactsEntryFromRow(
			(isTrimRow ? row[0] : row) as unknown[],
		);
		if (!isTrimRow) {
			return entry;
		}
		return {
			...entry,
			trimmedEntries: (row[1] as unknown[]).map(nativeLogEntryFromTrimRow),
		};
	}

	delete(hash: string): boolean {
		return this.native.graph_delete(hash);
	}

	deleteMany(hashes: Iterable<string>): number {
		return this.native.graph_delete_many([...hashes]);
	}

	oldestEntries(limit: number): NativeBackboneLogEntry[] {
		return this.native
			.graph_oldest_entries(limit)
			.map(nativeLogEntryFromTrimRow);
	}

	clear(): void {
		this.native.graph_clear();
	}

	heads(gid?: string): string[] {
		return this.native.graph_heads(gid);
	}

	hasHead(gid?: string): boolean {
		return this.native.graph_has_head(gid);
	}

	hasAnyHead(gids: Iterable<string>): boolean {
		return this.native.graph_has_any_head([...gids]);
	}

	hasAnyHeadBatch(gidSets: Iterable<Iterable<string>>): boolean[] {
		return this.native.graph_has_any_head_batch(
			[...gidSets].map((gids) => [...gids]),
		);
	}

	headDataEntries(gid?: string): any[] {
		return this.native.graph_head_data_entries(gid).map(headDataEntryFromRow);
	}

	maxHeadDataU32(gid?: string): number | undefined {
		return this.native.graph_max_head_data_u32(gid);
	}

	maxHeadDataU32Batch(gids: Iterable<string>): Array<number | undefined> {
		return this.native.graph_max_head_data_u32_batch([...gids]);
	}

	headEntries(gid?: string): any[] {
		return this.native.graph_head_entries(gid).map(headEntryFromRow);
	}

	joinHeadEntries(gid?: string): any[] {
		return this.native.graph_join_head_entries(gid).map(joinHeadEntryFromRow);
	}

	childJoinEntries(hash: string): any[] {
		return this.native.graph_child_join_entries(hash).map(joinHeadEntryFromRow);
	}

	entryMetadataBatch(hashes: Iterable<string>): Array<any | undefined> {
		return this.native
			.graph_entry_metadata_batch([...hashes])
			.map(metadataEntryFromRow);
	}

	entryMetadataHintsBatch(hashes: Iterable<string>): Array<any | undefined> {
		return (
			this.native.graph_entry_metadata_hints_batch ??
			this.native.graph_entry_metadata_batch
		)([...hashes]).map((row) => {
			if (row == null || !this.native.graph_entry_metadata_hints_batch) {
				return metadataEntryFromRow(row);
			}
			return requestPruneEntryFromRow(row);
		});
	}

	entrySignaturePublicKeysBatch(
		hashes: Iterable<string>,
	): Array<Uint8Array | undefined> {
		const hashList = [...hashes];
		return (
			this.native.graph_entry_signature_public_key_batch?.(hashList) ??
			hashList.map(() => undefined)
		);
	}

	uniqueReferenceGids(hash: string): string[] | undefined {
		return this.native.graph_unique_reference_gids(hash);
	}

	uniqueReferenceGidRowsBatch(hashes: Iterable<string>): any[] {
		return this.native.graph_unique_reference_gid_rows_batch([...hashes]);
	}

	uniqueReferenceGidRowsFlatBatch(
		hashes: Iterable<string>,
	): Array<[number, string, string]> | undefined {
		return this.native
			.graph_unique_reference_gid_rows_flat_batch?.([...hashes])
			?.map((row) => {
				const [position, hash, gid] = row;
				return [position, hash, gid] as [number, string, string];
			});
	}

	planDeleteRecursively(hashes: Iterable<string>, skipFirst = false): string[] {
		return this.native.graph_plan_delete_recursively([...hashes], skipFirst);
	}

	payloadSizeSum(): number {
		return this.native.graph_payload_size_sum();
	}

	oldestHash(): string | undefined {
		return this.native.graph_oldest_hash();
	}

	newestHash(): string | undefined {
		return this.native.graph_newest_hash();
	}

	countHasNext(next: string, excludeHash?: string): number {
		return this.native.graph_count_has_next(next, excludeHash);
	}

	shadowedGids(gid: string, next: string[], excludeHash?: string): string[] {
		return this.native.graph_shadowed_gids(gid, next, excludeHash);
	}

	planJoin(
		hash: string,
		next: string[],
		type: number,
		reset = false,
		cutCheck?: {
			gid: string;
			wallTime: bigint | number | string;
			logical?: number;
		},
	): any {
		const [skip, missingParents, cutChecked, coveredByCut] =
			this.native.graph_plan_join(
				hash,
				next,
				type,
				reset,
				cutCheck?.gid,
				cutCheck?.wallTime == null ? undefined : BigInt(cutCheck.wallTime),
				cutCheck?.logical,
			);
		return { skip, missingParents, cutChecked, coveredByCut };
	}

	planJoinBatch(
		entries: Array<{
			hash: string;
			next: string[];
			type: number;
			cutCheck?: {
				gid: string;
				wallTime: bigint | number | string;
				logical?: number;
			};
		}>,
		reset = false,
	): any[] {
		const hashes: string[] = [];
		const nexts: string[][] = [];
		const gids: string[] = [];
		const types = new Uint8Array(entries.length);
		const wallTimes = new BigUint64Array(entries.length);
		const logicals = new Uint32Array(entries.length);
		let cutCheck = false;
		for (let i = 0; i < entries.length; i++) {
			const entry = entries[i]!;
			hashes.push(entry.hash);
			nexts.push(entry.next);
			types[i] = entry.type;
			if (entry.cutCheck) {
				cutCheck = true;
				gids[i] = entry.cutCheck.gid;
				wallTimes[i] = BigInt(entry.cutCheck.wallTime);
				logicals[i] = entry.cutCheck.logical ?? 0;
			} else {
				gids[i] = "";
				wallTimes[i] = 0n;
				logicals[i] = 0;
			}
		}
		return this.native
			.graph_plan_join_batch(
				hashes,
				nexts,
				types,
				reset,
				gids,
				wallTimes,
				logicals,
				cutCheck,
			)
			.map(([skip, missingParents, cutChecked, coveredByCut]) => ({
				skip,
				missingParents,
				cutChecked,
				coveredByCut,
			}));
	}
}

export class NativeBackboneBlockStore {
	constructor(private readonly native: NativePeerbitBackboneHandle) {}

	status(): "open" {
		return "open";
	}

	open(): void {}

	close(): void {}

	async start(): Promise<void> {}

	async stop(): Promise<void> {}

	async put(
		data: Uint8Array | { block: { bytes: Uint8Array }; cid: string },
	): Promise<string> {
		const prepared =
			data instanceof Uint8Array ? await calculateRawCid(data) : data;
		this.native.block_put(prepared.cid, prepared.block.bytes);
		return prepared.cid;
	}

	async putMany(
		blocks: Array<Uint8Array | { block: { bytes: Uint8Array }; cid: string }>,
	): Promise<string[]> {
		const prepared = await Promise.all(
			blocks.map((block) =>
				block instanceof Uint8Array ? calculateRawCid(block) : block,
			),
		);
		this.native.block_put_many(
			prepared.map((block) => block.cid),
			prepared.map((block) => block.block.bytes),
		);
		return prepared.map((block) => block.cid);
	}

	putKnown(cid: string, bytes: Uint8Array): string {
		this.native.block_put(cid, bytes);
		return cid;
	}

	putKnownMany(
		blocks: Array<readonly [cid: string, bytes: Uint8Array]>,
	): string[] {
		if (blocks.length === 0) {
			return [];
		}
		const cids = new Array<string>(blocks.length);
		const bytes = new Array<Uint8Array>(blocks.length);
		for (let i = 0; i < blocks.length; i++) {
			const block = blocks[i]!;
			cids[i] = block[0];
			bytes[i] = block[1];
		}
		return this.putKnownManyColumns(cids, bytes);
	}

	putKnownManyColumns(cids: string[], bytes: Uint8Array[]): string[] {
		if (cids.length !== bytes.length) {
			throw new Error("Expected equal block column lengths");
		}
		if (cids.length === 0) {
			return [];
		}
		this.native.block_put_many(cids, bytes);
		return cids;
	}

	putImmutable(cid: string, bytes: Uint8Array): void {
		this.native.block_put(cid, bytes);
	}

	putManyImmutable(
		blocks: Array<readonly [cid: string, bytes: Uint8Array]>,
	): void {
		this.putKnownMany(blocks);
	}

	get(cid: string): Uint8Array | undefined {
		return this.native.block_get(cid);
	}

	async getMany(cids: string[]): Promise<Array<Uint8Array | undefined>> {
		return this.native.block_get_many(cids);
	}

	has(cid: string): boolean {
		return this.native.has_block(cid);
	}

	async hasMany(cids: string[]): Promise<boolean[]> {
		return this.native.block_has_many(cids);
	}

	rm(cid: string): void {
		this.native.block_delete(cid);
	}

	del(cid: string): void {
		this.rm(cid);
	}

	async rmMany(cids: string[]): Promise<number> {
		return this.native.block_delete_many(cids);
	}

	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		for (const [key, value] of this.native.block_entries()) {
			yield [key, value];
		}
	}

	size(): number {
		return this.native.block_size();
	}

	persisted(): boolean {
		return false;
	}

	waitFor(): Promise<string[]> {
		return Promise.resolve([]);
	}
}

const integerString = (value: bigint | number | string): string =>
	typeof value === "string"
		? value
		: typeof value === "number"
			? Math.trunc(value).toString()
			: value.toString();

const iterableToArray = <T>(values?: Iterable<T>): T[] => {
	if (!values) {
		return [];
	}
	return Array.isArray(values) ? values : [...values];
};

const EMPTY_UINT8_ARRAY = new Uint8Array(0);

type NativeBackboneDocumentIndexArgs = readonly [
	string,
	Uint8Array,
	string,
	number,
	boolean,
	NativeBackboneSimpleDocumentProjectionPlan | undefined,
	Uint8Array | undefined,
	Uint8Array | undefined,
];

type NativeBackboneNoNextAppendArgs = readonly [
	bigint,
	number,
	string,
	number,
	Uint8Array | undefined,
	Uint8Array,
	number,
	number,
	string,
	string,
	boolean,
];

type NativeBackboneNoNextStorageAppendArgs = readonly [
	...NativeBackboneNoNextAppendArgs,
	boolean,
];

type NativeBackboneStorageAppendArgs = readonly [
	bigint,
	number,
	string,
	string[],
	number,
	Uint8Array | undefined,
	Uint8Array,
	number,
	number,
	string,
	string,
	boolean,
	boolean,
];

const nativeDocumentIndexArgs = (
	documentIndex?: NativeBackboneAppendInput["documentIndex"],
): NativeBackboneDocumentIndexArgs | undefined =>
	documentIndex
		? [
				documentIndex.key,
				documentIndex.valuePrefixBytes ?? EMPTY_UINT8_ARRAY,
				documentIndex.existingCreated == null
					? ""
					: integerString(documentIndex.existingCreated),
				documentIndex.byteElementIndexLimit ?? 0,
				documentIndex.deleteTrimmedHeads === true,
				documentIndex.projection?.plan,
				documentIndex.projection?.encodedDocument,
				documentIndex.projection?.signer,
			]
		: undefined;

const nativeNoNextAppendArgs = (
	input: NativeBackboneAppendInput,
): NativeBackboneNoNextAppendArgs => [
	BigInt(input.wallTime),
	input.logical ?? 0,
	input.gid,
	input.type ?? 0,
	input.metaData,
	input.payloadData,
	input.replicas,
	input.roleAgeMs ?? 0,
	integerString(input.now ?? Date.now()),
	input.selfHash ?? "",
	input.selfReplicating ?? true,
];

const nativeNoNextStorageAppendArgs = (
	input: NativeBackboneStorageAppendInput,
): NativeBackboneNoNextStorageAppendArgs => [
	...nativeNoNextAppendArgs(input),
	input.resolveTrimmedEntries !== false,
];

const nativeStorageAppendArgs = (
	input: NativeBackboneStorageAppendInput,
): NativeBackboneStorageAppendArgs => [
	BigInt(input.wallTime),
	input.logical ?? 0,
	input.gid,
	iterableToArray(input.next),
	input.type ?? 0,
	input.metaData,
	input.payloadData,
	input.replicas,
	input.roleAgeMs ?? 0,
	integerString(input.now ?? Date.now()),
	input.selfHash ?? "",
	input.selfReplicating ?? true,
	input.resolveTrimmedEntries !== false,
];

const optionalIterableToArray = <T>(values?: Iterable<T>): T[] | undefined => {
	if (!values) {
		return undefined;
	}
	return Array.isArray(values) ? values : [...values];
};

const findLeaderArguments = (
	options?: NativeBackboneFindLeaderOptions,
): [
	number,
	string,
	string[] | undefined,
	boolean,
	string,
	boolean,
	boolean,
	boolean,
] => [
	options?.roleAge ?? 0,
	integerString(options?.now ?? Date.now()),
	optionalIterableToArray(options?.peerFilter),
	options?.expandPeerFilter === true,
	options?.selfHash ?? "",
	options?.selfReplicating === true,
	options?.fullReplicaFallback === true,
	options?.includeStrictFullReplica !== false,
];

export class NativePeerbitBackbone {
	readonly graph: NativeBackboneLogGraph;
	readonly storageBackedGraph: NativeBackboneLogGraph;
	readonly blocks: NativeBackboneBlockStore;
	private readonly documentProjectionPlanIds = new WeakMap<
		NativeBackboneSimpleDocumentProjectionPlan,
		number
	>();
	private readonly documentProjectionPlanStructuralIds = new Map<
		string,
		number
	>();

	private constructor(
		private readonly native: NativePeerbitBackboneHandle,
		private readonly resolution: RangeResolution,
	) {
		this.graph = new NativeBackboneLogGraph(native, {
			documentProjectionPlanId: this.documentProjectionPlanId.bind(this),
		});
		this.storageBackedGraph = new NativeBackboneLogGraph(native, {
			commitBlocks: false,
			documentProjectionPlanId: this.documentProjectionPlanId.bind(this),
		});
		this.blocks = new NativeBackboneBlockStore(native);
	}

	static async create(
		options: NativeBackboneOptions,
	): Promise<NativePeerbitBackbone> {
		const wasm = await loadWasm();
		const resolution = options.resolution ?? "u64";
		return new NativePeerbitBackbone(
			new wasm.NativePeerbitBackbone(
				resolution,
				options.clockId,
				options.privateKey,
				options.publicKey,
			),
			resolution,
		);
	}

	get logLength(): number {
		return this.native.log_len();
	}

	get blockLength(): number {
		return this.native.block_len();
	}

	hasLogEntry(hash: string): boolean {
		return this.native.has_log_entry(hash);
	}

	hasBlock(hash: string): boolean {
		return this.native.has_block(hash);
	}

	prepareRawReceiveBatch(
		blocks: Uint8Array[],
	): NativeBackboneRawReceivePreparedFacts[] {
		if (blocks.length === 0) {
			return [];
		}
		const columns = this.prepareRawReceiveColumnsBatch(blocks);
		if (columns) {
			return rawReceivePreparedFactsFromColumns(columns);
		}
		return this.native
			.prepare_raw_receive_batch(blocks)
			.map(rawReceivePreparedFactsFromRow);
	}

	prepareRawReceiveColumnsBatch(
		blocks: Uint8Array[],
		hashes?: string[],
		options?: { verifySignatures?: boolean },
	): NativeBackboneRawReceivePreparedFactsColumns | undefined {
		if (hashes && blocks.length !== hashes.length) {
			throw new Error("Expected equal raw receive block and hash lengths");
		}
		if (blocks.length === 0) {
			return [
				[],
				[],
				new Uint32Array(0),
				[],
				new BigUint64Array(0),
				new Uint32Array(0),
				[],
				[],
				new Uint8Array(0),
				[],
				[],
				new Uint32Array(0),
				new Uint8Array(0),
				new Uint32Array(0),
				new BigUint64Array(0),
			];
		}
		if (
			options?.verifySignatures === false &&
			hashes &&
			this.native.prepare_raw_receive_unverified_expected_columns_batch
		) {
			return this.native.prepare_raw_receive_unverified_expected_columns_batch(
				blocks,
				hashes,
			);
		}
		if (
			options?.verifySignatures === false &&
			!hashes &&
			this.native.prepare_raw_receive_unverified_columns_batch
		) {
			return this.native.prepare_raw_receive_unverified_columns_batch(blocks);
		}
		if (hashes && this.native.prepare_raw_receive_expected_columns_batch) {
			return this.native.prepare_raw_receive_expected_columns_batch(
				blocks,
				hashes,
			);
		}
		return this.native.prepare_raw_receive_columns_batch?.(blocks);
	}

	prepareRawReceiveExpectedColumnsBatch(
		blocks: Uint8Array[],
		hashes: string[],
		options?: { verifySignatures?: boolean },
	): NativeBackboneRawReceivePreparedFactsColumns | undefined {
		if (blocks.length !== hashes.length) {
			throw new Error("Expected equal raw receive block and hash lengths");
		}
		if (blocks.length === 0) {
			return this.prepareRawReceiveColumnsBatch(blocks, hashes, options);
		}
		if (
			options?.verifySignatures === false &&
			this.native.prepare_raw_receive_unverified_expected_compact_columns_batch
		) {
			return this.native.prepare_raw_receive_unverified_expected_compact_columns_batch(
				blocks,
				hashes,
			);
		}
		if (this.native.prepare_raw_receive_expected_compact_columns_batch) {
			return this.native.prepare_raw_receive_expected_compact_columns_batch(
				blocks,
				hashes,
			);
		}
		if (
			options?.verifySignatures === false &&
			this.native.prepare_raw_receive_unverified_expected_columns_batch
		) {
			return this.native.prepare_raw_receive_unverified_expected_columns_batch(
				blocks,
				hashes,
			);
		}
		if (this.native.prepare_raw_receive_expected_columns_batch) {
			return this.native.prepare_raw_receive_expected_columns_batch(
				blocks,
				hashes,
			);
		}
		return undefined;
	}

	prepareRawReceiveExpectedColumnsAndSelectionBatch(
		blocks: Uint8Array[],
		hashes: string[],
		options: {
			verifySignatures?: boolean;
			minReplicas: number;
			maxReplicas?: number;
			leaderOptions: NativeBackboneFindLeaderOptions;
			fromHash: string;
		},
	): NativeBackbonePreparedRawReceiveColumnsAndSelection | undefined {
		if (blocks.length !== hashes.length) {
			throw new Error("Expected equal raw receive block and hash lengths");
		}
		if (
			options.verifySignatures !== false ||
			!this.native
				.prepare_raw_receive_unverified_expected_compact_columns_and_selection_batch
		) {
			return undefined;
		}
		const row =
			this.native.prepare_raw_receive_unverified_expected_compact_columns_and_selection_batch(
				blocks,
				hashes,
				options.minReplicas,
				options.maxReplicas,
				...findLeaderArguments(options.leaderOptions),
				options.fromHash,
			);
		if (!row) {
			return undefined;
		}
		const [columns, selectionRow] = row;
		return {
			columns,
			selection: selectionRow
				? rawReceiveSelectionFromRow(this.resolution, selectionRow)
				: undefined,
		};
	}

	clearPreparedRawReceiveEntries(hashes: Iterable<string>): number {
		return this.native.clear_prepared_raw_receive_entries(
			iterableToArray(hashes),
		);
	}

	verifyPreparedRawReceiveEntries(
		hashes: Iterable<string>,
	): boolean[] | undefined {
		return this.graph.verifyPreparedRawReceiveEntries(hashes);
	}

	planPreparedRawReceiveGroups(
		hashes: Iterable<string>,
		options: { minReplicas: number; maxReplicas?: number },
	): NativeBackboneRawReceiveGroupPlan[] | undefined {
		if (!this.native.plan_prepared_raw_receive_groups) {
			return undefined;
		}
		const rows = this.native.plan_prepared_raw_receive_groups(
			iterableToArray(hashes),
			options.minReplicas,
			options.maxReplicas,
		);
		return rows?.map(rawReceiveGroupPlanFromRow);
	}

	planPreparedRawReceiveGroupIndexes(
		hashes: Iterable<string>,
		options: { minReplicas: number; maxReplicas?: number },
	): NativeBackboneRawReceiveGroupIndexPlan[] | undefined {
		if (!this.native.plan_prepared_raw_receive_group_indexes) {
			return undefined;
		}
		const rows = this.native.plan_prepared_raw_receive_group_indexes(
			iterableToArray(hashes),
			options.minReplicas,
			options.maxReplicas,
		);
		return rows?.map(rawReceiveGroupIndexPlanFromRow);
	}

	planPreparedRawReceiveGroupLeaders(
		hashes: Iterable<string>,
		options: { minReplicas: number; maxReplicas?: number },
		leaderOptions?: NativeBackboneFindLeaderOptions,
	): NativeBackboneRawReceiveGroupLeaderPlan[] | undefined {
		if (!this.native.plan_prepared_raw_receive_group_leaders) {
			return undefined;
		}
		const rows = this.native.plan_prepared_raw_receive_group_leaders(
			iterableToArray(hashes),
			options.minReplicas,
			options.maxReplicas,
			...findLeaderArguments(leaderOptions),
		);
		return rows?.map((row) =>
			rawReceiveGroupLeaderPlanFromRow(this.resolution, row),
		);
	}

	planPreparedRawReceiveGroupAssignments(
		hashes: Iterable<string>,
		options: { minReplicas: number; maxReplicas?: number },
		leaderOptions: NativeBackboneFindLeaderOptions,
		fromHash: string,
	): NativeBackboneRawReceiveGroupAssignmentPlan[] | undefined {
		if (!this.native.plan_prepared_raw_receive_group_assignments) {
			return undefined;
		}
		const rows = this.native.plan_prepared_raw_receive_group_assignments(
			iterableToArray(hashes),
			options.minReplicas,
			options.maxReplicas,
			...findLeaderArguments(leaderOptions),
			fromHash,
		);
		return rows?.map((row) =>
			rawReceiveGroupAssignmentPlanFromRow(this.resolution, row),
		);
	}

	planPreparedRawReceiveFastDrop(
		hashes: Iterable<string>,
		options: { minReplicas: number; maxReplicas?: number },
		leaderOptions: NativeBackboneFindLeaderOptions,
		fromHash: string,
	): NativeBackboneRawReceiveFastDropPlan | undefined {
		if (!this.native.plan_prepared_raw_receive_fast_drop) {
			return undefined;
		}
		const row = this.native.plan_prepared_raw_receive_fast_drop(
			iterableToArray(hashes),
			options.minReplicas,
			options.maxReplicas,
			...findLeaderArguments(leaderOptions),
			fromHash,
		);
		if (!row) {
			return undefined;
		}
		const [canDrop, groupCount, plannedHashCount] = row;
		return { canDrop, groupCount, plannedHashCount };
	}

	planPreparedRawReceiveSelection(
		hashes: Iterable<string>,
		options: { minReplicas: number; maxReplicas?: number },
		leaderOptions: NativeBackboneFindLeaderOptions,
		fromHash: string,
	): NativeBackboneRawReceiveSelectionPlan | undefined {
		if (!this.native.plan_prepared_raw_receive_selection) {
			return undefined;
		}
		const row = this.native.plan_prepared_raw_receive_selection(
			iterableToArray(hashes),
			options.minReplicas,
			options.maxReplicas,
			...findLeaderArguments(leaderOptions),
			fromHash,
		);
		return row ? rawReceiveSelectionFromRow(this.resolution, row) : undefined;
	}

	selectPreparedRawReceiveHashes(
		hashes: Iterable<string>,
		options: { minReplicas: number; maxReplicas?: number },
		leaderOptions: NativeBackboneFindLeaderOptions,
		fromHash: string,
	): NativeBackboneRawReceiveSelectionPlan | undefined {
		if (!this.native.select_prepared_raw_receive_hashes) {
			return undefined;
		}
		const row = this.native.select_prepared_raw_receive_hashes(
			iterableToArray(hashes),
			options.minReplicas,
			options.maxReplicas,
			...findLeaderArguments(leaderOptions),
			fromHash,
		);
		return row ? rawReceiveSelectionFromRow(this.resolution, row) : undefined;
	}

	getEntryCoordinateHashes(): string[] {
		return this.native.entry_coordinate_hashes();
	}

	getEntryCoordinates(hash: string): Array<number | bigint> | undefined {
		const coordinates = this.native.get_entry_coordinates(hash);
		return coordinates
			? rowsToNumbers(this.resolution, coordinates)
			: undefined;
	}

	getEntryHashesForHashNumbers(
		hashNumbers: Iterable<bigint | number | string>,
	): Map<bigint, string[]> {
		const rows = this.native.entry_hashes_for_hash_numbers(
			[...hashNumbers].map(integerString),
		);
		return rowsToHashNumberMap(rows);
	}

	getEntryHashesForHashNumbersU64(
		hashNumbers: BigUint64Array,
	): Map<bigint, string[]> | undefined {
		if (
			typeof BigUint64Array === "undefined" ||
			typeof this.native.entry_hashes_for_hash_numbers_u64 !== "function"
		) {
			return undefined;
		}
		return rowsToHashNumberMap(
			this.native.entry_hashes_for_hash_numbers_u64(hashNumbers),
		);
	}

	getEntryHashListForHashNumbersU64(
		hashNumbers: BigUint64Array,
	): string[] | undefined {
		if (
			typeof BigUint64Array === "undefined" ||
			typeof this.native.entry_hashes_for_hash_numbers_flat_u64 !== "function"
		) {
			return undefined;
		}
		return this.native.entry_hashes_for_hash_numbers_flat_u64(hashNumbers);
	}

	getEntryHashNumbersInRange(range: {
		start1: bigint | number | string;
		end1: bigint | number | string;
		start2: bigint | number | string;
		end2: bigint | number | string;
	}): bigint[] {
		return rowsToNumbers(
			"u64",
			this.native.entry_hash_numbers_in_range(
				integerString(range.start1),
				integerString(range.end1),
				integerString(range.start2),
				integerString(range.end2),
			),
		) as bigint[];
	}

	getEntryHashNumbersInRangeU64(range: {
		start1: bigint | number | string;
		end1: bigint | number | string;
		start2: bigint | number | string;
		end2: bigint | number | string;
	}): BigUint64Array | undefined {
		if (
			typeof BigUint64Array === "undefined" ||
			typeof this.native.entry_hash_numbers_in_range_u64 !== "function"
		) {
			return undefined;
		}
		return this.native.entry_hash_numbers_in_range_u64(
			integerString(range.start1),
			integerString(range.end1),
			integerString(range.start2),
			integerString(range.end2),
		);
	}

	countEntryCoordinatesInRanges(
		ranges: Iterable<{
			start1: bigint | number | string;
			end1: bigint | number | string;
			start2: bigint | number | string;
			end2: bigint | number | string;
		}>,
		options?: { includeAssignedToRangeBoundary?: boolean },
	): number {
		const start1: string[] = [];
		const end1: string[] = [];
		const start2: string[] = [];
		const end2: string[] = [];
		for (const range of ranges) {
			start1.push(integerString(range.start1));
			end1.push(integerString(range.end1));
			start2.push(integerString(range.start2));
			end2.push(integerString(range.end2));
		}
		return this.native.count_entry_coordinates_in_ranges(
			start1,
			end1,
			start2,
			end2,
			options?.includeAssignedToRangeBoundary === true,
		);
	}

	getEntryCoordinateFields(): NativeBackboneCoordinateFields[] {
		return this.native
			.entry_coordinate_fields()
			.map((row) => coordinateFieldsFromRow(this.resolution, row as unknown[]));
	}

	get coordinateIndexLength(): number {
		return this.native.coordinate_index_len();
	}

	get coordinateValueLength(): number {
		return this.native.coordinate_value_len();
	}

	hasCoordinateIndexHash(hash: string): boolean {
		return this.native.coordinate_index_has_hash(hash);
	}

	configureDocumentSchemaIr(
		schemaIr: Uint8Array,
	): NativeBackboneDocumentSchemaStats {
		const [rootFields, nodeCount, genericNodes] =
			this.native.configure_document_schema_ir(schemaIr);
		return { rootFields, nodeCount, genericNodes };
	}

	setDocumentByteElementIndexLimit(limit: number): void {
		this.native.set_document_byte_element_index_limit?.(limit);
	}

	setDocumentContextHeadField(field: number): void {
		this.native.set_document_context_head_field(field);
	}

	setDocumentContextFields(fields: {
		created: number;
		modified: number;
		head: number;
		gid: number;
		size: number;
	}): void {
		this.native.set_document_context_fields(
			fields.created,
			fields.modified,
			fields.head,
			fields.gid,
			fields.size,
		);
	}

	projectDocumentIndexSimple(
		encodedDocument: Uint8Array,
		plan: NativeBackboneSimpleDocumentProjectionPlan,
		context: NativeBackboneSimpleDocumentProjectionContext,
	): Uint8Array | undefined {
		try {
			return this.native.project_document_index_simple(
				encodedDocument,
				plan,
				integerString(context.created),
				integerString(context.modified),
				context.head ?? "",
				context.gid,
				context.size,
				context.signer,
			);
		} catch {
			return;
		}
	}

	setAppendProfileEnabled(enabled: boolean): void {
		this.native.set_append_profile_enabled(enabled);
	}

	resetAppendProfile(): void {
		this.native.reset_append_profile();
	}

	appendProfile(): NativeBackboneAppendProfile {
		const row = this.native.append_profile();
		return Object.fromEntries(
			nativeBackboneAppendProfileKeys.map((key, index) => [
				key,
				Number(row[index] ?? 0),
			]),
		) as NativeBackboneAppendProfile;
	}

	get documentIndexLength(): number {
		return this.native.document_index_len();
	}

	get documentValueLength(): number {
		return this.native.document_value_len();
	}

	documentExactStringFirstKey(
		field: number,
		value: string,
	): string | undefined {
		return this.native.document_exact_string_first_key(field, value);
	}

	documentValueBytes(key: string): Uint8Array | undefined {
		return this.native.document_value_bytes(key);
	}

	documentEntry(key: string): NativeBackboneDocumentEntry | undefined {
		return this.native.document_entry(key);
	}

	documentKeysExist(keys: Iterable<string>): Uint8Array {
		const batch = Array.isArray(keys) ? keys : Array.from(keys);
		if (batch.length === 0) {
			return new Uint8Array(0);
		}
		const exists = this.native.document_keys_exist;
		if (exists) {
			return exists.call(this.native, batch);
		}
		return Uint8Array.from(
			batch.map((key) => (this.documentEntry(key) ? 1 : 0)),
		);
	}

	documentFieldValue(
		key: string,
		field: number,
	): NativeBackboneDocumentFieldValue | undefined {
		return this.native.document_field_value(key, field);
	}

	documentContext(
		key: string,
	): [string, string, string, string, number] | undefined {
		return this.native.document_context(key);
	}

	documentContextBatch(
		keys: string[],
	): Array<[string, string, string, string, number] | undefined> {
		const batch = this.native.document_context_batch;
		return batch
			? batch.call(this.native, keys)
			: keys.map((key) => this.documentContext(key));
	}

	documentPreviousSignaturePublicKey(
		key: string,
	): { exists: boolean; publicKey?: Uint8Array } | undefined {
		const row = this.native.document_previous_signature_public_key?.(key);
		if (!row) {
			return;
		}
		const [exists, publicKey] = row;
		return { exists, publicKey };
	}

	documentContextsAndPreviousSignaturePublicKeys(
		keys: string[],
	):
		| Array<{
				context?: NativeBackboneDocumentContextFacts;
				publicKey?: Uint8Array;
		  }>
		| undefined {
		return this.native
			.document_context_previous_signature_public_key_batch?.(keys)
			.map(([contextRow, publicKey]) => ({
				context: documentContextFactsFromRow(contextRow),
				publicKey,
			}));
	}

	documentQuery(
		queryBytes: Uint8Array,
		sortBytes: Uint8Array,
	): NativeBackboneDocumentEntry[] {
		return this.native.document_query(queryBytes, sortBytes);
	}

	documentQueryPage(
		queryBytes: Uint8Array,
		sortBytes: Uint8Array,
		offset: number,
		limit: number,
	): NativeBackboneDocumentEntry[] {
		return this.native.document_query_page(
			queryBytes,
			sortBytes,
			offset,
			limit,
		);
	}

	private documentProjectionPlanId(
		plan: NativeBackboneSimpleDocumentProjectionPlan,
	): number {
		const cached = this.documentProjectionPlanIds.get(plan);
		if (cached !== undefined) {
			return cached;
		}
		const structuralKey = JSON.stringify(plan);
		const structuralCached =
			this.documentProjectionPlanStructuralIds.get(structuralKey);
		if (structuralCached !== undefined) {
			this.documentProjectionPlanIds.set(plan, structuralCached);
			return structuralCached;
		}
		const id = this.native.register_document_projection_plan(plan);
		this.documentProjectionPlanIds.set(plan, id);
		this.documentProjectionPlanStructuralIds.set(structuralKey, id);
		return id;
	}

	documentCount(queryBytes: Uint8Array): number {
		return this.native.document_count(queryBytes);
	}

	documentSum(
		queryBytes: Uint8Array,
		field: number,
	): ["none" | "i64" | "u64", string] {
		return this.native.document_sum(queryBytes, field);
	}

	putDocumentEncodedPartsStored(
		key: string,
		valuePrefixBytes: Uint8Array,
		valueSuffixBytes: Uint8Array,
		byteElementIndexLimit = 0,
	): void {
		this.native.put_document_encoded_parts_stored(
			key,
			valuePrefixBytes,
			valueSuffixBytes,
			byteElementIndexLimit,
		);
	}

	putDocumentEncodedPartsStoredBatch(
		values: Array<{
			key: string;
			valuePrefixBytes: Uint8Array;
			valueSuffixBytes: Uint8Array;
		}>,
		byteElementIndexLimit = 0,
	): void {
		if (values.length === 0) {
			return;
		}
		const keys = new Array<string>(values.length);
		const prefixes = new Array<Uint8Array>(values.length);
		const suffixes = new Array<Uint8Array>(values.length);
		for (let i = 0; i < values.length; i++) {
			const value = values[i]!;
			keys[i] = value.key;
			prefixes[i] = value.valuePrefixBytes;
			suffixes[i] = value.valueSuffixBytes;
		}
		this.native.put_document_encoded_parts_stored_batch(
			keys,
			prefixes,
			suffixes,
			byteElementIndexLimit,
		);
	}

	deleteDocument(key: string): boolean {
		return this.native.delete_document(key);
	}

	deleteDocuments(keys: Iterable<string>): number {
		const batch = Array.isArray(keys) ? keys : Array.from(keys);
		if (batch.length === 0) {
			return 0;
		}
		return this.native.delete_documents(batch);
	}

	deleteDocumentsResult(keys: Iterable<string>): Uint8Array {
		const batch = Array.isArray(keys) ? keys : Array.from(keys);
		if (batch.length === 0) {
			return new Uint8Array(0);
		}
		return this.native.delete_documents_result(batch);
	}

	clearDocumentIndex(): void {
		this.native.clear_document_index();
	}

	get documentPendingJournalLength(): number {
		return this.native.document_pending_journal_len();
	}

	get documentPendingJournalByteLength(): number {
		return this.native.document_pending_journal_byte_len();
	}

	get documentJournalEnabled(): boolean {
		return this.native.document_journal_enabled();
	}

	setDocumentJournalEnabled(enabled: boolean): void {
		this.native.set_document_journal_enabled(enabled);
	}

	documentJournalHeader(): Uint8Array {
		return this.native.document_journal_header();
	}

	documentJournal(): Uint8Array {
		return this.native.document_journal();
	}

	clearDocumentJournal(): void {
		this.native.clear_document_journal();
	}

	documentSnapshot(): Uint8Array {
		return this.native.document_snapshot();
	}

	loadDocumentSnapshotAndJournal(
		snapshot?: Uint8Array,
		journal?: Uint8Array,
	): number {
		return this.native.load_document_snapshot_and_journal(
			snapshot ?? new Uint8Array(),
			journal ?? new Uint8Array(),
		);
	}

	get coordinatePendingJournalLength(): number {
		return this.native.coordinate_pending_journal_len();
	}

	get coordinatePendingJournalByteLength(): number {
		return this.native.coordinate_pending_journal_byte_len();
	}

	get coordinateJournalEnabled(): boolean {
		return this.native.coordinate_journal_enabled();
	}

	setCoordinateJournalEnabled(enabled: boolean): void {
		this.native.set_coordinate_journal_enabled(enabled);
	}

	coordinateJournalHeader(): Uint8Array {
		return this.native.coordinate_journal_header();
	}

	coordinateJournal(): Uint8Array {
		return this.native.coordinate_journal();
	}

	clearCoordinateJournal(): void {
		this.native.clear_coordinate_journal();
	}

	coordinateSnapshot(): Uint8Array {
		return this.native.coordinate_snapshot();
	}

	loadCoordinateSnapshotAndJournal(
		snapshot?: Uint8Array,
		journal?: Uint8Array,
	): number {
		return this.native.load_coordinate_snapshot_and_journal(
			snapshot ?? new Uint8Array(),
			journal ?? new Uint8Array(),
		);
	}

	get documentSignerPendingJournalLength(): number {
		return this.native.document_signer_pending_journal_len();
	}

	get documentSignerPendingJournalByteLength(): number {
		return this.native.document_signer_pending_journal_byte_len();
	}

	get documentSignerJournalEnabled(): boolean {
		return this.native.document_signer_journal_enabled();
	}

	setDocumentSignerJournalEnabled(enabled: boolean): void {
		this.native.set_document_signer_journal_enabled(enabled);
	}

	documentSignerJournalHeader(): Uint8Array {
		return this.native.document_signer_journal_header();
	}

	documentSignerJournal(): Uint8Array {
		return this.native.document_signer_journal();
	}

	clearDocumentSignerJournal(): void {
		this.native.clear_document_signer_journal();
	}

	documentSignerSnapshot(): Uint8Array {
		return this.native.document_signer_snapshot();
	}

	loadDocumentSignerSnapshotAndJournal(
		snapshot?: Uint8Array,
		journal?: Uint8Array,
	): number {
		return this.native.load_document_signer_snapshot_and_journal(
			snapshot ?? new Uint8Array(),
			journal ?? new Uint8Array(),
		);
	}

	clear(): void {
		this.native.clear();
	}

	clearSharedLog(): void {
		this.native.clear_shared_log();
	}

	clearEntryCoordinates(): void {
		this.native.clear_entry_coordinates();
	}

	putRange(range: NativeBackboneRangeInput): void {
		this.native.put_range(
			range.id,
			range.hash,
			integerString(range.timestamp),
			integerString(range.start1),
			integerString(range.end1),
			integerString(range.start2),
			integerString(range.end2),
			integerString(range.width),
			range.mode,
		);
	}

	deleteRange(id: string): boolean {
		return this.native.delete_range(id);
	}

	putEntryCoordinates(
		hash: string,
		gid: string,
		coordinates: Iterable<bigint | number | string>,
		assignedToRangeBoundary: boolean,
		requestedReplicas: number,
		hashNumber: bigint | number | string,
	): void {
		this.native.put_entry_coordinates(
			hash,
			gid,
			integerString(hashNumber),
			[...coordinates].map(integerString),
			assignedToRangeBoundary,
			requestedReplicas,
		);
	}

	deleteEntryCoordinates(hash: string): boolean {
		return this.native.delete_entry_coordinates(hash);
	}

	deleteEntryCoordinatesBatch(hashes: Iterable<string>): void {
		this.native.delete_entry_coordinates_batch(iterableToArray(hashes));
	}

	commitEntryCoordinates(
		hash: string,
		gid: string,
		coordinates: Iterable<bigint | number | string>,
		nextHashes: Iterable<string>,
		assignedToRangeBoundary: boolean,
		requestedReplicas: number,
		hashNumber: bigint | number | string,
	): void {
		this.native.commit_entry_coordinates(
			hash,
			gid,
			integerString(hashNumber),
			[...coordinates].map(integerString),
			iterableToArray(nextHashes),
			assignedToRangeBoundary,
			requestedReplicas,
		);
	}

	commitEntryCoordinatesBatch(
		entries: Iterable<{
			hash: string;
			gid: string;
			coordinates: Iterable<bigint | number | string>;
			nextHashes: Iterable<string>;
			assignedToRangeBoundary: boolean;
			requestedReplicas: number;
			hashNumber: bigint | number | string;
		}>,
	): void {
		const rows = [...entries].map((entry) => {
			const coordinates = [...entry.coordinates].map(integerString);
			return {
				hash: entry.hash,
				gid: entry.gid,
				hashNumber: integerString(entry.hashNumber),
				coordinates,
				nextHashes: iterableToArray(entry.nextHashes),
				assignedToRangeBoundary: entry.assignedToRangeBoundary ? 1 : 0,
				requestedReplicas: entry.requestedReplicas,
			};
		});
		if (rows.length === 0) {
			return;
		}
		this.commitEntryCoordinatesColumnsBatch({
			hashes: rows.map((row) => row.hash),
			gids: rows.map((row) => row.gid),
			hashNumbers: rows.map((row) => row.hashNumber),
			coordinateBatches: rows.map((row) => row.coordinates),
			nextHashBatches: rows.map((row) => row.nextHashes),
			assignedToRangeBoundaries: new Uint8Array(
				rows.map((row) => row.assignedToRangeBoundary),
			),
			requestedReplicas: rows.map((row) => row.requestedReplicas),
		});
	}

	commitEntryCoordinatesColumnsBatch(
		columns: NativeBackboneCoordinateCommitColumns,
	): void {
		const {
			hashes,
			gids,
			nextHashBatches,
			assignedToRangeBoundaries,
		} = columns;
		if (hashes.length === 0) {
			return;
		}
		validateNativeBackboneCoordinateCommitColumns(columns);
		if (
			this.native.commit_entry_coordinates_batch_u64 &&
			hasNativeBackboneNumericCoordinateCommitColumns(columns)
		) {
			this.native.commit_entry_coordinates_batch_u64(
				hashes,
				gids,
				columns.hashNumberValues,
				columns.coordinateCounts,
				columns.coordinateValues,
				nextHashBatches,
				assignedToRangeBoundaries,
				columns.requestedReplicaValues,
			);
			return;
		}
		const coordinateStringColumns =
			nativeBackboneCoordinateCommitStringColumns(columns);
		if (!this.native.commit_entry_coordinates_batch) {
			for (let i = 0; i < hashes.length; i++) {
				this.native.commit_entry_coordinates(
					hashes[i]!,
					gids[i]!,
					coordinateStringColumns.hashNumbers[i]!,
					coordinateStringColumns.coordinateBatches[i]!,
					nextHashBatches[i]!,
					assignedToRangeBoundaries[i] === 1,
					coordinateStringColumns.requestedReplicas[i]!,
				);
			}
			return;
		}
		this.native.commit_entry_coordinates_batch(
			hashes,
			gids,
			coordinateStringColumns.hashNumbers,
			coordinateStringColumns.coordinateBatches,
			nextHashBatches,
			assignedToRangeBoundaries,
			coordinateStringColumns.requestedReplicas,
		);
	}

	addGidPeers(gid: string, peers: Iterable<string>, reset = false): number {
		return this.native.add_gid_peers(gid, iterableToArray(peers), reset);
	}

	removeGidPeer(peer: string, gid?: string): void {
		this.native.remove_gid_peer(peer, gid);
	}

	removeGidPeers(peer: string, gids: Iterable<string>): void {
		const gidArray = iterableToArray(gids);
		if (this.native.remove_gid_peers) {
			this.native.remove_gid_peers(peer, gidArray);
			return;
		}
		for (const gid of gidArray) {
			this.native.remove_gid_peer(peer, gid);
		}
	}

	deleteGidPeers(gid: string): boolean {
		return this.native.delete_gid_peers(gid);
	}

	clearGidPeers(): void {
		this.native.clear_gid_peers();
	}

	markEntriesKnownByPeer(hashes: Iterable<string>, peer: string): void {
		this.native.mark_entries_known_by_peer(iterableToArray(hashes), peer);
	}

	removeEntriesKnownByPeer(hashes: Iterable<string>, peer: string): void {
		this.native.remove_entries_known_by_peer(iterableToArray(hashes), peer);
	}

	removePeerFromEntryKnownPeers(peer: string): void {
		this.native.remove_peer_from_entry_known_peers(peer);
	}

	clearEntryKnownPeers(): void {
		this.native.clear_entry_known_peers();
	}

	getGrid(
		from: bigint | number | string,
		count: number,
	): Array<number | bigint> {
		return rowsToNumbers(
			this.resolution,
			this.native.get_grid(integerString(from), count),
		);
	}

	getGidCoordinates(gid: string, count: number): Array<number | bigint> {
		return rowsToNumbers(
			this.resolution,
			this.native.get_gid_coordinates(gid, count),
		);
	}

	findLeaders(
		cursors: Iterable<bigint | number | string>,
		replicas: number,
		options?: NativeBackboneFindLeaderOptions,
	): Map<string, NativeBackboneLeaderSample> {
		const rows = this.native.find_leaders(
			[...cursors].map(integerString),
			replicas,
			...findLeaderArguments(options),
		);
		return rowsToSamples(rows) ?? new Map();
	}

	findLeadersBatch(
		items: Iterable<NativeBackboneLeaderCursorBatchInput>,
		options?: NativeBackboneFindLeaderOptions,
	): Array<Map<string, NativeBackboneLeaderSample>> {
		const entries = [...items];
		const rows = this.native.find_leaders_batch(
			entries.map((entry) => [...entry.cursors].map(integerString)),
			entries.map((entry) => entry.replicas),
			...findLeaderArguments(options),
		);
		return rows.map((row) => rowsToSamples(row) ?? new Map());
	}

	planLeadersForGid(
		gid: string,
		replicas: number,
		options?: NativeBackboneFindLeaderOptions,
	): NativeBackboneLeaderPlan {
		const [coordinateRows, leaderRows] = this.native.plan_entry_leaders_for_gid(
			gid,
			replicas,
			...findLeaderArguments(options),
		);
		const coordinateStrings = coordinateRows.map((coordinate) =>
			String(coordinate),
		);
		return {
			coordinates: rowsToNumbers(this.resolution, coordinateStrings),
			coordinateStrings,
			leaders: rowsToSamples(leaderRows) ?? new Map(),
		};
	}

	planLeadersForGidsBatch(
		items: Iterable<NativeBackboneLeaderGidBatchInput>,
		options?: NativeBackboneFindLeaderOptions,
	): NativeBackboneLeaderPlan[] {
		const entries = [...items];
		const rows = this.native.plan_leaders_for_gids_batch(
			entries.map((entry) => entry.gid),
			entries.map((entry) => entry.replicas),
			...findLeaderArguments(options),
		);
		return rows.map(([coordinateRows, leaderRows]) => {
			const coordinateStrings = coordinateRows.map((coordinate) =>
				String(coordinate),
			);
			return {
				coordinates: rowsToNumbers(this.resolution, coordinateStrings),
				coordinateStrings,
				leaders: rowsToSamples(leaderRows) ?? new Map(),
			};
		});
	}

	planLeaderSamplesForGidsBatch(
		items: Iterable<NativeBackboneLeaderGidBatchInput>,
		options?: NativeBackboneFindLeaderOptions,
	): Array<Map<string, NativeBackboneLeaderSample>> | undefined {
		if (!this.native.plan_leader_samples_for_gids_batch) {
			return undefined;
		}
		const entries = iterableToArray(items);
		const rows = this.native.plan_leader_samples_for_gids_batch(
			entries.map((entry) => entry.gid),
			entries.map((entry) => entry.replicas),
			...findLeaderArguments(options),
		);
		return rows.map(
			(leaderRows) => rowsToSamples(leaderRows as unknown[]) ?? new Map(),
		);
	}

	planRequestPruneLeaderHints(
		hashes: Iterable<string>,
		skipHashes: Iterable<string>,
		options?: NativeBackboneFindLeaderOptions,
	): NativeBackboneRequestPruneHints | undefined {
		if (!this.native.plan_request_prune_leader_hints) {
			return undefined;
		}
		const [
			entryRows,
			presentBlockHashes,
			localLeaderHashes,
			peerHistoryGids,
			peerHistoryRemovedHashes,
		] = this.native.plan_request_prune_leader_hints(
			[...hashes],
			[...skipHashes],
			...findLeaderArguments(options),
		) as [unknown[], string[], string[], string[], string[]];
		const entries = new Map<
			string,
			{ hash: string; gid: string; data?: Uint8Array; replicas?: number }
		>();
		const replicaCounts = new Map<string, number>();
		for (const row of entryRows) {
			const entry = requestPruneEntryFromRow(row);
			entries.set(entry.hash, entry);
			if (entry.replicas != null) {
				replicaCounts.set(entry.hash, entry.replicas);
			}
		}
		return {
			entries,
			presentBlockHashes: new Set(presentBlockHashes),
			localLeaderHashes: new Set(localLeaderHashes),
			replicaCounts,
			peerHistoryGids,
			peerHistoryRemovedHashes: new Set(peerHistoryRemovedHashes),
		};
	}

	planRequestPruneLeaderHintColumns(
		hashes: Iterable<string>,
		skipHashes: Iterable<string>,
		options?: NativeBackboneFindLeaderOptions,
	): NativeBackboneRequestPruneHintColumns | undefined {
		if (!this.native.plan_request_prune_leader_hint_columns) {
			return undefined;
		}
		const [
			gids,
			data,
			presentBlockFlags,
			localLeaderFlags,
			replicaCounts,
			peerHistoryGids,
			peerHistoryRemovedFlags,
		] = this.native.plan_request_prune_leader_hint_columns(
			iterableToArray(hashes),
			iterableToArray(skipHashes),
			...findLeaderArguments(options),
		) as [
			Array<string | undefined>,
			Array<Uint8Array | undefined>,
			Uint8Array,
			Uint8Array,
			Uint32Array,
			string[],
			Uint8Array,
		];
		return {
			gids,
			data,
			presentBlockFlags,
			localLeaderFlags,
			replicaCounts,
			peerHistoryGids,
			peerHistoryRemovedFlags,
		};
	}

	planRequestPruneAllConfirmed(
		hashes: Iterable<string>,
		prunePeer: string,
		options?: NativeBackboneFindLeaderOptions & { omitPeerHistoryGids?: boolean },
	): NativeBackboneRequestPruneAllConfirmed | undefined {
		const hashArray = iterableToArray(hashes);
		if (
			options?.omitPeerHistoryGids === true &&
			this.native.plan_request_prune_all_confirmed_no_gid_return
		) {
			return {
				allConfirmed:
					this.native.plan_request_prune_all_confirmed_no_gid_return(
						hashArray,
						prunePeer,
						...findLeaderArguments(options),
					),
				peerHistoryGids: [],
			};
		}
		if (!this.native.plan_request_prune_all_confirmed) {
			return undefined;
		}
		const [allConfirmed, peerHistoryGids] =
			this.native.plan_request_prune_all_confirmed(
				hashArray,
				prunePeer,
				...findLeaderArguments(options),
			) as [boolean, string[]];
		return {
			allConfirmed,
			peerHistoryGids,
		};
	}

	planEntryAssignmentForGid(
		gid: string,
		replicas: number,
		options?: NativeBackboneFindLeaderOptions,
	): NativeBackboneEntryAssignmentPlan {
		const [coordinateRows, leaderRows, assignedToRangeBoundary] =
			this.native.plan_entry_assignment_for_gid(
				gid,
				replicas,
				...findLeaderArguments(options),
			);
		return {
			coordinates: rowsToNumbers(this.resolution, coordinateRows),
			leaders: rowsToSamples(leaderRows) ?? new Map(),
			assignedToRangeBoundary,
		};
	}

	planRepairDispatchForEntries(
		input: NativeBackboneRepairDispatchInput,
		options?: NativeBackboneFindLeaderOptions,
	): NativeBackboneRepairDispatchPlan {
		const entries = [...input.entries];
		const pendingModes = [...input.pendingModes];
		const rows = this.native.plan_repair_dispatch_for_entries(
			entries.map((entry) => entry.hash),
			entries.map((entry) => entry.gid),
			entries.map((entry) => entry.requestedReplicas),
			entries.map((entry) => [...entry.coordinates].map(integerString)),
			pendingModes,
			pendingModes.map((mode) => [
				...(input.pendingPeersByMode.get(mode) ?? []),
			]),
			pendingModes.map((mode) => {
				const optimisticByGid = input.optimisticPeersByMode?.get(mode);
				return entries.map((entry) => [
					...(optimisticByGid?.get(entry.gid) ?? []),
				]);
			}),
			input.fullReplicaRepairCandidates
				? [...input.fullReplicaRepairCandidates]
				: [],
			input.fullReplicaRepairCandidateCount,
			...findLeaderArguments({
				...options,
				selfHash: input.selfHash,
			}),
		);
		return rowsToRepairDispatchPlan(rows);
	}

	planRepairDispatchForResidentEntries(
		input: NativeBackboneResidentRepairDispatchInput,
		options?: NativeBackboneFindLeaderOptions,
	): NativeBackboneRepairDispatchPlan {
		const pendingModes = [...input.pendingModes];
		const optimisticGidsByMode: string[][] = [];
		const optimisticPeersByGidByMode: string[][][] = [];
		for (const mode of pendingModes) {
			const optimisticByGid = input.optimisticPeersByMode?.get(mode);
			const gids: string[] = [];
			const peersByGid: string[][] = [];
			if (optimisticByGid) {
				for (const [gid, peers] of optimisticByGid) {
					gids.push(gid);
					peersByGid.push([...peers]);
				}
			}
			optimisticGidsByMode.push(gids);
			optimisticPeersByGidByMode.push(peersByGid);
		}

		const rows = this.native.plan_repair_dispatch_for_resident_entries(
			pendingModes,
			pendingModes.map((mode) => [
				...(input.pendingPeersByMode.get(mode) ?? []),
			]),
			optimisticGidsByMode,
			optimisticPeersByGidByMode,
			input.fullReplicaRepairCandidates
				? [...input.fullReplicaRepairCandidates]
				: [],
			input.fullReplicaRepairCandidateCount,
			...findLeaderArguments({
				...options,
				selfHash: input.selfHash,
			}),
		);
		return rowsToRepairDispatchPlan(rows);
	}

	planLocalAppendForGidCompact(
		input: {
			entryHash: string;
			gid: string;
			hashNumber?: bigint | number | string;
			nextHashes?: Iterable<string>;
			replicas: number;
			selfHash: string;
		},
		options?: NativeBackboneFindLeaderOptions,
	): NativeBackboneAppendPlan {
		const [leaderRows, isLeader, assignedToRangeBoundary, coordinatePlanRow] =
			this.native.plan_local_append_for_gid_compact(
				input.entryHash,
				input.gid,
				integerString(input.hashNumber ?? 0),
				iterableToArray(input.nextHashes),
				input.replicas,
				...findLeaderArguments({
					...options,
					selfHash: input.selfHash,
				}),
			);
		const coordinate = appendCoordinatePlanFromRow(
			this.resolution,
			coordinatePlanRow,
		);
		return {
			coordinates: coordinate.coordinates,
			leaders: rowsToSamples(leaderRows),
			isLeader,
			assignedToRangeBoundary,
			coordinate,
		};
	}

	commitLocalAppendForGidCompact(
		input: {
			entryHash: string;
			gid: string;
			hashNumber?: bigint | number | string;
			nextHashes?: Iterable<string>;
			deleteHashes?: Iterable<string>;
			replicas: number;
			selfHash: string;
		},
		options?: NativeBackboneFindLeaderOptions,
	): NativeBackboneAppendPlan {
		const [leaderRows, isLeader, assignedToRangeBoundary, coordinatePlanRow] =
			this.native.commit_local_append_for_gid_compact(
				input.entryHash,
				input.gid,
				integerString(input.hashNumber ?? 0),
				iterableToArray(input.nextHashes),
				iterableToArray(input.deleteHashes),
				input.replicas,
				...findLeaderArguments({
					...options,
					selfHash: input.selfHash,
				}),
			);
		const coordinate = appendCoordinatePlanFromRow(
			this.resolution,
			coordinatePlanRow,
		);
		return {
			coordinates: coordinate.coordinates,
			leaders: rowsToSamples(leaderRows),
			isLeader,
			assignedToRangeBoundary,
			coordinate,
		};
	}

	planAppendForGid(
		input: {
			entryHash: string;
			gid: string;
			hashNumber?: bigint | number | string;
			nextHashes?: Iterable<string>;
			replicas: number;
			fullReplicaCandidates?: Iterable<string>;
			fallbackRecipients?: Iterable<string>;
			selfHash: string;
			deliveryEnabled: boolean;
			reliabilityAck: boolean;
			minAcks?: number;
			requireRecipients: boolean;
		},
		options?: NativeBackboneFindLeaderOptions,
	): NativeBackboneAppendPlan {
		const [
			coordinateRows,
			leaderRows,
			isLeader,
			assignedToRangeBoundary,
			delivery,
			coordinatePlanRow,
		] = this.native.plan_append_for_gid(
			input.entryHash,
			input.gid,
			integerString(input.hashNumber ?? 0),
			iterableToArray(input.nextHashes),
			input.replicas,
			iterableToArray(input.fullReplicaCandidates),
			iterableToArray(input.fallbackRecipients),
			input.selfHash,
			input.deliveryEnabled,
			input.reliabilityAck,
			input.minAcks,
			input.requireRecipients,
			...findLeaderArguments({
				...options,
				selfHash: input.selfHash,
			}),
		);
		return {
			coordinates: rowsToNumbers(this.resolution, coordinateRows),
			leaders: rowsToSamples(leaderRows),
			isLeader,
			assignedToRangeBoundary,
			delivery: appendDeliveryPlanFromRow(delivery),
			coordinate: appendCoordinatePlanFromRow(
				this.resolution,
				coordinatePlanRow,
			),
		};
	}

	planAppendForGidsBatch(
		input: {
			entries: Iterable<NativeBackboneAppendEntryBatchInput>;
			fullReplicaCandidates?: Iterable<string>;
			fallbackRecipients?: Iterable<string>;
			selfHash: string;
			deliveryEnabled: boolean;
			reliabilityAck: boolean;
			minAcks?: number;
			requireRecipients: boolean;
		},
		options?: NativeBackboneFindLeaderOptions,
	): NativeBackboneAppendPlan[] {
		const entries = [...input.entries];
		const rows = this.native.plan_append_for_gids_batch(
			entries.map((entry) => entry.entryHash),
			entries.map((entry) => entry.gid),
			entries.map((entry) => integerString(entry.hashNumber ?? 0)),
			entries.map((entry) => iterableToArray(entry.nextHashes)),
			entries.map((entry) => entry.replicas),
			iterableToArray(input.fullReplicaCandidates),
			iterableToArray(input.fallbackRecipients),
			input.selfHash,
			input.deliveryEnabled,
			input.reliabilityAck,
			input.minAcks,
			input.requireRecipients,
			...findLeaderArguments({
				...options,
				selfHash: input.selfHash,
			}),
		);
		return rows.map(
			([
				coordinateRows,
				leaderRows,
				isLeader,
				assignedToRangeBoundary,
				delivery,
				coordinatePlanRow,
			]) => ({
				coordinates: rowsToNumbers(this.resolution, coordinateRows),
				leaders: rowsToSamples(leaderRows),
				isLeader,
				assignedToRangeBoundary,
				delivery: appendDeliveryPlanFromRow(delivery),
				coordinate: appendCoordinatePlanFromRow(
					this.resolution,
					coordinatePlanRow,
				),
			}),
		);
	}

	planReceiveCoordinatesForGidsBatch(
		input: {
			entries: Iterable<NativeBackboneAppendEntryBatchInput>;
			selfHash: string;
		},
		options?: NativeBackboneFindLeaderOptions,
	): NativeBackboneReceiveCoordinatePlan[] {
		const entries = [...input.entries];
		const rows = this.native.plan_receive_coordinates_for_gids_batch(
			entries.map((entry) => entry.entryHash),
			entries.map((entry) => entry.gid),
			entries.map((entry) => integerString(entry.hashNumber ?? 0)),
			entries.map((entry) => iterableToArray(entry.nextHashes)),
			entries.map((entry) => entry.replicas),
			...findLeaderArguments({
				...options,
				selfHash: input.selfHash,
			}),
		);
		return rows.map(
			([
				coordinateRows,
				leaderRows,
				isLeader,
				assignedToRangeBoundary,
				coordinatePlanRow,
			]) => ({
				coordinates: rowsToNumbers(this.resolution, coordinateRows),
				leaders: rowsToSamples(leaderRows),
				isLeader,
				assignedToRangeBoundary,
				coordinate: appendCoordinatePlanFromRow(
					this.resolution,
					coordinatePlanRow,
				),
			}),
		);
	}

	appendPlainNoNextTransaction(
		input: NativeBackboneAppendInput,
	): NativeBackboneAppendResult {
		return this.preparePlainCommittedNoNextStorageAppendTransaction(input);
	}

	preparePlainNoNextStorageAppendTransaction(
		input: NativeBackboneStorageAppendInput,
	): NativeBackboneStorageAppendResult {
		const baseArgs = nativeNoNextStorageAppendArgs(input);
		const documentIndexArgs = nativeDocumentIndexArgs(input.documentIndex);
		const row =
			input.trimLengthTo == null
				? documentIndexArgs
					? this.native.prepare_plain_no_next_storage_append_document_index_transaction(
							...baseArgs,
							...documentIndexArgs,
						)
					: this.native.prepare_plain_no_next_storage_append_transaction(
							...baseArgs,
						)
				: documentIndexArgs
					? this.native.prepare_plain_no_next_storage_append_document_index_transaction_trim(
							...baseArgs,
							...documentIndexArgs,
							input.trimLengthTo,
						)
					: this.native.prepare_plain_no_next_storage_append_transaction_trim(
							...baseArgs,
							input.trimLengthTo,
						);
		return storageAppendResultFromRow(this.resolution, row);
	}

	preparePlainStorageAppendTransaction(
		input: NativeBackboneStorageAppendInput,
	): NativeBackboneStorageAppendResult {
		const baseArgs = nativeStorageAppendArgs(input);
		const documentIndexArgs = nativeDocumentIndexArgs(input.documentIndex);
		const row =
			input.trimLengthTo == null
				? documentIndexArgs
					? this.native.prepare_plain_storage_append_document_index_transaction(
							...baseArgs,
							...documentIndexArgs,
						)
					: this.native.prepare_plain_storage_append_transaction(...baseArgs)
				: documentIndexArgs
					? this.native.prepare_plain_storage_append_document_index_transaction_trim(
							...baseArgs,
							...documentIndexArgs,
							input.trimLengthTo,
						)
					: this.native.prepare_plain_storage_append_transaction_trim(
							...baseArgs,
							input.trimLengthTo,
						);
		return storageAppendResultFromRow(this.resolution, row);
	}

	preparePlainCommittedStorageAppendTransaction(
		input: NativeBackboneStorageAppendInput,
	): NativeBackboneAppendResult {
		if (input.documentDeleteKey && input.documentIndex) {
			throw new Error(
				"Native backbone append cannot both put and delete a document index row",
			);
		}
		if (input.documentDeleteKey) {
			const baseArgs = nativeStorageAppendArgs(input);
			const row =
				input.trimLengthTo == null
					? this.native.prepare_plain_committed_storage_append_document_delete_transaction(
							...baseArgs,
							input.documentDeleteKey,
						)
					: this.native.prepare_plain_committed_storage_append_document_delete_transaction_trim(
							...baseArgs,
							input.documentDeleteKey,
							input.trimLengthTo,
						);
			return committedStorageAppendResultFromRow(this.resolution, row);
		}
		const documentIndex = input.documentIndex;
		if (documentIndex?.useLatestContext) {
			if (input.resolveTrimmedEntries === false) {
				const requiredPreviousSignerPublicKey =
					documentIndex.requiredPreviousSignerPublicKey;
				if (requiredPreviousSignerPublicKey) {
					const nativeCompactRequired =
						this.native
							.prepare_plain_committed_storage_append_document_index_latest_required_previous_signer_compact_transaction;
					if (nativeCompactRequired) {
						const row = nativeCompactRequired.call(
							this.native,
							BigInt(input.wallTime),
							input.logical ?? 0,
							input.gid,
							input.type ?? 0,
							input.metaData,
							input.payloadData,
							input.replicas,
							input.roleAgeMs ?? 0,
							integerString(input.now ?? Date.now()),
							input.selfHash ?? "",
							input.selfReplicating ?? true,
							documentIndex.key,
							documentIndex.valuePrefixBytes ?? EMPTY_UINT8_ARRAY,
							documentIndex.byteElementIndexLimit ?? 0,
							documentIndex.deleteTrimmedHeads === true,
							documentIndex.projection?.plan,
							documentIndex.projection?.encodedDocument,
							documentIndex.projection?.signer,
							requiredPreviousSignerPublicKey,
							input.trimLengthTo,
						);
						return compactCommittedLatestStorageAppendResultFromRow(
							this.resolution,
							row,
						);
					}
				}
				const projection = documentIndex.projection;
					if (projection) {
						const projectionPlanId = this.documentProjectionPlanId(projection.plan);
						const nativeCompactPlainPutPayload =
							documentIndex.usePlainPutPayload === true
							? this.native
									.prepare_plain_committed_storage_append_document_index_latest_cached_plan_compact_plain_put_payload_transaction
							: undefined;
					if (nativeCompactPlainPutPayload) {
						const row = nativeCompactPlainPutPayload.call(
							this.native,
							BigInt(input.wallTime),
							input.logical ?? 0,
							input.gid,
							input.type ?? 0,
							input.metaData,
							input.payloadData,
							input.replicas,
							input.roleAgeMs ?? 0,
							integerString(input.now ?? Date.now()),
							input.selfHash ?? "",
							input.selfReplicating ?? true,
							documentIndex.key,
							documentIndex.byteElementIndexLimit ?? 0,
							documentIndex.deleteTrimmedHeads === true,
							projectionPlanId,
							projection.signer,
							input.trimLengthTo,
						);
						return compactCommittedLatestStorageAppendResultFromRow(
							this.resolution,
							row,
						);
					}
					const nativeCompactCached =
						this.native
							.prepare_plain_committed_storage_append_document_index_latest_cached_plan_compact_transaction;
					if (nativeCompactCached) {
						const row = nativeCompactCached.call(
							this.native,
							BigInt(input.wallTime),
							input.logical ?? 0,
							input.gid,
							input.type ?? 0,
							input.metaData,
							input.payloadData,
							input.replicas,
							input.roleAgeMs ?? 0,
							integerString(input.now ?? Date.now()),
							input.selfHash ?? "",
							input.selfReplicating ?? true,
							documentIndex.key,
							documentIndex.byteElementIndexLimit ?? 0,
							documentIndex.deleteTrimmedHeads === true,
							projectionPlanId,
							projection.encodedDocument,
							projection.signer,
							input.trimLengthTo,
						);
						return compactCommittedLatestStorageAppendResultFromRow(
							this.resolution,
								row,
							);
						}
					}
					if (
						!requiredPreviousSignerPublicKey &&
						documentIndex.usePlainPutPayload === true
					) {
						const nativeCompactPlainPutPayload =
							this.native
								.prepare_plain_committed_storage_append_document_index_latest_compact_plain_put_payload_transaction;
						if (nativeCompactPlainPutPayload) {
							const row = nativeCompactPlainPutPayload.call(
								this.native,
								BigInt(input.wallTime),
								input.logical ?? 0,
								input.gid,
								input.type ?? 0,
								input.metaData,
								input.payloadData,
								input.replicas,
								input.roleAgeMs ?? 0,
								integerString(input.now ?? Date.now()),
								input.selfHash ?? "",
								input.selfReplicating ?? true,
								documentIndex.key,
								documentIndex.byteElementIndexLimit ?? 0,
								documentIndex.deleteTrimmedHeads === true,
								input.trimLengthTo,
							);
							return compactCommittedLatestStorageAppendResultFromRow(
								this.resolution,
								row,
							);
						}
					}
					const nativeCompact =
						this.native
							.prepare_plain_committed_storage_append_document_index_latest_compact_transaction;
				if (nativeCompact) {
					const row = nativeCompact.call(
						this.native,
						BigInt(input.wallTime),
						input.logical ?? 0,
						input.gid,
						input.type ?? 0,
						input.metaData,
						input.payloadData,
						input.replicas,
						input.roleAgeMs ?? 0,
						integerString(input.now ?? Date.now()),
						input.selfHash ?? "",
						input.selfReplicating ?? true,
						documentIndex.key,
						documentIndex.valuePrefixBytes ?? EMPTY_UINT8_ARRAY,
						documentIndex.byteElementIndexLimit ?? 0,
						documentIndex.deleteTrimmedHeads === true,
						documentIndex.projection?.plan,
						documentIndex.projection?.encodedDocument,
						documentIndex.projection?.signer,
						input.trimLengthTo,
					);
					return compactCommittedLatestStorageAppendResultFromRow(
						this.resolution,
						row,
					);
				}
			}
			const requiredPreviousSignerPublicKey =
				documentIndex.requiredPreviousSignerPublicKey;
			if (requiredPreviousSignerPublicKey) {
				const requiredPreviousSignerTransaction =
					this.native
						.prepare_plain_committed_storage_append_document_index_latest_required_previous_signer_transaction;
				if (!requiredPreviousSignerTransaction) {
					throw new Error(
						"Native backbone requires previous-signer policy transaction support",
					);
				}
				const row = requiredPreviousSignerTransaction.call(
					this.native,
					BigInt(input.wallTime),
					input.logical ?? 0,
					input.gid,
					input.type ?? 0,
					input.metaData,
					input.payloadData,
					input.replicas,
					input.roleAgeMs ?? 0,
					integerString(input.now ?? Date.now()),
					input.selfHash ?? "",
					input.selfReplicating ?? true,
					input.resolveTrimmedEntries !== false,
					documentIndex.key,
					documentIndex.valuePrefixBytes ?? EMPTY_UINT8_ARRAY,
					documentIndex.byteElementIndexLimit ?? 0,
					documentIndex.deleteTrimmedHeads === true,
					documentIndex.projection?.plan,
					documentIndex.projection?.encodedDocument,
					documentIndex.projection?.signer,
					requiredPreviousSignerPublicKey,
					input.trimLengthTo,
				);
				return committedStorageAppendResultFromRow(this.resolution, row);
			}
			const projection = documentIndex.projection;
			if (projection) {
				const row =
					this.native.prepare_plain_committed_storage_append_document_index_latest_cached_plan_transaction(
						BigInt(input.wallTime),
						input.logical ?? 0,
						input.gid,
						input.type ?? 0,
						input.metaData,
						input.payloadData,
						input.replicas,
						input.roleAgeMs ?? 0,
						integerString(input.now ?? Date.now()),
						input.selfHash ?? "",
						input.selfReplicating ?? true,
						input.resolveTrimmedEntries !== false,
						documentIndex.key,
						documentIndex.byteElementIndexLimit ?? 0,
						documentIndex.deleteTrimmedHeads === true,
						this.documentProjectionPlanId(projection.plan),
						projection.encodedDocument,
						projection.signer,
						input.trimLengthTo,
					);
				return committedStorageAppendResultFromRow(this.resolution, row);
			}
			const row =
				this.native.prepare_plain_committed_storage_append_document_index_latest_transaction(
					BigInt(input.wallTime),
					input.logical ?? 0,
					input.gid,
					input.type ?? 0,
					input.metaData,
					input.payloadData,
					input.replicas,
					input.roleAgeMs ?? 0,
					integerString(input.now ?? Date.now()),
					input.selfHash ?? "",
					input.selfReplicating ?? true,
					input.resolveTrimmedEntries !== false,
					documentIndex.key,
					documentIndex.valuePrefixBytes ?? EMPTY_UINT8_ARRAY,
					documentIndex.byteElementIndexLimit ?? 0,
					documentIndex.deleteTrimmedHeads === true,
					documentIndex.projection?.plan,
					documentIndex.projection?.encodedDocument,
					documentIndex.projection?.signer,
					input.trimLengthTo,
				);
			return committedStorageAppendResultFromRow(this.resolution, row);
		}
		const baseArgs = nativeStorageAppendArgs(input);
		const documentIndexArgs = nativeDocumentIndexArgs(documentIndex);
		const row =
			input.trimLengthTo == null
				? documentIndexArgs
					? this.native.prepare_plain_committed_storage_append_document_index_transaction(
							...baseArgs,
							...documentIndexArgs,
						)
					: this.native.prepare_plain_committed_storage_append_transaction(
							...baseArgs,
						)
				: documentIndexArgs
					? this.native.prepare_plain_committed_storage_append_document_index_transaction_trim(
							...baseArgs,
							...documentIndexArgs,
							input.trimLengthTo,
						)
					: this.native.prepare_plain_committed_storage_append_transaction_trim(
							...baseArgs,
							input.trimLengthTo,
						);
		return committedStorageAppendResultFromRow(this.resolution, row);
	}

	preparePlainCommittedStorageAppendDocumentIndexLatestBatchTransaction(
		input: NativeBackboneCommittedLatestDocumentIndexBatchInput,
	): NativeBackboneAppendResult[] | undefined {
		if (input.entries.length === 0) {
			return [];
		}
		const useCompact = input.resolveTrimmedEntries === false;
		const requiredPreviousSignerPublicKeys = input.entries
			.map((entry) => entry.documentIndex.requiredPreviousSignerPublicKey)
			.filter((key): key is Uint8Array => !!key);
		const requiredPreviousSignerPublicKey =
			requiredPreviousSignerPublicKeys[0];
		if (
			requiredPreviousSignerPublicKeys.length > 0 &&
			requiredPreviousSignerPublicKeys.length !== input.entries.length
		) {
			return undefined;
		}
		if (
			requiredPreviousSignerPublicKey &&
			requiredPreviousSignerPublicKeys.some(
				(key) =>
					key.byteLength !== requiredPreviousSignerPublicKey.byteLength ||
					key.some((byte, index) => byte !== requiredPreviousSignerPublicKey[index]),
			)
		) {
			return undefined;
		}
			const projected = input.entries.every(
				(entry) => entry.documentIndex.projection,
			);
			if (projected) {
				const usePlainPutPayload = input.entries.every(
					(entry) => entry.documentIndex.usePlainPutPayload === true,
				);
			const baseArgs = [
				new BigUint64Array(
					input.entries.map((entry) => BigInt(entry.wallTime)),
				),
				new Uint32Array(
					input.entries.map((entry) => entry.logical ?? 0),
				),
				input.entries.map((entry) => entry.gid),
				input.type ?? 0,
				input.entries.map((entry) => entry.metaData),
				input.entries.map((entry) => entry.payloadData),
				input.replicas,
				input.roleAgeMs ?? 0,
				integerString(input.now ?? Date.now()),
				input.selfHash ?? "",
				input.selfReplicating ?? true,
			] as const;
			const documentPlanArgs = [
				input.entries.map((entry) => entry.documentIndex.key),
				input.documentByteElementIndexLimit ?? 0,
				input.documentDeleteTrimmedHeads === true,
				new Uint32Array(
					input.entries.map((entry) =>
						this.documentProjectionPlanId(
							entry.documentIndex.projection!.plan,
						),
					),
				),
			] as const;
			const documentEncodedDocuments = input.entries.map(
				(entry) => entry.documentIndex.projection!.encodedDocument,
			);
			const documentSigners = input.entries.map(
				(entry) => entry.documentIndex.projection!.signer,
			);
			const nativeCompactCachedBatch =
				this.native
					.prepare_plain_committed_storage_append_document_index_latest_cached_plan_compact_batch_transaction;
				const nativeCompactCachedPlainPayloadBatch =
					this.native
						.prepare_plain_committed_storage_append_document_index_latest_cached_plan_compact_plain_put_payload_batch_transaction;
				if (requiredPreviousSignerPublicKey) {
					if (useCompact) {
						const nativeRequiredCompactCachedBatch =
							this.native
								.prepare_plain_committed_storage_append_document_index_latest_required_previous_signer_cached_plan_compact_batch_transaction;
						if (!nativeRequiredCompactCachedBatch) {
							return undefined;
						}
						const rows = nativeRequiredCompactCachedBatch.call(
							this.native,
							...baseArgs,
							...documentPlanArgs,
							documentEncodedDocuments,
							documentSigners,
							requiredPreviousSignerPublicKey,
							input.trimLengthTo,
						);
						return rows.map((row) =>
							compactCommittedLatestStorageAppendResultFromRow(
								this.resolution,
								row,
							),
						);
					}
					const nativeRequiredCachedBatch =
						this.native
							.prepare_plain_committed_storage_append_document_index_latest_required_previous_signer_cached_plan_batch_transaction;
					if (!nativeRequiredCachedBatch) {
						return undefined;
					}
					const rows = nativeRequiredCachedBatch.call(
						this.native,
						...baseArgs,
						input.resolveTrimmedEntries !== false,
						...documentPlanArgs,
						documentEncodedDocuments,
						documentSigners,
						requiredPreviousSignerPublicKey,
						input.trimLengthTo,
					);
					return rows.map((row) =>
						committedStorageAppendResultFromRow(this.resolution, row),
					);
				}
				if (
					useCompact &&
					usePlainPutPayload &&
				nativeCompactCachedPlainPayloadBatch
			) {
				const rows = nativeCompactCachedPlainPayloadBatch.call(
					this.native,
					...baseArgs,
					...documentPlanArgs,
					documentSigners,
					input.trimLengthTo,
				);
				return rows.map((row) =>
					compactCommittedLatestStorageAppendResultFromRow(
						this.resolution,
						row,
					),
				);
			}
			if (useCompact && nativeCompactCachedBatch) {
				const rows = nativeCompactCachedBatch.call(
					this.native,
					...baseArgs,
					...documentPlanArgs,
					documentEncodedDocuments,
					documentSigners,
					input.trimLengthTo,
				);
				return rows.map((row) =>
					compactCommittedLatestStorageAppendResultFromRow(
						this.resolution,
						row,
					),
				);
			}
			const nativeCachedBatch =
				this.native
					.prepare_plain_committed_storage_append_document_index_latest_cached_plan_batch_transaction;
			if (!nativeCachedBatch) {
				return undefined;
			}
			const rows = nativeCachedBatch.call(
				this.native,
				...baseArgs,
				input.resolveTrimmedEntries !== false,
				...documentPlanArgs,
				documentEncodedDocuments,
				documentSigners,
				input.trimLengthTo,
			);
			return rows.map((row) =>
				committedStorageAppendResultFromRow(this.resolution, row),
			);
		}
		if (
			input.entries.some((entry) => entry.documentIndex.projection) ||
			input.entries.some(
				(entry) => !entry.documentIndex.valuePrefixBytes,
			)
		) {
			return undefined;
		}
		const baseArgs = [
			new BigUint64Array(
				input.entries.map((entry) => BigInt(entry.wallTime)),
			),
			new Uint32Array(input.entries.map((entry) => entry.logical ?? 0)),
			input.entries.map((entry) => entry.gid),
			input.type ?? 0,
			input.entries.map((entry) => entry.metaData),
			input.entries.map((entry) => entry.payloadData),
			input.replicas,
			input.roleAgeMs ?? 0,
			integerString(input.now ?? Date.now()),
			input.selfHash ?? "",
			input.selfReplicating ?? true,
		] as const;
			const documentKeys = input.entries.map((entry) => entry.documentIndex.key);
			const usePlainPutPayload = input.entries.every(
				(entry) => entry.documentIndex.usePlainPutPayload === true,
			);
			if (
				!requiredPreviousSignerPublicKey &&
				useCompact &&
				usePlainPutPayload
			) {
				const nativeCompactPlainPutPayloadBatch =
					this.native
						.prepare_plain_committed_storage_append_document_index_latest_compact_plain_put_payload_batch_transaction;
				if (nativeCompactPlainPutPayloadBatch) {
					const rows = nativeCompactPlainPutPayloadBatch.call(
						this.native,
						...baseArgs,
						documentKeys,
						input.documentByteElementIndexLimit ?? 0,
						input.documentDeleteTrimmedHeads === true,
						input.trimLengthTo,
					);
					return rows.map((row) =>
						compactCommittedLatestStorageAppendResultFromRow(
							this.resolution,
							row,
						),
					);
				}
			}
			const documentValuePrefixBytes = input.entries.map(
				(entry) => entry.documentIndex.valuePrefixBytes!,
			);
		const documentByteElementIndexLimit =
			input.documentByteElementIndexLimit ?? 0;
		const documentDeleteTrimmedHeads =
			input.documentDeleteTrimmedHeads === true;
		const documentArgs = [
			documentKeys,
			documentValuePrefixBytes,
			documentByteElementIndexLimit,
			documentDeleteTrimmedHeads,
			input.trimLengthTo,
		] as const;
		if (requiredPreviousSignerPublicKey) {
			if (useCompact) {
				const nativeRequiredCompactBatch =
					this.native
						.prepare_plain_committed_storage_append_document_index_latest_required_previous_signer_compact_batch_transaction;
				if (!nativeRequiredCompactBatch) {
					return undefined;
				}
				const rows = nativeRequiredCompactBatch.call(
					this.native,
					...baseArgs,
					documentKeys,
					documentValuePrefixBytes,
					documentByteElementIndexLimit,
					documentDeleteTrimmedHeads,
					requiredPreviousSignerPublicKey,
					input.trimLengthTo,
				);
				return rows.map((row) =>
					compactCommittedLatestStorageAppendResultFromRow(
						this.resolution,
						row,
					),
				);
			}
			const nativeRequiredBatch =
				this.native
					.prepare_plain_committed_storage_append_document_index_latest_required_previous_signer_batch_transaction;
			if (!nativeRequiredBatch) {
				return undefined;
			}
			const rows = nativeRequiredBatch.call(
				this.native,
				...baseArgs,
				input.resolveTrimmedEntries !== false,
				documentKeys,
				documentValuePrefixBytes,
				documentByteElementIndexLimit,
				documentDeleteTrimmedHeads,
				requiredPreviousSignerPublicKey,
				input.trimLengthTo,
			);
			return rows.map((row) =>
				committedStorageAppendResultFromRow(this.resolution, row),
			);
		}
		const nativeCompactBatch =
			this.native
				.prepare_plain_committed_storage_append_document_index_latest_compact_batch_transaction;
		if (useCompact && nativeCompactBatch) {
			const rows = nativeCompactBatch.call(
				this.native,
				...baseArgs,
				...documentArgs,
			);
			return rows.map((row) =>
				compactCommittedLatestStorageAppendResultFromRow(
					this.resolution,
					row,
				),
			);
		}
		const nativeBatch =
			this.native
				.prepare_plain_committed_storage_append_document_index_latest_batch_transaction;
		if (!nativeBatch) {
			return undefined;
		}
		const rows = nativeBatch.call(
			this.native,
			...baseArgs,
			input.resolveTrimmedEntries !== false,
			...documentArgs,
		);
		return rows.map((row) =>
			committedStorageAppendResultFromRow(this.resolution, row),
		);
	}

	preparePlainCommittedNoNextStorageAppendTransaction(
		input: NativeBackboneStorageAppendInput,
	): NativeBackboneAppendResult {
		const baseArgs = nativeNoNextStorageAppendArgs(input);
		const projection = input.documentIndex?.projection;
		if (input.documentIndex && projection) {
			const documentIndexArgs = [
				input.documentIndex.key,
				input.documentIndex.existingCreated == null
					? ""
					: integerString(input.documentIndex.existingCreated),
				input.documentIndex.byteElementIndexLimit ?? 0,
				input.documentIndex.deleteTrimmedHeads === true,
				this.documentProjectionPlanId(projection.plan),
				projection.encodedDocument,
				projection.signer,
			] as const;
			const row =
				input.trimLengthTo == null
					? this.native.prepare_plain_committed_no_next_storage_append_document_index_cached_plan_transaction(
							...baseArgs,
							...documentIndexArgs,
						)
					: this.native.prepare_plain_committed_no_next_storage_append_document_index_cached_plan_transaction_trim(
							...baseArgs,
							...documentIndexArgs,
							input.trimLengthTo,
						);
			return committedStorageAppendResultFromRow(this.resolution, row);
		}
		const documentIndexArgs = nativeDocumentIndexArgs(input.documentIndex);
		const row =
			input.trimLengthTo == null
				? documentIndexArgs
					? this.native.prepare_plain_committed_no_next_storage_append_document_index_transaction(
							...baseArgs,
							...documentIndexArgs,
						)
					: this.native.prepare_plain_committed_no_next_storage_append_transaction(
							...baseArgs,
						)
				: documentIndexArgs
					? this.native.prepare_plain_committed_no_next_storage_append_document_index_transaction_trim(
							...baseArgs,
							...documentIndexArgs,
							input.trimLengthTo,
						)
					: this.native.prepare_plain_committed_no_next_storage_append_transaction_trim(
							...baseArgs,
							input.trimLengthTo,
						);
		return committedStorageAppendResultFromRow(this.resolution, row);
	}

	preparePlainCommittedNoNextStorageAppendDocumentIndexCompactTransaction(
		input: NativeBackboneStorageAppendInput,
	): NativeBackboneAppendResult {
		const documentIndex = input.documentIndex;
		if (!documentIndex) {
			return this.preparePlainCommittedNoNextStorageAppendTransaction(input);
		}
		const projection = documentIndex.projection;
			if (projection) {
				const projectionPlanId = this.documentProjectionPlanId(projection.plan);
				const plainPutPayload =
					documentIndex.usePlainPutPayload === true
					? this.native
							.prepare_plain_committed_no_next_storage_append_document_index_cached_plan_compact_plain_put_payload_transaction
					: undefined;
			const row = plainPutPayload
				? plainPutPayload.call(
						this.native,
						BigInt(input.wallTime),
						input.logical ?? 0,
						input.gid,
						input.type ?? 0,
						input.metaData,
						input.payloadData,
						input.replicas,
						input.roleAgeMs ?? 0,
						integerString(input.now ?? Date.now()),
						input.selfHash ?? "",
						input.selfReplicating ?? true,
						documentIndex.key,
						documentIndex.existingCreated == null
							? ""
							: integerString(documentIndex.existingCreated),
						documentIndex.byteElementIndexLimit ?? 0,
						documentIndex.deleteTrimmedHeads === true,
						projectionPlanId,
						projection.signer,
						input.trimLengthTo,
					)
				: this.native.prepare_plain_committed_no_next_storage_append_document_index_cached_plan_compact_transaction(
						BigInt(input.wallTime),
						input.logical ?? 0,
						input.gid,
						input.type ?? 0,
						input.metaData,
						input.payloadData,
						input.replicas,
						input.roleAgeMs ?? 0,
						integerString(input.now ?? Date.now()),
						input.selfHash ?? "",
						input.selfReplicating ?? true,
						documentIndex.key,
						documentIndex.existingCreated == null
							? ""
							: integerString(documentIndex.existingCreated),
						documentIndex.byteElementIndexLimit ?? 0,
						documentIndex.deleteTrimmedHeads === true,
						projectionPlanId,
						projection.encodedDocument,
						projection.signer,
						input.trimLengthTo,
					);
			return compactCommittedNoNextStorageAppendResultFromRow(
				this.resolution,
				row,
				);
			}
			if (documentIndex.usePlainPutPayload === true) {
				const plainPutPayload =
					this.native
						.prepare_plain_committed_no_next_storage_append_document_index_compact_plain_put_payload_transaction;
				if (plainPutPayload) {
					const row = plainPutPayload.call(
						this.native,
						BigInt(input.wallTime),
						input.logical ?? 0,
						input.gid,
						input.type ?? 0,
						input.metaData,
						input.payloadData,
						input.replicas,
						input.roleAgeMs ?? 0,
						integerString(input.now ?? Date.now()),
						input.selfHash ?? "",
						input.selfReplicating ?? true,
						documentIndex.key,
						documentIndex.existingCreated == null
							? ""
							: integerString(documentIndex.existingCreated),
						documentIndex.byteElementIndexLimit ?? 0,
						documentIndex.deleteTrimmedHeads === true,
						input.trimLengthTo,
					);
					return compactCommittedNoNextStorageAppendResultFromRow(
						this.resolution,
						row,
					);
				}
			}
			const row =
				this.native.prepare_plain_committed_no_next_storage_append_document_index_compact_transaction(
					BigInt(input.wallTime),
				input.logical ?? 0,
				input.gid,
				input.type ?? 0,
				input.metaData,
				input.payloadData,
				input.replicas,
				input.roleAgeMs ?? 0,
				integerString(input.now ?? Date.now()),
				input.selfHash ?? "",
				input.selfReplicating ?? true,
				documentIndex.key,
				documentIndex.valuePrefixBytes ?? EMPTY_UINT8_ARRAY,
				documentIndex.existingCreated == null
					? ""
					: integerString(documentIndex.existingCreated),
				documentIndex.byteElementIndexLimit ?? 0,
				documentIndex.deleteTrimmedHeads === true,
				input.trimLengthTo,
			);
		return compactCommittedNoNextStorageAppendResultFromRow(
			this.resolution,
			row,
		);
	}

	preparePlainCommittedNoNextStorageAppendDocumentIndexCompactBatchTransaction(
		input: NativeBackboneCommittedNoNextDocumentIndexBatchInput,
	): NativeBackboneAppendResult[] | undefined {
		if (input.entries.length === 0) {
			return [];
		}
		const projected = input.entries.every(
			(entry) => entry.documentIndex.projection,
		);
		if (projected) {
			const usePlainPutPayload = input.entries.every(
				(entry) => entry.documentIndex.usePlainPutPayload === true,
			);
			const baseArgs = [
				new BigUint64Array(
					input.entries.map((entry) => BigInt(entry.wallTime)),
				),
				new Uint32Array(
					input.entries.map((entry) => entry.logical ?? 0),
				),
				input.entries.map((entry) => entry.gid),
				input.type ?? 0,
				input.entries.map((entry) => entry.metaData),
				input.entries.map((entry) => entry.payloadData),
				input.replicas,
				input.roleAgeMs ?? 0,
				integerString(input.now ?? Date.now()),
				input.selfHash ?? "",
				input.selfReplicating ?? true,
				input.entries.map((entry) => entry.documentIndex.key),
				input.entries.map((entry) =>
					entry.documentIndex.existingCreated == null
						? ""
						: integerString(entry.documentIndex.existingCreated),
				),
				input.documentByteElementIndexLimit ?? 0,
				input.documentDeleteTrimmedHeads === true,
				new Uint32Array(
					input.entries.map((entry) =>
						this.documentProjectionPlanId(
							entry.documentIndex.projection!.plan,
						),
					),
				),
			] as const;
			const rows = usePlainPutPayload
				? this.native
						.prepare_plain_committed_no_next_storage_append_document_index_cached_plan_compact_plain_put_payload_batch_transaction?.call(
							this.native,
							...baseArgs,
							input.entries.map(
								(entry) =>
									entry.documentIndex.projection!.signer,
							),
							input.trimLengthTo,
						)
				: this.native
						.prepare_plain_committed_no_next_storage_append_document_index_cached_plan_compact_batch_transaction?.call(
							this.native,
							...baseArgs,
							input.entries.map(
								(entry) =>
									entry.documentIndex.projection!
										.encodedDocument,
							),
							input.entries.map(
								(entry) =>
									entry.documentIndex.projection!.signer,
							),
							input.trimLengthTo,
						);
			if (!rows) {
				return undefined;
			}
			return rows.map((row) =>
				compactCommittedNoNextStorageAppendResultFromRow(
					this.resolution,
					row,
				),
			);
		}
			if (
				input.entries.some((entry) => entry.documentIndex.projection) ||
				input.entries.some(
					(entry) => !entry.documentIndex.valuePrefixBytes,
				)
			) {
				return undefined;
			}
			const usePlainPutPayload = input.entries.every(
				(entry) => entry.documentIndex.usePlainPutPayload === true,
			);
			if (usePlainPutPayload) {
				const nativeBatch =
					this.native
						.prepare_plain_committed_no_next_storage_append_document_index_compact_plain_put_payload_batch_transaction;
				if (nativeBatch) {
					const rows = nativeBatch.call(
						this.native,
						new BigUint64Array(
							input.entries.map((entry) => BigInt(entry.wallTime)),
						),
						new Uint32Array(
							input.entries.map((entry) => entry.logical ?? 0),
						),
						input.entries.map((entry) => entry.gid),
						input.type ?? 0,
						input.entries.map((entry) => entry.metaData),
						input.entries.map((entry) => entry.payloadData),
						input.replicas,
						input.roleAgeMs ?? 0,
						integerString(input.now ?? Date.now()),
						input.selfHash ?? "",
						input.selfReplicating ?? true,
						input.entries.map((entry) => entry.documentIndex.key),
						input.entries.map((entry) =>
							entry.documentIndex.existingCreated == null
								? ""
								: integerString(entry.documentIndex.existingCreated),
						),
						input.documentByteElementIndexLimit ?? 0,
						input.documentDeleteTrimmedHeads === true,
						input.trimLengthTo,
					);
					return rows.map((row) =>
						compactCommittedNoNextStorageAppendResultFromRow(
							this.resolution,
							row,
						),
					);
				}
			}
			const nativeBatch =
				this.native
					.prepare_plain_committed_no_next_storage_append_document_index_compact_batch_transaction;
		if (!nativeBatch) {
			return undefined;
		}
		const rows = nativeBatch.call(
			this.native,
			new BigUint64Array(
				input.entries.map((entry) => BigInt(entry.wallTime)),
			),
			new Uint32Array(input.entries.map((entry) => entry.logical ?? 0)),
			input.entries.map((entry) => entry.gid),
			input.type ?? 0,
			input.entries.map((entry) => entry.metaData),
			input.entries.map((entry) => entry.payloadData),
			input.replicas,
			input.roleAgeMs ?? 0,
			integerString(input.now ?? Date.now()),
			input.selfHash ?? "",
			input.selfReplicating ?? true,
			input.entries.map((entry) => entry.documentIndex.key),
			input.entries.map(
				(entry) =>
					entry.documentIndex.valuePrefixBytes ?? EMPTY_UINT8_ARRAY,
			),
			input.entries.map((entry) =>
				entry.documentIndex.existingCreated == null
					? ""
					: integerString(entry.documentIndex.existingCreated),
			),
			input.documentByteElementIndexLimit ?? 0,
			input.documentDeleteTrimmedHeads === true,
			input.trimLengthTo,
		);
		return rows.map((row) =>
			compactCommittedNoNextStorageAppendResultFromRow(
				this.resolution,
				row,
			),
		);
	}

	benchmarkPlainCommittedNoNextStorageAppendTransactionLoop(input: {
		iterations: number;
		wallTimeStart: bigint | number | string;
		payloadData: Uint8Array;
		replicas: number;
		selfHash: string;
		useDocumentIndex?: boolean;
		documentByteElementIndexLimit?: number;
		trimLengthTo?: number;
	}): NativeBackboneLoopBenchmark {
		const row =
			this.native.benchmark_plain_committed_no_next_storage_append_transaction_loop(
				input.iterations,
				BigInt(input.wallTimeStart),
				input.payloadData,
				input.replicas,
				input.selfHash,
				input.useDocumentIndex === true,
				input.documentByteElementIndexLimit ?? 0,
				input.trimLengthTo,
			);
		return {
			totalMs: Number(row[0] ?? 0),
			logLength: Number(row[1] ?? 0),
			blockLength: Number(row[2] ?? 0),
			coordinateLength: Number(row[3] ?? 0),
			documentLength: Number(row[4] ?? 0),
		};
	}
}

export class NativeBackboneMemoryCoordinatePersistenceStore
	implements NativeBackboneCoordinatePersistenceStore
{
	readonly files = new Map<string, Uint8Array>();

	async read(name: string): Promise<Uint8Array | undefined> {
		const file = this.files.get(validateCoordinatePersistenceName(name));
		return file ? copyBytes(file) : undefined;
	}

	async write(name: string, bytes: Uint8Array): Promise<void> {
		this.files.set(validateCoordinatePersistenceName(name), copyBytes(bytes));
	}

	async append(name: string, bytes: Uint8Array): Promise<void> {
		const validName = validateCoordinatePersistenceName(name);
		const existing = this.files.get(validName);
		this.files.set(
			validName,
			existing ? concatBytes([existing, bytes]) : copyBytes(bytes),
		);
	}

	async remove(name: string): Promise<void> {
		this.files.delete(validateCoordinatePersistenceName(name));
	}
}

export class NativeBackboneNodeCoordinatePersistenceStore
	implements NativeBackboneCoordinatePersistenceStore
{
	private readonly appendHandles = new Map<
		string,
		Promise<NativeBackboneNodeAppendFileHandle>
	>();

	constructor(
		private readonly directory: string,
		private readonly fs?: NativeBackboneNodeFs,
	) {}

	private async nodeFs(): Promise<NativeBackboneNodeFs> {
		return this.fs ?? (await import("node:fs/promises"));
	}

	private async filePath(name: string): Promise<string> {
		const { join } = await import("node:path");
		return join(this.directory, validateCoordinatePersistenceName(name));
	}

	private async ensureDirectory(): Promise<NativeBackboneNodeFs> {
		const fs = await this.nodeFs();
		await fs.mkdir(this.directory, { recursive: true });
		return fs;
	}

	private async closeAppendHandle(path: string): Promise<void> {
		const handle = this.appendHandles.get(path);
		if (!handle) {
			return;
		}
		this.appendHandles.delete(path);
		await (await handle).close();
	}

	private async appendHandle(
		fs: NativeBackboneNodeFs,
		path: string,
	): Promise<NativeBackboneNodeAppendFileHandle | undefined> {
		if (!fs.open) {
			return undefined;
		}
		let handle = this.appendHandles.get(path);
		if (!handle) {
			handle = fs.open(path, "a");
			this.appendHandles.set(path, handle);
		}
		return handle;
	}

	async read(name: string): Promise<Uint8Array | undefined> {
		const fs = await this.nodeFs();
		try {
			return await fs.readFile(await this.filePath(name));
		} catch (error) {
			if (isNotFoundError(error)) {
				return undefined;
			}
			throw error;
		}
	}

	async write(name: string, bytes: Uint8Array): Promise<void> {
		const fs = await this.ensureDirectory();
		const path = await this.filePath(name);
		await this.closeAppendHandle(path);
		await fs.writeFile(path, bytes);
	}

	async append(name: string, bytes: Uint8Array): Promise<void> {
		const fs = await this.ensureDirectory();
		const path = await this.filePath(name);
		const handle = await this.appendHandle(fs, path);
		if (handle) {
			await handle.write(bytes);
			return;
		}
		await fs.appendFile(path, bytes);
	}

	async remove(name: string): Promise<void> {
		const fs = await this.nodeFs();
		const path = await this.filePath(name);
		await this.closeAppendHandle(path);
		try {
			await fs.rm(path, { force: true });
		} catch (error) {
			if (!isNotFoundError(error)) {
				throw error;
			}
		}
	}

	async close(): Promise<void> {
		const handles = [...this.appendHandles.values()];
		this.appendHandles.clear();
		await Promise.all(handles.map(async (handle) => (await handle).close()));
	}
}

export class NativeBackboneNodeCoordinatePersistence
	implements NativeBackboneCoordinatePersistenceAdapter
{
	readonly flushOnAppend: boolean;
	readonly flushMaxPendingBytes?: number;
	readonly flushIntervalMs?: number;
	readonly compactMaxJournalBytes?: number;
	readonly compactMaxJournalRecords?: number;
	private readonly snapshotFile: string;
	private readonly journalFile: string;
	private readonly documentSnapshotFile: string;
	private readonly documentJournalFile: string;
	private readonly documentSignerSnapshotFile: string;
	private readonly documentSignerJournalFile: string;
	private readonly fs?: NativeBackboneNodeFs;
	private readonly writeBufferMaxBytes?: number;
	private readonly appendHandles = new Map<
		string,
		Promise<NativeBackboneNodeAppendFileHandle>
	>();
	private readonly journalWriteBuffers = new Map<string, Uint8Array[]>();
	private journalWriteBufferBytes = 0;
	private journalInitialized: boolean | undefined;
	private journalByteLength = 0;
	private journalRecordCount = 0;
	private documentJournalInitialized: boolean | undefined;
	private documentJournalByteLength = 0;
	private documentJournalRecordCount = 0;
	private documentSignerJournalInitialized: boolean | undefined;
	private documentSignerJournalByteLength = 0;
	private documentSignerJournalRecordCount = 0;
	private lastFlushMs = Date.now();

	constructor(
		private readonly directory: string,
		options: NativeBackboneNodeCoordinatePersistenceOptions = {},
	) {
		this.snapshotFile =
			options.snapshot ?? nativeBackboneCoordinatePersistenceFiles.snapshot;
		this.journalFile =
			options.journal ?? nativeBackboneCoordinatePersistenceFiles.journal;
		this.documentSnapshotFile =
			options.documentSnapshot ??
			nativeBackboneCoordinatePersistenceFiles.documentSnapshot;
		this.documentJournalFile =
			options.documentJournal ??
			nativeBackboneCoordinatePersistenceFiles.documentJournal;
		this.documentSignerSnapshotFile =
			options.documentSignerSnapshot ??
			nativeBackboneCoordinatePersistenceFiles.documentSignerSnapshot;
		this.documentSignerJournalFile =
			options.documentSignerJournal ??
			nativeBackboneCoordinatePersistenceFiles.documentSignerJournal;
		this.flushOnAppend = options.flushOnAppend ?? true;
		if (options.flushMaxPendingBytes != null) {
			this.flushMaxPendingBytes = Math.max(0, options.flushMaxPendingBytes);
		} else if (this.flushOnAppend === false) {
			this.flushMaxPendingBytes =
				defaultNativeBackboneCoordinateFlushMaxPendingBytes;
		}
		if (options.flushIntervalMs != null) {
			this.flushIntervalMs = Math.max(0, options.flushIntervalMs);
		}
		if (options.compactMaxJournalBytes != null) {
			this.compactMaxJournalBytes = Math.max(
				0,
				options.compactMaxJournalBytes,
			);
		}
		if (options.compactMaxJournalRecords != null) {
			this.compactMaxJournalRecords = Math.max(
				0,
				options.compactMaxJournalRecords,
			);
		}
		this.fs = options.fs;
		if (options.writeBufferMaxBytes != null) {
			this.writeBufferMaxBytes = Math.max(0, options.writeBufferMaxBytes);
		} else if (this.flushOnAppend === false) {
			this.writeBufferMaxBytes = this.flushMaxPendingBytes;
		}
	}

	private async nodeFs(): Promise<NativeBackboneNodeFs> {
		return this.fs ?? (await import("node:fs/promises"));
	}

	private async filePath(name: string): Promise<string> {
		const { join } = await import("node:path");
		return join(this.directory, validateCoordinatePersistenceName(name));
	}

	private async ensureDirectory(): Promise<NativeBackboneNodeFs> {
		const fs = await this.nodeFs();
		await fs.mkdir(this.directory, { recursive: true });
		return fs;
	}

	private async closeAppendHandle(path: string): Promise<void> {
		const handle = this.appendHandles.get(path);
		if (!handle) {
			return;
		}
		this.appendHandles.delete(path);
		await (await handle).close();
	}

	private async appendHandle(
		fs: NativeBackboneNodeFs,
		path: string,
	): Promise<NativeBackboneNodeAppendFileHandle | undefined> {
		if (!fs.open) {
			return undefined;
		}
		let handle = this.appendHandles.get(path);
		if (!handle) {
			handle = fs.open(path, "a");
			this.appendHandles.set(path, handle);
		}
		return handle;
	}

	private async readFile(name: string): Promise<Uint8Array | undefined> {
		const fs = await this.nodeFs();
		try {
			return await fs.readFile(await this.filePath(name));
		} catch (error) {
			if (isNotFoundError(error)) {
				return undefined;
			}
			throw error;
		}
	}

	private async writeFile(name: string, bytes: Uint8Array): Promise<void> {
		const fs = await this.ensureDirectory();
		const path = await this.filePath(name);
		await this.closeAppendHandle(path);
		await fs.writeFile(path, bytes);
	}

	private async appendFile(name: string, bytes: Uint8Array): Promise<void> {
		if (bytes.byteLength === 0) {
			return;
		}
		const fs = await this.ensureDirectory();
		const path = await this.filePath(name);
		const handle = await this.appendHandle(fs, path);
		if (handle) {
			await handle.write(bytes);
			return;
		}
		await fs.appendFile(path, bytes);
	}

	private async removeFile(name: string): Promise<void> {
		const fs = await this.nodeFs();
		const path = await this.filePath(name);
		await this.closeAppendHandle(path);
		try {
			await fs.rm(path, { force: true });
		} catch (error) {
			if (!isNotFoundError(error)) {
				throw error;
			}
		}
	}

	private async appendJournalBytes(
		fileName: string,
		bytes: Uint8Array,
	): Promise<void> {
		if (this.writeBufferMaxBytes == null) {
			await this.appendFile(fileName, bytes);
			return;
		}
		const chunk = copyBytes(bytes);
		const validName = validateCoordinatePersistenceName(fileName);
		let buffer = this.journalWriteBuffers.get(validName);
		if (!buffer) {
			buffer = [];
			this.journalWriteBuffers.set(validName, buffer);
		}
		buffer.push(chunk);
		this.journalWriteBufferBytes += chunk.byteLength;
		if (this.journalWriteBufferBytes >= this.writeBufferMaxBytes) {
			await this.flushJournalWriteBuffer();
		}
	}

	async flushJournalWriteBuffer(fileName?: string): Promise<void> {
		const fileNames = fileName
			? [validateCoordinatePersistenceName(fileName)]
			: [...this.journalWriteBuffers.keys()];
		for (const name of fileNames) {
			const chunks = this.journalWriteBuffers.get(name);
			if (!chunks || chunks.length === 0) {
				continue;
			}
			this.journalWriteBuffers.delete(name);
			const bytes = chunks.length === 1 ? chunks[0]! : concatBytes(chunks);
			this.journalWriteBufferBytes -= bytes.byteLength;
			await this.appendFile(name, bytes);
		}
	}

	async hydrate(backbone: NativePeerbitBackbone): Promise<number> {
		await this.flushJournalWriteBuffer();
		const [
			snapshot,
			journal,
			documentSnapshot,
			documentJournal,
			documentSignerSnapshot,
			documentSignerJournal,
		] = await Promise.all([
			this.readFile(this.snapshotFile),
			this.readFile(this.journalFile),
			this.readFile(this.documentSnapshotFile),
			this.readFile(this.documentJournalFile),
			this.readFile(this.documentSignerSnapshotFile),
			this.readFile(this.documentSignerJournalFile),
		]);
		const operations = backbone.loadCoordinateSnapshotAndJournal(
			snapshot,
			journal,
		);
		const documentOperations = backbone.loadDocumentSnapshotAndJournal(
			documentSnapshot,
			documentJournal,
		);
		const documentSignerOperations = backbone.loadDocumentSignerSnapshotAndJournal(
			documentSignerSnapshot,
			documentSignerJournal,
		);
		this.journalInitialized = !!journal && journal.byteLength > 0;
		this.journalByteLength = journal?.byteLength ?? 0;
		this.journalRecordCount = operations;
		this.documentJournalInitialized =
			!!documentJournal && documentJournal.byteLength > 0;
		this.documentJournalByteLength = documentJournal?.byteLength ?? 0;
		this.documentJournalRecordCount = documentOperations;
		this.documentSignerJournalInitialized =
			!!documentSignerJournal && documentSignerJournal.byteLength > 0;
		this.documentSignerJournalByteLength =
			documentSignerJournal?.byteLength ?? 0;
		this.documentSignerJournalRecordCount = documentSignerOperations;
		backbone.setCoordinateJournalEnabled(true);
		backbone.setDocumentJournalEnabled(true);
		backbone.setDocumentSignerJournalEnabled(true);
		this.lastFlushMs = Date.now();
		return operations + documentOperations + documentSignerOperations;
	}

	shouldFlushJournalOnAppend(
		backbone: NativePeerbitBackbone,
		now = Date.now(),
	): boolean {
		if (this.flushOnAppend !== false) {
			return true;
		}
		const pendingLength =
			backbone.coordinatePendingJournalLength +
			backbone.documentPendingJournalLength +
			backbone.documentSignerPendingJournalLength;
		if (pendingLength === 0) {
			return false;
		}
		if (
			this.flushMaxPendingBytes != null &&
			backbone.coordinatePendingJournalByteLength +
				backbone.documentPendingJournalByteLength +
				backbone.documentSignerPendingJournalByteLength >=
				this.flushMaxPendingBytes
		) {
			return true;
		}
		return (
			this.flushIntervalMs != null &&
			now - this.lastFlushMs >= this.flushIntervalMs
		);
	}

	flushJournalOnAppend(
		backbone: NativePeerbitBackbone,
	): number | Promise<number> {
		if (!this.shouldFlushJournalOnAppend(backbone)) {
			return 0;
		}
		return this.flushJournal(backbone);
	}

	async flushJournal(backbone: NativePeerbitBackbone): Promise<number> {
		let written = 0;
		const coordinateRecords = backbone.coordinateJournal();
		const coordinateRecordCount = backbone.coordinatePendingJournalLength;
		if (coordinateRecords.byteLength > 0) {
			if (this.journalInitialized === undefined) {
				const existing = await this.readFile(this.journalFile);
				this.journalInitialized = !!existing && existing.byteLength > 0;
			}
			const bytes = this.journalInitialized
				? coordinateRecords
				: concatBytes([backbone.coordinateJournalHeader(), coordinateRecords]);
			await this.appendJournalBytes(this.journalFile, bytes);
			this.journalInitialized = true;
			this.journalByteLength += bytes.byteLength;
			this.journalRecordCount += coordinateRecordCount;
			backbone.clearCoordinateJournal();
			written += coordinateRecords.byteLength;
		}
		const documentRecords = backbone.documentJournal();
		const documentRecordCount = backbone.documentPendingJournalLength;
		if (documentRecords.byteLength > 0) {
			if (this.documentJournalInitialized === undefined) {
				const existing = await this.readFile(this.documentJournalFile);
				this.documentJournalInitialized = !!existing && existing.byteLength > 0;
			}
			const bytes = this.documentJournalInitialized
				? documentRecords
				: concatBytes([backbone.documentJournalHeader(), documentRecords]);
			await this.appendJournalBytes(this.documentJournalFile, bytes);
			this.documentJournalInitialized = true;
			this.documentJournalByteLength += bytes.byteLength;
			this.documentJournalRecordCount += documentRecordCount;
			backbone.clearDocumentJournal();
			written += documentRecords.byteLength;
		}
		const signerRecords = backbone.documentSignerJournal();
		const signerRecordCount = backbone.documentSignerPendingJournalLength;
		if (signerRecords.byteLength > 0) {
			if (this.documentSignerJournalInitialized === undefined) {
				const existing = await this.readFile(this.documentSignerJournalFile);
				this.documentSignerJournalInitialized =
					!!existing && existing.byteLength > 0;
			}
			const bytes = this.documentSignerJournalInitialized
				? signerRecords
				: concatBytes([backbone.documentSignerJournalHeader(), signerRecords]);
			await this.appendJournalBytes(this.documentSignerJournalFile, bytes);
			this.documentSignerJournalInitialized = true;
			this.documentSignerJournalByteLength += bytes.byteLength;
			this.documentSignerJournalRecordCount += signerRecordCount;
			backbone.clearDocumentSignerJournal();
			written += signerRecords.byteLength;
		}
		if (written === 0) {
			this.lastFlushMs = Date.now();
			return 0;
		}
		this.lastFlushMs = Date.now();
		if (this.shouldCompactJournal()) {
			await this.compact(backbone);
		}
		return written;
	}

	private shouldCompactJournal(): boolean {
		return (
			(this.compactMaxJournalBytes != null &&
				this.journalByteLength +
					this.documentJournalByteLength +
					this.documentSignerJournalByteLength >=
					this.compactMaxJournalBytes) ||
			(this.compactMaxJournalRecords != null &&
				this.journalRecordCount +
					this.documentJournalRecordCount +
					this.documentSignerJournalRecordCount >=
					this.compactMaxJournalRecords)
		);
	}

	async compact(backbone: NativePeerbitBackbone): Promise<void> {
		await Promise.all([
			this.writeFile(this.snapshotFile, backbone.coordinateSnapshot()),
			this.writeFile(this.documentSnapshotFile, backbone.documentSnapshot()),
			this.writeFile(
				this.documentSignerSnapshotFile,
				backbone.documentSignerSnapshot(),
			),
		]);
		this.journalWriteBuffers.clear();
		this.journalWriteBufferBytes = 0;
		await Promise.all([
			this.removeFile(this.journalFile),
			this.removeFile(this.documentJournalFile),
			this.removeFile(this.documentSignerJournalFile),
		]);
		this.journalInitialized = false;
		this.journalByteLength = 0;
		this.journalRecordCount = 0;
		this.documentJournalInitialized = false;
		this.documentJournalByteLength = 0;
		this.documentJournalRecordCount = 0;
		this.documentSignerJournalInitialized = false;
		this.documentSignerJournalByteLength = 0;
		this.documentSignerJournalRecordCount = 0;
		backbone.clearCoordinateJournal();
		backbone.clearDocumentJournal();
		backbone.clearDocumentSignerJournal();
		this.lastFlushMs = Date.now();
	}

	async close(): Promise<void> {
		await this.flushJournalWriteBuffer();
		const handles = [...this.appendHandles.values()];
		this.appendHandles.clear();
		await Promise.all(handles.map(async (handle) => (await handle).close()));
	}
}

export class NativeBackboneOPFSCoordinatePersistenceStore
	implements NativeBackboneCoordinatePersistenceStore
{
	constructor(private readonly directory: NativeBackboneOPFSDirectoryHandle) {}

	static async create(options?: {
		root?: NativeBackboneOPFSDirectoryHandle;
		directory?: string | string[];
	}): Promise<NativeBackboneOPFSCoordinatePersistenceStore> {
		const root = options?.root ?? (await this.defaultRoot());
		let directory = root;
		for (const part of this.directoryParts(options?.directory)) {
			directory = await directory.getDirectoryHandle(part, { create: true });
		}
		return new NativeBackboneOPFSCoordinatePersistenceStore(directory);
	}

	private static async defaultRoot(): Promise<NativeBackboneOPFSDirectoryHandle> {
		const storage = (
			globalThis as {
				navigator?: {
					storage?: {
						getDirectory?: () => Promise<NativeBackboneOPFSDirectoryHandle>;
					};
				};
			}
		).navigator?.storage;
		const root = await storage?.getDirectory?.();
		if (!root) {
			throw new Error("OPFS getDirectory is not available in this runtime");
		}
		return root;
	}

	private static directoryParts(directory?: string | string[]): string[] {
		if (!directory) {
			return [];
		}
		const parts = Array.isArray(directory)
			? directory
			: directory.split("/").filter(Boolean);
		return parts.map(validateCoordinatePersistenceName);
	}

	async read(name: string): Promise<Uint8Array | undefined> {
		try {
			const handle = await this.directory.getFileHandle(
				validateCoordinatePersistenceName(name),
				{ create: false },
			);
			const file = await handle.getFile();
			return new Uint8Array(await file.arrayBuffer());
		} catch (error) {
			if (isNotFoundError(error)) {
				return undefined;
			}
			throw error;
		}
	}

	async write(name: string, bytes: Uint8Array): Promise<void> {
		const handle = await this.directory.getFileHandle(
			validateCoordinatePersistenceName(name),
			{ create: true },
		);
		const writable = await handle.createWritable();
		try {
			await writable.write(bytes);
		} finally {
			await writable.close();
		}
	}

	async append(name: string, bytes: Uint8Array): Promise<void> {
		const handle = await this.directory.getFileHandle(
			validateCoordinatePersistenceName(name),
			{ create: true },
		);
		if (handle.createSyncAccessHandle) {
			let access: NativeBackboneOPFSSyncAccessHandle | undefined;
			try {
				access = await handle.createSyncAccessHandle();
			} catch {
				// Main-thread OPFS and some browser contexts do not expose sync handles.
			}
			if (access) {
				try {
					access.write(bytes, { at: access.getSize() });
					access.flush?.();
					return;
				} finally {
					access.close();
				}
			}
		}
		const file = await handle.getFile();
		const writable = await handle.createWritable({ keepExistingData: true });
		try {
			await writable.seek(file.size);
			await writable.write(bytes);
		} finally {
			await writable.close();
		}
	}

	async remove(name: string): Promise<void> {
		try {
			await this.directory.removeEntry(validateCoordinatePersistenceName(name));
		} catch (error) {
			if (!isNotFoundError(error)) {
				throw error;
			}
		}
	}
}

export class NativeBackboneBufferedCoordinatePersistenceStore
	implements NativeBackboneCoordinatePersistenceStore
{
	private readonly buffers = new Map<string, Uint8Array[]>();
	private bufferedBytes = 0;

	constructor(
		private readonly inner: NativeBackboneCoordinatePersistenceStore,
		private readonly options: { maxBufferedBytes?: number } = {},
	) {}

	private buffer(name: string): Uint8Array[] {
		const validName = validateCoordinatePersistenceName(name);
		let buffer = this.buffers.get(validName);
		if (!buffer) {
			buffer = [];
			this.buffers.set(validName, buffer);
		}
		return buffer;
	}

	async read(name: string): Promise<Uint8Array | undefined> {
		await this.flush(name);
		return this.inner.read(name);
	}

	async write(name: string, bytes: Uint8Array): Promise<void> {
		await this.flush(name);
		await this.inner.write(name, bytes);
	}

	async append(name: string, bytes: Uint8Array): Promise<void> {
		const chunk = copyBytes(bytes);
		this.buffer(name).push(chunk);
		this.bufferedBytes += chunk.byteLength;
		if (
			this.options.maxBufferedBytes != null &&
			this.bufferedBytes >= this.options.maxBufferedBytes
		) {
			await this.flush();
		}
	}

	async remove(name: string): Promise<void> {
		await this.flush(name);
		await this.inner.remove?.(name);
	}

	async flush(name?: string): Promise<void> {
		const names = name
			? [validateCoordinatePersistenceName(name)]
			: [...this.buffers.keys()];
		for (const fileName of names) {
			const chunks = this.buffers.get(fileName);
			if (!chunks || chunks.length === 0) {
				continue;
			}
			this.buffers.delete(fileName);
			const bytes = concatBytes(chunks);
			this.bufferedBytes -= bytes.byteLength;
			await this.inner.append(fileName, bytes);
		}
		await this.inner.flush?.();
	}

	async close(): Promise<void> {
		await this.flush();
		await this.inner.close?.();
	}
}

export class NativeBackboneCoordinatePersistence {
	readonly flushOnAppend: boolean;
	readonly flushMaxPendingBytes?: number;
	readonly flushIntervalMs?: number;
	readonly compactMaxJournalBytes?: number;
	readonly compactMaxJournalRecords?: number;
	private readonly snapshotFile: string;
	private readonly journalFile: string;
	private readonly documentSnapshotFile: string;
	private readonly documentJournalFile: string;
	private readonly documentSignerSnapshotFile: string;
	private readonly documentSignerJournalFile: string;
	private journalInitialized: boolean | undefined;
	private journalByteLength = 0;
	private journalRecordCount = 0;
	private documentJournalInitialized: boolean | undefined;
	private documentJournalByteLength = 0;
	private documentJournalRecordCount = 0;
	private documentSignerJournalInitialized: boolean | undefined;
	private documentSignerJournalByteLength = 0;
	private documentSignerJournalRecordCount = 0;
	private lastFlushMs = Date.now();

	constructor(
		private readonly store: NativeBackboneCoordinatePersistenceStore,
		options: NativeBackboneCoordinatePersistenceOptions = {},
	) {
		this.snapshotFile =
			options.snapshot ?? nativeBackboneCoordinatePersistenceFiles.snapshot;
		this.journalFile =
			options.journal ?? nativeBackboneCoordinatePersistenceFiles.journal;
		this.documentSnapshotFile =
			options.documentSnapshot ??
			nativeBackboneCoordinatePersistenceFiles.documentSnapshot;
		this.documentJournalFile =
			options.documentJournal ??
			nativeBackboneCoordinatePersistenceFiles.documentJournal;
		this.documentSignerSnapshotFile =
			options.documentSignerSnapshot ??
			nativeBackboneCoordinatePersistenceFiles.documentSignerSnapshot;
		this.documentSignerJournalFile =
			options.documentSignerJournal ??
			nativeBackboneCoordinatePersistenceFiles.documentSignerJournal;
		this.flushOnAppend = options.flushOnAppend ?? true;
		if (options.flushMaxPendingBytes != null) {
			this.flushMaxPendingBytes = Math.max(0, options.flushMaxPendingBytes);
		} else if (this.flushOnAppend === false) {
			this.flushMaxPendingBytes =
				defaultNativeBackboneCoordinateFlushMaxPendingBytes;
		}
		if (options.flushIntervalMs != null) {
			this.flushIntervalMs = Math.max(0, options.flushIntervalMs);
		}
		if (options.compactMaxJournalBytes != null) {
			this.compactMaxJournalBytes = Math.max(
				0,
				options.compactMaxJournalBytes,
			);
		}
		if (options.compactMaxJournalRecords != null) {
			this.compactMaxJournalRecords = Math.max(
				0,
				options.compactMaxJournalRecords,
			);
		}
	}

	async hydrate(backbone: NativePeerbitBackbone): Promise<number> {
		const [
			snapshot,
			journal,
			documentSnapshot,
			documentJournal,
			documentSignerSnapshot,
			documentSignerJournal,
		] = await Promise.all([
			this.store.read(this.snapshotFile),
			this.store.read(this.journalFile),
			this.store.read(this.documentSnapshotFile),
			this.store.read(this.documentJournalFile),
			this.store.read(this.documentSignerSnapshotFile),
			this.store.read(this.documentSignerJournalFile),
		]);
		const operations = backbone.loadCoordinateSnapshotAndJournal(
			snapshot,
			journal,
		);
		const documentOperations = backbone.loadDocumentSnapshotAndJournal(
			documentSnapshot,
			documentJournal,
		);
		const documentSignerOperations = backbone.loadDocumentSignerSnapshotAndJournal(
			documentSignerSnapshot,
			documentSignerJournal,
		);
		this.journalInitialized = !!journal && journal.byteLength > 0;
		this.journalByteLength = journal?.byteLength ?? 0;
		this.journalRecordCount = operations;
		this.documentJournalInitialized =
			!!documentJournal && documentJournal.byteLength > 0;
		this.documentJournalByteLength = documentJournal?.byteLength ?? 0;
		this.documentJournalRecordCount = documentOperations;
		this.documentSignerJournalInitialized =
			!!documentSignerJournal && documentSignerJournal.byteLength > 0;
		this.documentSignerJournalByteLength =
			documentSignerJournal?.byteLength ?? 0;
		this.documentSignerJournalRecordCount = documentSignerOperations;
		backbone.setCoordinateJournalEnabled(true);
		backbone.setDocumentJournalEnabled(true);
		backbone.setDocumentSignerJournalEnabled(true);
		this.lastFlushMs = Date.now();
		return operations + documentOperations + documentSignerOperations;
	}

	shouldFlushJournalOnAppend(
		backbone: NativePeerbitBackbone,
		now = Date.now(),
	): boolean {
		if (this.flushOnAppend !== false) {
			return true;
		}
		const pendingLength =
			backbone.coordinatePendingJournalLength +
			backbone.documentPendingJournalLength +
			backbone.documentSignerPendingJournalLength;
		if (pendingLength === 0) {
			return false;
		}
		if (
			this.flushMaxPendingBytes != null &&
			backbone.coordinatePendingJournalByteLength +
				backbone.documentPendingJournalByteLength +
				backbone.documentSignerPendingJournalByteLength >=
				this.flushMaxPendingBytes
		) {
			return true;
		}
		return (
			this.flushIntervalMs != null &&
			now - this.lastFlushMs >= this.flushIntervalMs
		);
	}

	flushJournalOnAppend(
		backbone: NativePeerbitBackbone,
	): number | Promise<number> {
		if (!this.shouldFlushJournalOnAppend(backbone)) {
			return 0;
		}
		return this.flushJournal(backbone);
	}

	async flushJournal(backbone: NativePeerbitBackbone): Promise<number> {
		let written = 0;
		const coordinateRecords = backbone.coordinateJournal();
		const coordinateRecordCount = backbone.coordinatePendingJournalLength;
		if (coordinateRecords.byteLength > 0) {
			if (this.journalInitialized === undefined) {
				const existing = await this.store.read(this.journalFile);
				this.journalInitialized = !!existing && existing.byteLength > 0;
			}
			const bytes = this.journalInitialized
				? coordinateRecords
				: concatBytes([backbone.coordinateJournalHeader(), coordinateRecords]);
			await this.store.append(this.journalFile, bytes);
			this.journalInitialized = true;
			this.journalByteLength += bytes.byteLength;
			this.journalRecordCount += coordinateRecordCount;
			backbone.clearCoordinateJournal();
			written += coordinateRecords.byteLength;
		}
		const documentRecords = backbone.documentJournal();
		const documentRecordCount = backbone.documentPendingJournalLength;
		if (documentRecords.byteLength > 0) {
			if (this.documentJournalInitialized === undefined) {
				const existing = await this.store.read(this.documentJournalFile);
				this.documentJournalInitialized = !!existing && existing.byteLength > 0;
			}
			const bytes = this.documentJournalInitialized
				? documentRecords
				: concatBytes([backbone.documentJournalHeader(), documentRecords]);
			await this.store.append(this.documentJournalFile, bytes);
			this.documentJournalInitialized = true;
			this.documentJournalByteLength += bytes.byteLength;
			this.documentJournalRecordCount += documentRecordCount;
			backbone.clearDocumentJournal();
			written += documentRecords.byteLength;
		}
		const signerRecords = backbone.documentSignerJournal();
		const signerRecordCount = backbone.documentSignerPendingJournalLength;
		if (signerRecords.byteLength > 0) {
			if (this.documentSignerJournalInitialized === undefined) {
				const existing = await this.store.read(this.documentSignerJournalFile);
				this.documentSignerJournalInitialized =
					!!existing && existing.byteLength > 0;
			}
			const bytes = this.documentSignerJournalInitialized
				? signerRecords
				: concatBytes([backbone.documentSignerJournalHeader(), signerRecords]);
			await this.store.append(this.documentSignerJournalFile, bytes);
			this.documentSignerJournalInitialized = true;
			this.documentSignerJournalByteLength += bytes.byteLength;
			this.documentSignerJournalRecordCount += signerRecordCount;
			backbone.clearDocumentSignerJournal();
			written += signerRecords.byteLength;
		}
		if (written === 0) {
			this.lastFlushMs = Date.now();
			return 0;
		}
		this.lastFlushMs = Date.now();
		if (this.shouldCompactJournal()) {
			await this.compact(backbone);
		}
		return written;
	}

	private shouldCompactJournal(): boolean {
		return (
			(this.compactMaxJournalBytes != null &&
				this.journalByteLength +
					this.documentJournalByteLength +
					this.documentSignerJournalByteLength >=
					this.compactMaxJournalBytes) ||
			(this.compactMaxJournalRecords != null &&
				this.journalRecordCount +
					this.documentJournalRecordCount +
					this.documentSignerJournalRecordCount >=
					this.compactMaxJournalRecords)
		);
	}

	async compact(backbone: NativePeerbitBackbone): Promise<void> {
		await Promise.all([
			this.store.write(this.snapshotFile, backbone.coordinateSnapshot()),
			this.store.write(this.documentSnapshotFile, backbone.documentSnapshot()),
			this.store.write(
				this.documentSignerSnapshotFile,
				backbone.documentSignerSnapshot(),
			),
		]);
		await Promise.all([
			this.store.remove?.(this.journalFile),
			this.store.remove?.(this.documentJournalFile),
			this.store.remove?.(this.documentSignerJournalFile),
		]);
		this.journalInitialized = false;
		this.journalByteLength = 0;
		this.journalRecordCount = 0;
		this.documentJournalInitialized = false;
		this.documentJournalByteLength = 0;
		this.documentJournalRecordCount = 0;
		this.documentSignerJournalInitialized = false;
		this.documentSignerJournalByteLength = 0;
		this.documentSignerJournalRecordCount = 0;
		backbone.clearCoordinateJournal();
		backbone.clearDocumentJournal();
		backbone.clearDocumentSignerJournal();
	}

	async close(): Promise<void> {
		await this.store.flush?.();
		await this.store.close?.();
	}
}

const isNativeBackboneCoordinatePersistenceAdapter = (
	value: NativeBackboneCoordinatePersistenceConfig,
): value is NativeBackboneCoordinatePersistenceAdapter =>
	typeof (value as NativeBackboneCoordinatePersistenceAdapter).hydrate ===
		"function" &&
	typeof (value as NativeBackboneCoordinatePersistenceAdapter).flushJournal ===
		"function";

export const createNativeBackboneCoordinatePersistence = (
	config: NativeBackboneCoordinatePersistenceConfig,
): NativeBackboneCoordinatePersistenceAdapter => {
	if (isNativeBackboneCoordinatePersistenceAdapter(config)) {
		return config;
	}
	const { store, buffered, ...options } = config;
	const isBuffered = buffered === true || !!buffered;
	const resolvedStore =
		buffered === true
			? new NativeBackboneBufferedCoordinatePersistenceStore(store)
			: buffered
				? new NativeBackboneBufferedCoordinatePersistenceStore(store, buffered)
				: store;
	return new NativeBackboneCoordinatePersistence(resolvedStore, {
		...options,
		compactMaxJournalBytes:
			isBuffered && options.compactMaxJournalBytes == null
				? defaultNativeBackboneCoordinateCompactMaxJournalBytes
				: options.compactMaxJournalBytes,
	});
};

export const createBufferedNativeBackboneCoordinatePersistence = (
	store: NativeBackboneCoordinatePersistenceStore,
	options: NativeBackboneBufferedCoordinatePersistenceOptions = {},
): NativeBackboneCoordinatePersistenceAdapter => {
	const maxBufferedBytes =
		options.maxBufferedBytes ??
		options.flushMaxPendingBytes ??
		defaultNativeBackboneCoordinateFlushMaxPendingBytes;
	const flushMaxPendingBytes =
		options.flushMaxPendingBytes ?? maxBufferedBytes;
	return new NativeBackboneCoordinatePersistence(
		new NativeBackboneBufferedCoordinatePersistenceStore(store, {
			maxBufferedBytes,
		}),
		{
			snapshot: options.snapshot,
			journal: options.journal,
			documentSnapshot: options.documentSnapshot,
			documentJournal: options.documentJournal,
			documentSignerSnapshot: options.documentSignerSnapshot,
			documentSignerJournal: options.documentSignerJournal,
			flushOnAppend: false,
			flushMaxPendingBytes,
			flushIntervalMs: options.flushIntervalMs,
			compactMaxJournalBytes:
				options.compactMaxJournalBytes ??
				defaultNativeBackboneCoordinateCompactMaxJournalBytes,
			compactMaxJournalRecords: options.compactMaxJournalRecords,
		},
	);
};

export const createBufferedNativeBackboneNodeCoordinatePersistence = (
	directory: string,
	options: NativeBackboneNodeCoordinatePersistenceOptions = {},
): NativeBackboneCoordinatePersistenceAdapter => {
	const flushMaxPendingBytes =
		options.flushMaxPendingBytes ??
		defaultNativeBackboneCoordinateFlushMaxPendingBytes;
	return new NativeBackboneNodeCoordinatePersistence(directory, {
		...options,
		flushOnAppend: false,
		flushMaxPendingBytes,
		writeBufferMaxBytes: options.writeBufferMaxBytes ?? flushMaxPendingBytes,
		compactMaxJournalBytes:
			options.compactMaxJournalBytes ??
			defaultNativeBackboneCoordinateCompactMaxJournalBytes,
	});
};

export const createNativePeerbitBackbone = NativePeerbitBackbone.create;
