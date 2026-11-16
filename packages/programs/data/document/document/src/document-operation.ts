import {
	type DeleteByStringKeyOperation,
	type DeleteOperation,
	type Operation,
	type PutOperation,
	type PutWithKeyOperation,
	isDeleteOperation,
	isPutOperation,
} from "./operation.js";
import { ProofDeleteOperation, ProofPutOperation } from "./proof-operations.js";

/** Payloads returned by Documents; the store's encoding determines the format. */
export type DocumentOperation =
	| Operation
	| ProofPutOperation
	| ProofDeleteOperation;

type Inspection =
	| {
			format: "legacy";
			type: "put";
			operation: PutOperation | PutWithKeyOperation;
	  }
	| {
			format: "legacy";
			type: "delete";
			operation: DeleteOperation | DeleteByStringKeyOperation;
	  }
	| {
			format: "owner-proofs-v1";
			type: "put";
			operation: ProofPutOperation;
	  }
	| {
			format: "owner-proofs-v1";
			type: "delete";
			operation: ProofDeleteOperation;
	  };

/**
 * Inspect an already-decoded payload without changing it. This does not verify
 * signatures, scope, evidence or admission, and returns the original object.
 * Decode entry payloads using their store's encoding, not the legacy codec.
 */
export const inspectDocumentOperation = (
	value: unknown,
): Inspection | undefined => {
	if (
		value instanceof ProofPutOperation &&
		value.constructor === ProofPutOperation
	)
		return { format: "owner-proofs-v1", type: "put", operation: value };
	if (
		value instanceof ProofDeleteOperation &&
		value.constructor === ProofDeleteOperation
	)
		return { format: "owner-proofs-v1", type: "delete", operation: value };
	const operation = value as Operation;
	if (isPutOperation(operation))
		return { format: "legacy", type: "put", operation };
	if (isDeleteOperation(operation))
		return { format: "legacy", type: "delete", operation };
	return undefined;
};
