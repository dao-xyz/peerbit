import { field, variant } from "@dao-xyz/borsh";
import * as indexerTypes from "@peerbit/indexer-interface";
import { BORSH_ENCODING } from "@peerbit/log";

@variant(0)
export class Operation /* <T> */ {}

export const BORSH_ENCODING_OPERATION = BORSH_ENCODING(Operation);

// @deprecated
@variant(0)
export class PutWithKeyOperation extends Operation {
	@field({ type: "string" })
	key: string;

	@field({ type: Uint8Array })
	data: Uint8Array;

	constructor(props: { key: string; data: Uint8Array }) {
		super();
		this.key = props.key;
		this.data = props.data;
	}
}

// @deprecated
/* @variant(1)
export class PutAllOperation<T> extends Operation<T> {
    @field({ type: vec(PutOperation) })
    docs: PutOperation<T>[];

    constructor(props?: { docs: PutOperation<T>[] }) {
        super();
        if (props) {
            this.docs = props.docs;
        }
    }
}
 */

// @deprecated
@variant(2)
export class DeleteByStringKeyOperation extends Operation {
	@field({ type: "string" })
	key: string;

	constructor(props: { key: string }) {
		super();
		this.key = props.key;
	}

	toDeleteOperation(): DeleteOperation {
		return new DeleteOperation({ key: indexerTypes.toId(this.key) });
	}
}

export const coerceDeleteOperation = (
	operation: DeleteOperation | DeleteByStringKeyOperation,
): DeleteOperation => {
	return operation instanceof DeleteByStringKeyOperation
		? operation.toDeleteOperation()
		: operation;
};

@variant(3)
export class PutOperation extends Operation {
	@field({ type: Uint8Array })
	data: Uint8Array;

	constructor(props: { data: Uint8Array }) {
		super();
		this.data = props.data;
	}
}

export const isPutOperation = (
	operation: Operation,
): operation is PutOperation | PutWithKeyOperation => {
	return (
		operation instanceof PutOperation ||
		operation instanceof PutWithKeyOperation
	);
};

/**
 * Delete a document at a key
 */
@variant(4)
export class DeleteOperation extends Operation {
	@field({ type: indexerTypes.IdKey })
	key: indexerTypes.IdKey;

	constructor(props: { key: indexerTypes.IdKey }) {
		super();
		this.key = props.key;
	}
}

export const isDeleteOperation = (
	operation: Operation,
): operation is DeleteOperation | DeleteByStringKeyOperation => {
	return (
		operation instanceof DeleteOperation ||
		operation instanceof DeleteByStringKeyOperation
	);
};
