import { field, fixedArray, variant } from "@dao-xyz/borsh";
import { method, service } from "@dao-xyz/borsh-rpc";

@variant("counter_open")
export class OpenCounterRequest {
	@field({ type: fixedArray("u8", 32) })
	id: Uint8Array;

	constructor(properties?: { id?: Uint8Array }) {
		this.id = properties?.id ?? new Uint8Array(32);
	}
}

@service()
export class CounterService {
	private _impl:
		| {
				get: () => Promise<bigint>;
				increment: (amount: bigint) => Promise<bigint>;
				close: () => Promise<void>;
		  }
		| undefined;

	constructor(impl?: {
		get: () => Promise<bigint>;
		increment: (amount: bigint) => Promise<bigint>;
		close: () => Promise<void>;
	}) {
		this._impl = impl;
	}

	@method({ returns: "u64" })
	async get(): Promise<bigint> {
		if (!this._impl) throw new Error("CounterService not bound");
		return this._impl.get();
	}

	@method({ args: "u64", returns: "u64" })
	async increment(amount: bigint): Promise<bigint> {
		if (!this._impl) throw new Error("CounterService not bound");
		return this._impl.increment(amount);
	}

	@method({ returns: "void" })
	async close(): Promise<void> {
		if (!this._impl) throw new Error("CounterService not bound");
		return this._impl.close();
	}
}
