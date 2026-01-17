import { field, variant } from "@dao-xyz/borsh";

export const TEST_DOC_TYPE = "test_message";

export const makeTestId = (seed: string, marker: number): Uint8Array => {
	const id = new Uint8Array(32);
	id[0] = marker;
	const bytes = new TextEncoder().encode(seed);
	for (let i = 0; i < bytes.length; i++) {
		id[1 + (i % 31)] ^= bytes[i]!;
	}
	return id;
};

export const TEST_DOC_ID = makeTestId("documents-default", 1);
export const COUNTER_ID = makeTestId("counter-default", 3);

@variant(TEST_DOC_TYPE)
export class TestMessage {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	author: string;

	@field({ type: "string" })
	text: string;

	@field({ type: "u64" })
	timestamp: bigint;

	constructor(props?: {
		id?: string;
		author: string;
		text: string;
		timestamp: bigint;
	}) {
		this.id = props?.id ?? "";
		this.author = props?.author ?? "";
		this.text = props?.text ?? "";
		this.timestamp = props?.timestamp ?? 0n;
	}
}
