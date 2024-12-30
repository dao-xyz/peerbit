import { field, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { SharedLog } from "@peerbit/shared-log";

@variant("test-log")
export class TestLog extends Program {
	@field({ type: SharedLog })
	log: SharedLog<Uint8Array, any>;

	constructor() {
		super();
		this.log = new SharedLog({ id: new Uint8Array(32) });
	}

	async open(args?: any): Promise<void> {
		return this.log.open(args);
	}
}
