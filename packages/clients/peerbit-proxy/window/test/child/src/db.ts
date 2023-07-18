import { variant, field } from "@dao-xyz/borsh";
import { SharedLog } from "@peerbit/shared-log";
import { Program } from "@peerbit/program";

@variant("test-log")
export class TestLog extends Program {
	@field({ type: SharedLog })
	log: SharedLog<Uint8Array>;

	constructor() {
		super();
		this.log = new SharedLog({ id: new Uint8Array(32) });
	}

	async open(args?: any): Promise<void> {
		return this.log.open(args);
	}
}
