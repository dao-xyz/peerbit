import { field, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { SharedLog } from "@peerbit/shared-log";

@variant("LogToReplicate")
export class LogToReplicate extends Program {
	@field({ type: SharedLog })
	log!: SharedLog;

	constructor() {
		super();
		this.log = new SharedLog({ id: new Uint8Array(32) });
	}

	async open(_args?: any): Promise<void> {
		await this.log.open();
	}
}
