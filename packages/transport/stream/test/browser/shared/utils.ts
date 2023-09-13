import {
	ConnectionManagerOptions,
	DirectStream,
	DirectStreamComponents
} from "@peerbit/stream";

export class TestDirectStream extends DirectStream {
	constructor(
		components: DirectStreamComponents,
		options: {
			id?: string;
			pingInterval?: number | null;
			connectionManager?: ConnectionManagerOptions;
		} = {}
	) {
		super(components, [options.id || "/browser-test/0.0.0"], {
			canRelayMessage: true,
			emitSelf: false,
			connectionManager: options.connectionManager || {
				autoDial: false
			},
			...options
		});
	}
}
