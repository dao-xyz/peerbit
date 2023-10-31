import {
	ConnectionManagerArguments,
	DirectStream,
	DirectStreamComponents
} from "@peerbit/stream";

export class TestDirectStream extends DirectStream {
	constructor(
		components: DirectStreamComponents,
		options: {
			id?: string;
			connectionManager?: ConnectionManagerArguments;
		} = {}
	) {
		super(components, [options.id || "/browser-test/0.0.0"], {
			canRelayMessage: true,
			emitSelf: false,
			connectionManager: options.connectionManager || false,
			...options
		});
	}
}
