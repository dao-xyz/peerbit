import {
	type ConnectionManagerArguments,
	DirectStream,
	type DirectStreamComponents
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
			connectionManager: options.connectionManager || false,
			...options
		});
	}
}
