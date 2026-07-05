import {
	DirectStream,
	type DirectStreamComponents,
	type DirectStreamOptions,
} from "@peerbit/stream";

export class TestDirectStream extends DirectStream {
	constructor(
		components: DirectStreamComponents,
		options: {
			id?: string;
		} & DirectStreamOptions = {},
	) {
		super(components, [options.id || "/browser-test/0.0.0"], {
			canRelayMessage: true,
			connectionManager: options.connectionManager || false,
			...options,
		});
	}
}
