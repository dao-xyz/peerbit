export class NoPeersError extends Error {
	constructor(topic: string) {
		super(
			`No peers found for topic ${topic}. Please make sure you are connected to the network and try again.`,
		);
	}
}
