import * as connection from "../connection.js";
import { CustomEvent } from "@libp2p/interface/events";
import type { TypedEventTarget } from "@libp2p/interface/events";

export class EventEmitterNode extends connection.MessageNode {
	constructor(
		readonly eventEmitter: TypedEventTarget<{
			hello: CustomEvent<connection.Hello>;
			data: CustomEvent<connection.DataMessage>;
		}>
	) {
		super({
			addEventListener: <K extends keyof connection.EventMessages>(
				k: K,
				fn
			) => {
				this.eventEmitter.addEventListener(k, (ev) => {
					fn(ev.detail as connection.EventMessages[K]);
				});
			},
			dispatchEvent: (msg) =>
				this.eventEmitter.dispatchEvent(
					new CustomEvent(msg.type, { detail: msg })
				)
		});
	}
}
