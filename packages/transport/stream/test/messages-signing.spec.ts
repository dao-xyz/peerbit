import { serialize } from "@dao-xyz/borsh";
import { expect } from "chai";
import {
	DataMessage,
	MessageHeader,
	SilentDelivery,
} from "@peerbit/stream-interface";

const serializeUnsignedMessage = (message: DataMessage) => {
	const mode = message.header.mode;
	message.header.mode = undefined as any;
	const signatures = message.header.signatures;
	message.header.signatures = undefined;
	const bytes = serialize(message);
	message.header.signatures = signatures;
	message.header.mode = mode;
	return bytes;
};

describe("message signing", () => {
	it("caches signable data-message bytes independently of routing changes", () => {
		const message = new DataMessage({
			header: new MessageHeader({
				session: 1,
				mode: new SilentDelivery({
					to: ["peer-a"],
					redundancy: 1,
				}),
			}),
			data: new Uint8Array([1, 2, 3]),
		});

		expect(Array.from(message.getSignableBytes())).to.deep.equal(
			Array.from(serializeUnsignedMessage(message)),
		);

		const cached = message.getSignableBytes();
		message.header.mode.to = ["peer-b", "peer-c"];

		expect(message.getSignableBytes()).to.equal(cached);
		expect(Array.from(message.getSignableBytes())).to.deep.equal(
			Array.from(serializeUnsignedMessage(message)),
		);
	});

	it("serializes data-message bytes canonically after routing changes", () => {
		const message = new DataMessage({
			header: new MessageHeader({
				session: 1,
				mode: new SilentDelivery({
					to: ["peer-a"],
					redundancy: 2,
				}),
			}),
			data: new Uint8Array([4, 5, 6]),
		});

		expect(Array.from(message.bytes())).to.deep.equal(
			Array.from(serialize(message)),
		);

		message.header.mode.to = ["peer-z"];

		expect(Array.from(message.bytes())).to.deep.equal(
			Array.from(serialize(message)),
		);
	});
});
