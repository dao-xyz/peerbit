import { serialize } from "@dao-xyz/borsh";
import { expect } from "chai";
import {
	DataMessage,
	MessageHeader,
	SilentDelivery,
} from "@peerbit/stream-interface";
import { Uint8ArrayList } from "uint8arraylist";

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

const toByteArray = (bytes: Uint8Array | Uint8ArrayList) =>
	bytes instanceof Uint8Array ? bytes : bytes.subarray();

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

		expect(Array.from(toByteArray(message.bytes()))).to.deep.equal(
			Array.from(serialize(message)),
		);

		message.header.mode.to = ["peer-z"];

		expect(Array.from(toByteArray(message.bytes()))).to.deep.equal(
			Array.from(serialize(message)),
		);
	});

	it("keeps the data buffer segmented when serializing a data-message", () => {
		const payload = new Uint8Array([7, 8, 9]);
		const message = new DataMessage({
			header: new MessageHeader({
				session: 1,
				mode: new SilentDelivery({
					to: ["peer-a"],
					redundancy: 1,
				}),
			}),
			data: payload,
		});

		const bytes = message.bytes();
		expect(bytes).to.be.instanceOf(Uint8ArrayList);
		expect([...((bytes as Uint8ArrayList) as Iterable<Uint8Array>)].at(-1)).to.equal(
			payload,
		);
		expect(Array.from(toByteArray(bytes))).to.deep.equal(
			Array.from(serialize(message)),
		);
	});

	it("decodes segmented payloads lazily from serialized data-messages", () => {
		const payload = new Uint8Array([10, 11, 12, 13]);
		const encoded = new DataMessage({
			header: new MessageHeader({
				session: 1,
				mode: new SilentDelivery({
					to: ["peer-a"],
					redundancy: 1,
				}),
			}),
			data: payload,
		}).bytes() as Uint8ArrayList;

		const decoded = DataMessage.from(encoded);
		const decodedAny = decoded as any;

		expect(decoded.hasData).to.equal(true);
		expect(decodedAny._data).to.equal(undefined);
		expect(decodedAny._dataBytes).to.be.instanceOf(Uint8ArrayList);
		expect(Array.from(toByteArray(decoded.bytes()))).to.deep.equal(
			Array.from(toByteArray(encoded)),
		);
		expect(Array.from(decoded.data!)).to.deep.equal(Array.from(payload));
	});
});
