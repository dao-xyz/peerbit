import { expect } from "chai";
import {
	DeliveryError,
	InvalidMessageError,
	NotStartedError,
} from "@peerbit/stream-interface";
import { dontThrowIfDeliveryError } from "../src/index.js";

describe("stream errors", () => {
	it("uses stable error names", () => {
		expect(new DeliveryError("failed").name).to.equal("DeliveryError");
		expect(new InvalidMessageError("invalid").name).to.equal(
			"InvalidMessageError",
		);
		expect(new NotStartedError().name).to.equal("NotStartedError");
	});

	it("recognizes delivery errors from other module identities", () => {
		const foreignDeliveryError = new Error("delivery failed");
		foreignDeliveryError.name = "DeliveryError";

		expect(() => dontThrowIfDeliveryError(foreignDeliveryError)).to.not.throw();
		expect(() => dontThrowIfDeliveryError(new DeliveryError("local"))).to.not.throw();
		expect(() => dontThrowIfDeliveryError(new Error("boom"))).to.throw("boom");
	});
});
