import { expect } from "chai";
import { getRetiredAWSManagementError } from "../src/remotes.js";

describe("legacy remote origins", () => {
	it("gives actionable cleanup details for an AWS origin", () => {
		const error = getRetiredAWSManagementError({
			type: "aws",
			instanceId: "i-0123456789",
			region: "eu-north-1",
		});

		expect(error.message).to.include("i-0123456789");
		expect(error.message).to.include("eu-north-1");
		expect(error.message).to.include("AWS console");
	});
});
