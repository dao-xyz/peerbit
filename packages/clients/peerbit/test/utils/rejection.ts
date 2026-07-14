import { expect } from "chai";

export const expectRejectedWith = async (
	promise: Promise<unknown>,
	expected: string | RegExp,
) => {
	try {
		await promise;
	} catch (error) {
		const message =
			typeof error === "object" && error !== null && "message" in error
				? String((error as { message: unknown }).message)
				: String(error);
		if (typeof expected === "string") {
			expect(message).to.contain(expected);
		} else {
			expect(message).to.match(expected);
		}
		return;
	}
	throw new Error("Expected promise to reject");
};
