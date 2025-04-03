import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { debouncedAccumulatorMap } from "../src/debounce.js";

describe("debounceAcculmulatorMap", () => {
	it("will wait for asynchronus function to finish before calling again", async () => {
		let invokedSize: number[] = [];
		const debounce = debouncedAccumulatorMap(async (map) => {
			await delay(1000);
			invokedSize.push(map.size);
		}, 100);

		debounce.add({ key: "1", value: 1 });
		await delay(200);
		debounce.add({ key: "2", value: 2 });
		debounce.add({ key: "3", value: 3 });

		await waitForResolved(() => expect(invokedSize).to.deep.eq([1, 2]));
	});

	it("will aggregate while waiting for asynchronus function to finish", async () => {
		let invokedSize: number[] = [];
		const debounce = debouncedAccumulatorMap(async (map) => {
			await delay(1000);
			invokedSize.push(map.size);
		}, 100);

		debounce.add({ key: "1", value: 1 });
		await delay(200);
		debounce.add({ key: "2", value: 2 });
		await delay(200);
		debounce.add({ key: "3", value: 3 });

		await waitForResolved(() => expect(invokedSize).to.deep.eq([1, 2]));

		debounce.add({ key: "4", value: 4 });

		await waitForResolved(() => expect(invokedSize).to.deep.eq([1, 2, 1]));
	});
});
