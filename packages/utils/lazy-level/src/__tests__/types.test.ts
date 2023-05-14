import { SimpleLevel } from "..";
import { MemoryLevel } from "memory-level";

describe("SimpleLevel", () =>
	it("level is SimpleLevel", async () => {
		const level: SimpleLevel = new MemoryLevel<string, Uint8Array>();
		await level.close();
	}));
