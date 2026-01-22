import { render, waitFor } from "@testing-library/react";
import React from "react";
import { describe, expect, it } from "vitest";
import { useProgram } from "../src/useProgram.js";

class FakeProgram {
	address: string;
	closed = false;
	allPrograms: any[] = [];
	events = new EventTarget();
	constructor(address: string) {
		this.address = address;
	}
	getTopics() {
		return [];
	}
	async getReady() {
		return new Map();
	}
	async close() {
		this.closed = true;
	}
}

describe("useProgram hook", () => {
	it("opens program via peer context and exposes peers list", async () => {
		const program = new FakeProgram("dummy-address");
		const fakePeer = {
			identity: { publicKey: { hashcode: () => "peer-1" } },
			open: async () => program,
		};

		let latest: ReturnType<typeof useProgram<any>> | undefined;
		const Wrapper = () => {
			latest = useProgram(fakePeer as any, "dummy-address");
			return null;
		};

		render(<Wrapper />);

		await waitFor(() => expect(latest?.program).toBeDefined());
		expect(latest?.program).toEqual(program);
		expect(latest?.peers).toEqual([fakePeer.identity.publicKey]);
		expect(latest?.loading).toBe(false);
		expect(latest?.status).toBe("ready");
		expect(latest?.error).toBeUndefined();
	});

	it("is idle (not loading) when peer is missing", async () => {
		let latest: ReturnType<typeof useProgram<any>> | undefined;
		const Wrapper = () => {
			latest = useProgram(undefined, "dummy-address");
			return null;
		};

		render(<Wrapper />);

		await waitFor(() => expect(latest?.status).toBe("idle"));
		expect(latest?.loading).toBe(false);
		expect(latest?.program).toBeUndefined();
	});

	it("reports error when open fails", async () => {
		const fakePeer = {
			identity: { publicKey: { hashcode: () => "peer-1" } },
			open: async () => {
				throw new Error("open failed");
			},
		};

		let latest: ReturnType<typeof useProgram<any>> | undefined;
		const Wrapper = () => {
			latest = useProgram(fakePeer as any, "dummy-address");
			return null;
		};

		render(<Wrapper />);

		await waitFor(() => expect(latest?.status).toBe("error"));
		expect(latest?.loading).toBe(false);
		expect(latest?.program).toBeUndefined();
		expect(latest?.error?.message).toBe("open failed");
	});
});
