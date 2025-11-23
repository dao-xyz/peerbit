import { expect, it, describe } from "vitest";
import React from "react";
import { render, waitFor } from "@testing-library/react";
import { PeerContext } from "@peerbit/react";
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

		const ctx = {
			type: "node" as const,
			peer: fakePeer as any,
			promise: undefined,
			loading: false,
			status: "connected" as const,
			persisted: false,
			tabIndex: 0,
			error: undefined,
		};

		let latest: ReturnType<typeof useProgram<any>> | undefined;
		const Wrapper = () => {
			latest = useProgram("dummy-address");
			return null;
		};

		render(
			<PeerContext.Provider value={ctx as any}>
				<Wrapper />
			</PeerContext.Provider>,
		);

		await waitFor(() => expect(latest?.program).toBeDefined());
		expect(latest?.program).toEqual(program);
		expect(latest?.peers).toEqual([fakePeer.identity.publicKey]);
		expect(latest?.loading).toBe(false);
	});
});
