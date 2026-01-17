import type { AnyStore } from "@peerbit/any-store-interface";
import { CanonicalClient } from "@peerbit/canonical-client";
import { CanonicalHost } from "@peerbit/canonical-host";
import { expect } from "chai";
import { openAnyStore } from "../src/client.js";
import { createAnyStoreModule } from "../src/host.js";

class MemoryStore implements AnyStore {
	private data = new Map<string, Uint8Array>();
	private sublevels = new Map<string, MemoryStore>();
	private opened = false;

	status() {
		return this.opened ? "open" : "closed";
	}

	open() {
		this.opened = true;
	}

	close() {
		this.opened = false;
	}

	get(key: string) {
		return this.data.get(key);
	}

	put(key: string, value: Uint8Array) {
		this.data.set(key, value);
	}

	del(key: string) {
		this.data.delete(key);
	}

	sublevel(name: string) {
		let sub = this.sublevels.get(name);
		if (!sub) {
			sub = new MemoryStore();
			if (this.opened) sub.open();
			this.sublevels.set(name, sub);
		}
		return sub;
	}

	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		for (const entry of this.data.entries()) {
			yield entry;
		}
	}

	clear() {
		this.data.clear();
	}

	size() {
		let size = 0;
		for (const value of this.data.values()) {
			size += value.byteLength;
		}
		return size;
	}

	persisted() {
		return false;
	}
}

describe("@peerbit/any-store-proxy", () => {
	it("proxies AnyStore over canonical modules", async () => {
		const store = new MemoryStore();
		const host = new CanonicalHost({
			peer: async () => {
				throw new Error("not used");
			},
			peerId: async () => "peer-id",
		});
		host.registerModule(
			createAnyStoreModule({
				createStore: async () => store,
			}),
		);

		const control = new MessageChannel();
		host.attachControlPort(control.port1);

		const client = new CanonicalClient(control.port2);
		const proxy = await openAnyStore({ client });

		await proxy.open();
		await proxy.put("a", new Uint8Array([1]));
		const got = await proxy.get("a");
		expect(got).to.deep.equal(new Uint8Array([1]));

		const sub = await proxy.sublevel("sub");
		await sub.put("b", new Uint8Array([2]));
		const subGot = await sub.get("b");
		expect(subGot).to.deep.equal(new Uint8Array([2]));

		const keys: string[] = [];
		for await (const [key] of proxy.iterator()) {
			keys.push(key);
		}
		expect(keys).to.deep.equal(["a"]);

		await proxy.del("a");
		expect(await proxy.get("a")).to.equal(undefined);

		expect(await proxy.size()).to.equal(0);
		expect(await proxy.persisted()).to.equal(false);

		await proxy.close();
		proxy.closePort();
	});
});
