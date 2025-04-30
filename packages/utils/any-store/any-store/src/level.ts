import { type AnyStore } from "@peerbit/any-store-interface";
import { type AbstractLevel } from "abstract-level";
import { ClassicLevel } from "classic-level";

type GetFn = (
	key: string,
) => Uint8Array | undefined | Promise<Uint8Array | undefined>;
const getOrGetSync = (level: AbstractLevel<any, any, any>): GetFn => {
	const canGetSync = (level.supports as any)["getSync"] !== false;
	if (!canGetSync) {
		return (key: string): Promise<Uint8Array | undefined> => {
			return level.get(key);
		};
	} else {
		return (key: string): Uint8Array | undefined => {
			return level.getSync(key);
		};
	}
};

export class LevelStore implements AnyStore {
	private getFn: GetFn;
	constructor(readonly store: AbstractLevel<any, any, any>) {
		this.getFn = getOrGetSync(store);
	}

	status() {
		return this.store.status;
	}

	async close() {
		if (!this.store) {
			return Promise.reject(new Error("No cache store found to close"));
		}

		if (this.status() !== "closed" && this.status() !== "closing") {
			await this.store.close();
			return Promise.resolve();
		}
	}

	async open() {
		if (!this.store) {
			return Promise.reject(new Error("No cache store found to open"));
		}
		if (this.status() !== "open") {
			await this.store.open();
			return Promise.resolve();
		} else {
			await this.store.open({ passive: true });
		}
	}

	get(key: string) {
		return this.getFn(key);
	}

	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		const iterator = this.store.iterator<any, Uint8Array>({
			valueEncoding: "view",
		});
		for await (const [key, value] of iterator) {
			yield [key, value];
		}
	}

	async clear(): Promise<void> {
		await this.store.clear();
	}

	async put(key: string, value: Uint8Array) {
		return this.store.put(key, value, {
			valueEncoding: "view",
			sync: true,
		} as any); // sync option to make sure read after write behaves correctly
	}

	// Remove a value and key from the cache
	async del(key: string) {
		if (this.store.status !== "open") {
			throw new Error("Cache store not open: " + this.store.status);
		}

		await this.store.del(key);
	}

	async size(): Promise<number> {
		let size = 0;
		if (this.store instanceof ClassicLevel) {
			const e = this.store.keys({
					limit: 1,
					fillCache: !1,
				}),
				a = await e.next();
			await e.close();
			const t = this.store.keys({
					limit: 1,
					reverse: !0,
					fillCache: !1,
				}),
				s = await t.next();
			return (
				await t.close(),
				this.store.approximateSize(a, s + "\uffff", {
					keyEncoding: "utf8",
				})
			);
		} else {
			for await (const v of this.iterator()) {
				size += v[1].length;
			}
		}
		return size;
	}

	async sublevel(name: string) {
		return new LevelStore(this.store.sublevel(name, { valueEncoding: "view" }));
	}

	persisted() {
		return this.store instanceof ClassicLevel;
	}
}
