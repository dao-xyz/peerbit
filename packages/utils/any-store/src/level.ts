import { AbstractLevel } from "abstract-level";
import { AnyStore, MaybePromise } from "./interface.js";
import { MemoryLevel } from "memory-level";
import { ClassicLevel } from "classic-level";

const isNotFoundError = (err) =>
	err.toString().indexOf("NotFoundError: Key not found in database") === -1 &&
	err.toString().indexOf("NotFound") === -1;

export class LevelStore implements AnyStore {
	constructor(readonly store: AbstractLevel<any, any, any>) {}

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
		if (!this.store)
			return Promise.reject(new Error("No cache store found to open"));
		if (this.status() !== "open") {
			await this.store.open();
			return Promise.resolve();
		} else {
			await this.store.open({ passive: true });
		}
	}

	async get(key: string): Promise<Uint8Array | undefined> {
		return new Promise<Uint8Array | undefined>((resolve, reject) => {
			this.store.get(key, (err, result) => {
				if (err) {
					// Ignore error if key was not found
					if (isNotFoundError(err)) {
						return reject(err);
					}
					resolve(undefined);
				}
				resolve(result);
			});
		});
	}

	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		const iterator = this.store.iterator<any, Uint8Array>({
			valueEncoding: "view"
		});
		for await (const [key, value] of iterator) {
			yield [key, value];
		}
	}

	async clear(): Promise<void> {
		await this.store.clear();
	}

	async put(key: string, value: Uint8Array) {
		// Remove when https://github.com/Level/classic-level/issues/87 is fixed
		/* if (this.store instanceof ClassicLevel) {
			await this.store.del(key, { sync: true });
		} */

		return this.store.put(key, value, { valueEncoding: "view" });
	}

	// Remove a value and key from the cache
	async del(key: string) {
		if (this.store.status !== "open") {
			throw new Error("Cache store not open: " + this.store.status);
		}

		return new Promise<void>((resolve, reject) => {
			this.store.del(key, (err) => {
				if (err) {
					// Ignore error if key was not found
					if (isNotFoundError(err)) {
						return reject(err);
					}
				}
				resolve();
			});
		});
	}

	async size(): Promise<number> {
		let size = 0;
		if (this.store instanceof ClassicLevel) {
			const e = this.store.keys({
					limit: 1,
					fillCache: !1
				}),
				a = await e.next();
			await e.close();
			const t = this.store.keys({
					limit: 1,
					reverse: !0,
					fillCache: !1
				}),
				s = await t.next();
			return (
				await t.close(),
				this.store.approximateSize(a, s + "\uffff", {
					keyEncoding: "utf8"
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
}
