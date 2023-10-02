import { AbstractLevel } from "abstract-level";
import { AnyStore } from "./interface.js";

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
		}
	}

	async get(key: string): Promise<Uint8Array | undefined> {
		if (this.store.status !== "open") {
			throw new Error("Cache store not open: " + this.store.status);
		}
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

	async sublevel(name: string) {
		return new LevelStore(this.store.sublevel(name, { valueEncoding: "view" }));
	}
}
