import type { AbstractLevel } from "abstract-level";

export class Session {
	programs: KV;
	imports: KV;
	constructor(
		readonly level: AbstractLevel<
			string | Buffer | Uint8Array,
			string,
			Uint8Array
		>,
	) {
		this.imports = new KV(
			this.level.sublevel<string, Uint8Array>("imports", {
				keyEncoding: "utf8",
				valueEncoding: "view",
			}),
		);
		this.programs = new KV(
			this.level.sublevel<string, Uint8Array>("programs", {
				keyEncoding: "utf8",
				valueEncoding: "view",
			}),
		);
	}

	async clear() {
		await this.imports.clear();
		await this.programs.clear();
	}
}

export class KV {
	constructor(
		readonly level: AbstractLevel<
			string | Buffer | Uint8Array,
			string,
			Uint8Array
		>,
	) {}

	add(key: string, arg: Uint8Array) {
		return this.level.put(key, arg);
	}

	remove(key: string) {
		return this.level.del(key);
	}

	async all(): Promise<[string, Uint8Array][]> {
		const res: [string, Uint8Array][] = [];
		for await (const [key, value] of this.level.iterator()) {
			res.push([key, value]);
		}
		return res;
	}

	async open() {
		await this.level.open();
	}

	async close() {
		await this.level.close();
	}

	async clear() {
		await this.level.clear();
	}
}
