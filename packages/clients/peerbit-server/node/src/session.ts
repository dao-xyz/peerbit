import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
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

const encoder = new TextEncoder();
const decoder = new TextDecoder();

@variant(0)
export class JSONArgs {
	@field({ type: Uint8Array })
	private _args: Uint8Array;

	cache: Record<string, any> | undefined;
	constructor(
		args: Record<
			string,
			string | number | bigint | Uint8Array | boolean | null | undefined
		>,
	) {
		this.cache = args;
		this._args = this.encode(args);
	}
	private encode(obj: any): Uint8Array {
		return new Uint8Array(encoder.encode(JSON.stringify(obj)));
	}

	private decode(bytes: Uint8Array) {
		return (this.cache = JSON.parse(decoder.decode(bytes).toString()));
	}

	get args(): Record<
		string,
		string | number | bigint | Uint8Array | boolean | null | undefined
	> {
		return this.cache || this.decode(this._args);
	}

	get bytes(): Uint8Array {
		return serialize(this);
	}

	static from(bytes: Uint8Array): JSONArgs {
		return deserialize(bytes, JSONArgs);
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

	get(key: string): Promise<Uint8Array | undefined> {
		return this.level.get(key);
	}
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
