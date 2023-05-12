import { JSON_ENCODING } from "@dao-xyz/peerbit-log";
import { Log } from "@dao-xyz/peerbit-log";
import { EncryptionTemplateMaybeEncrypted } from "@dao-xyz/peerbit-log";
import { variant, field } from "@dao-xyz/borsh";
import { Program } from "@dao-xyz/peerbit-program";
import { Operation } from "@dao-xyz/peerbit-document";

export class KeyValueIndex {
	_index: any;
	_log: Log<any>;
	constructor() {
		this._index = {};
	}

	get(key: string) {
		return this._index[key];
	}

	setup(log: Log<any>) {
		this._log = log;
	}

	async updateIndex() {
		const values = await this._log.values.toArray();
		const handled: { [key: string]: boolean } = {};
		for (let i = values.length - 1; i >= 0; i--) {
			const item = values[i];
			if (handled[item.payload.getValue().key]) {
				continue;
			}
			handled[item.payload.getValue().key] = true;
			if (item.payload.getValue().op === "PUT") {
				this._index[item.payload.getValue().key] =
					item.payload.getValue().value;
				continue;
			}
			if (item.payload.getValue().op === "DEL") {
				delete this._index[item.payload.getValue().key];
				continue;
			}
		}
	}
}

const encoding = JSON_ENCODING;

@variant("kvstore")
export class KeyBlocks<T> extends Program {
	_index: KeyValueIndex;

	@field({ type: Log })
	log: Log<Operation<T>>;

	constructor(properties: { id: Uint8Array }) {
		super(properties);
		this.log = new Log();
	}
	async setup() {
		this._index = new KeyValueIndex();

		this.log.setup({
			onChange: this._index.updateIndex.bind(this._index),
			encoding,
			canAppend: () => Promise.resolve(true),
		});
	}

	get all() {
		return this._index._index;
	}

	get(key: string) {
		return this._index.get(key);
	}

	set(
		key: string,
		data: any,
		options?: {
			pin?: boolean;
			reciever?: EncryptionTemplateMaybeEncrypted;
		}
	) {
		return this.put(key, data, options);
	}

	put(
		key: string,
		data: any,
		options?: {
			pin?: boolean;
			reciever?: EncryptionTemplateMaybeEncrypted;
		}
	) {
		return this.log.append(
			{
				op: "PUT",
				key: key,
				value: data,
			},
			{ ...options }
		);
	}

	del(
		key: string,
		options?: {
			pin?: boolean;
			reciever?: EncryptionTemplateMaybeEncrypted;
		}
	) {
		return this.log.append(
			{
				op: "DEL",
				key: key,
				value: undefined,
			},
			{ ...options }
		);
	}
}
