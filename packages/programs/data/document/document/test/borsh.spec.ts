import { deserialize, field, serialize } from "@dao-xyz/borsh";
import { expect } from "chai";
import { copySerialization } from "../src/borsh";

describe("borsh", () => {
	it("can append fields to an already defined class", () => {
		class IndexedClass {
			@field({ type: "string" })
			id: string;

			@field({ type: "string" })
			name: string;

			constructor(value: IndexedClass) {
				Object.assign(this, value);
			}
		}

		class Context {
			@field({ type: "string" })
			context: string;

			constructor(value: Context) {
				Object.assign(this, value);
			}
		}
		class IndexedClassWithContext<I> {
			@field({ type: Context })
			__context: Context;

			constructor(value: I, context: Context) {
				Object.assign(this, value);
				this.__context = context;
			}
		}

		copySerialization(IndexedClass, IndexedClassWithContext);
		copySerialization(IndexedClass, IndexedClassWithContext); // invoke multiple times to assert that it is idempotent
		copySerialization(IndexedClass, IndexedClassWithContext); // invoke multiple times to assert that it is idempotent

		const obj = new IndexedClassWithContext(
			{ id: "1", name: "2" },
			{ context: "3" },
		);
		const indexedClass = new IndexedClassWithContext(
			obj,
			new Context({ context: "3" }),
		);
		const ser = serialize(indexedClass);
		const der = deserialize(ser, IndexedClassWithContext);
		expect(der).to.deep.equal(indexedClass);
	});
});
