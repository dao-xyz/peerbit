import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { expect } from "chai";
import { copySerialization, registerIndexFieldAccessor } from "../src/borsh.js";

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

	it("preserves serialized field accessors on the wrapped index type", () => {
		const empty = new Uint8Array();

		@variant(0)
		class LegacyIndexedClass {
			@field({ type: "string" })
			id: string;

			@field({ type: Uint8Array })
			content: Uint8Array;

			constructor(id: string, content: Uint8Array) {
				this.id = id;
				this.content = content;
			}
		}

		@variant(0)
		class CompactIndexedClass {
			@field({ type: "string" })
			id: string;

			get content(): Uint8Array {
				return empty;
			}

			set content(content: Uint8Array) {
				void content;
			}

			constructor(id: string) {
				this.id = id;
			}
		}
		registerIndexFieldAccessor(CompactIndexedClass, "content", {
			type: Uint8Array,
		});
		registerIndexFieldAccessor(CompactIndexedClass, "content", {
			type: Uint8Array,
		});

		class Context {
			@field({ type: "string" })
			context: string;

			constructor(context: string) {
				this.context = context;
			}
		}

		class LegacyWithContext {
			@field({ type: Context })
			__context: Context;

			constructor(value: LegacyIndexedClass, context: Context) {
				Object.assign(this, value);
				this.__context = context;
			}
		}

		class CompactWithContext {
			@field({ type: Context })
			__context: Context;

			constructor(value: CompactIndexedClass, context: Context) {
				Object.assign(this, value);
				this.__context = context;
			}
		}

		copySerialization(LegacyIndexedClass, LegacyWithContext);
		copySerialization(CompactIndexedClass, CompactWithContext);
		copySerialization(CompactIndexedClass, CompactWithContext);

		const legacyBytes = serialize(
			new LegacyWithContext(
				new LegacyIndexedClass("chunk", Uint8Array.from([1, 2, 3])),
				new Context("head"),
			),
		);
		const migrated = deserialize(
			legacyBytes,
			CompactWithContext,
		) as CompactWithContext & CompactIndexedClass;

		expect(migrated.id).to.equal("chunk");
		expect(migrated.content).to.equal(empty);
		expect(migrated).not.to.have.own.property("content");
		expect(migrated.__context.context).to.equal("head");

		const compact = new CompactWithContext(
			new CompactIndexedClass("next"),
			new Context("next-head"),
		);
		const roundTrip = deserialize(
			serialize(compact),
			CompactWithContext,
		) as CompactWithContext & CompactIndexedClass;

		expect(roundTrip.id).to.equal("next");
		expect(roundTrip.content).to.equal(empty);
		expect(roundTrip).not.to.have.own.property("content");
		expect(roundTrip.__context.context).to.equal("next-head");
	});

	it("does not preserve an unregistered accessor", () => {
		class Unregistered {
			get content(): Uint8Array {
				return new Uint8Array();
			}

			set content(content: Uint8Array) {
				void content;
			}
		}
		(field({ type: Uint8Array }) as unknown as PropertyDecorator)(
			Unregistered.prototype,
			"content",
		);
		(variant(0) as unknown as ClassDecorator)(Unregistered);

		class Wrapped {
			@field({ type: "string" })
			context: string;
		}
		copySerialization(Unregistered, Wrapped);

		expect(
			Object.getOwnPropertyDescriptor(Wrapped.prototype, "content"),
		).to.equal(undefined);
	});

	it("rejects inherited and already-decorated accessors", () => {
		@variant(0)
		class DecoratedBase {
			@field({ type: "string" })
			id: string;
		}

		class Derived extends DecoratedBase {
			get content(): Uint8Array {
				return new Uint8Array();
			}

			set content(content: Uint8Array) {
				void content;
			}
		}
		expect(() =>
			registerIndexFieldAccessor(Derived, "content", {
				type: Uint8Array,
			}),
		).to.throw("cannot inherit from a Borsh-decorated class");

		class UndecoratedMiddle extends DecoratedBase {}
		class Grandchild extends UndecoratedMiddle {
			get content(): Uint8Array {
				return new Uint8Array();
			}

			set content(content: Uint8Array) {
				void content;
			}
		}
		expect(() =>
			registerIndexFieldAccessor(Grandchild, "content", {
				type: Uint8Array,
			}),
		).to.throw("cannot inherit from a Borsh-decorated class");

		class AlreadyDecorated {
			get content(): Uint8Array {
				return new Uint8Array();
			}

			set content(content: Uint8Array) {
				void content;
			}
		}
		(field({ type: Uint8Array }) as unknown as PropertyDecorator)(
			AlreadyDecorated.prototype,
			"content",
		);
		expect(() =>
			registerIndexFieldAccessor(AlreadyDecorated, "content", {
				type: Uint8Array,
			}),
		).to.throw("already registered as a Borsh field");
	});

	it("requires a two-sided accessor defined on the registered prototype", () => {
		class GetterOnly {
			get content(): Uint8Array {
				return new Uint8Array();
			}
		}

		class SetterOnly {
			set content(content: Uint8Array) {
				void content;
			}
		}

		expect(() =>
			registerIndexFieldAccessor(GetterOnly, "content", {
				type: Uint8Array,
			}),
		).to.throw("must define both a getter and setter");
		expect(() =>
			registerIndexFieldAccessor(SetterOnly, "content", {
				type: Uint8Array,
			}),
		).to.throw("must define both a getter and setter");
		expect(() =>
			registerIndexFieldAccessor(class Missing {}, "content", {
				type: Uint8Array,
			}),
		).to.throw("must define both a getter and setter");
	});

	it("rejects wrapper field and prototype collisions", () => {
		class IndexedClass {
			get content(): Uint8Array {
				return new Uint8Array();
			}

			set content(content: Uint8Array) {
				void content;
			}
		}
		registerIndexFieldAccessor(IndexedClass, "content", {
			type: Uint8Array,
		});

		class SchemaCollision {
			@field({ type: Uint8Array })
			content: Uint8Array;
		}
		expect(() => copySerialization(IndexedClass, SchemaCollision)).to.throw(
			"wrapper schema already contains that field",
		);

		class PrototypeCollision {
			@field({ type: "string" })
			context: string;

			get content(): Uint8Array {
				return new Uint8Array();
			}
		}
		expect(() => copySerialization(IndexedClass, PrototypeCollision)).to.throw(
			"wrapper prototype already defines it",
		);

		Reflect.deleteProperty(PrototypeCollision.prototype, "content");
		copySerialization(IndexedClass, PrototypeCollision);
		expect(
			Object.getOwnPropertyDescriptor(PrototypeCollision.prototype, "content")
				?.get,
		).to.equal(
			Object.getOwnPropertyDescriptor(IndexedClass.prototype, "content")?.get,
		);
	});
});
