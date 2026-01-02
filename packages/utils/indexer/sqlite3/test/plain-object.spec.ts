import { field, variant, vec } from "@dao-xyz/borsh";
import { id, toId } from "@peerbit/indexer-interface";
import { expect } from "chai";
import { SQLLiteIndex } from "../src/engine.js";
import { create } from "../src/index.js";
import { setup } from "./utils.js";

describe("plain object inputs", () => {
	let index: Awaited<ReturnType<typeof setup<any>>>;

	afterEach(async () => {
		await index.store.stop();
	});

	@variant("SubDoc")
	class SubDoc {
		@field({ type: "string" })
		value: string;

		constructor(props: SubDoc) {
			this.value = props.value;
		}
	}

	@variant("RootDocInline")
	class RootDocInline {
		@id({ type: "string" })
		id: string;

		@field({ type: SubDoc })
		sub: SubDoc;

		constructor(props: RootDocInline) {
			this.id = props.id;
			// Accept plain objects for nested values
			this.sub = props.sub as any;
		}
	}

	it("inlines nested object from plain object", async () => {
		index = await setup({ schema: RootDocInline }, create);
		const store = index.store as SQLLiteIndex<RootDocInline>;

		await store.put(
			new RootDocInline({
				id: "1",
				sub: { value: "hello" } as any,
			}),
		);

		const fetched = await store.get(toId("1"));
		expect(fetched!.value).to.be.instanceOf(RootDocInline);
		expect(fetched!.value.sub).to.be.instanceOf(SubDoc);
		expect(fetched!.value.sub.value).to.equal("hello");
	});

	@variant("RootDocArray")
	class RootDocArray {
		@id({ type: "string" })
		id: string;

		@field({ type: vec(SubDoc) })
		subs: SubDoc[];

		constructor(props: RootDocArray) {
			this.id = props.id;
			// Accept plain objects for array elements
			this.subs = props.subs as any;
		}
	}

	it("stores vec(nested) elements from plain objects", async () => {
		index = await setup({ schema: RootDocArray }, create);
		const store = index.store as SQLLiteIndex<RootDocArray>;

		await store.put(
			new RootDocArray({
				id: "1",
				subs: [{ value: "a" }, { value: "b" }] as any,
			}),
		);

		const fetched = await store.get(toId("1"));
		expect(fetched!.value).to.be.instanceOf(RootDocArray);
		expect(fetched!.value.subs).to.have.length(2);
		expect(fetched!.value.subs[0]).to.be.instanceOf(SubDoc);
		expect(fetched!.value.subs.map((x: SubDoc) => x.value)).to.deep.equal([
			"a",
			"b",
		]);
	});

	abstract class PolyBase {}

	@variant("PolyA")
	class PolyA extends PolyBase {
		@field({ type: "string" })
		value: string;

		constructor(props: PolyA) {
			super();
			this.value = props.value;
		}
	}

	@variant("PolyB")
	class PolyB extends PolyBase {
		@field({ type: "string" })
		value: string;

		constructor(props: PolyB) {
			super();
			this.value = props.value;
		}
	}

	@variant("RootDocPoly")
	class RootDocPoly {
		@id({ type: "string" })
		id: string;

		@field({ type: PolyBase })
		sub: PolyBase;

		constructor(props: RootDocPoly) {
			this.id = props.id;
			// Allow plain objects to hit the polymorphic resolution path
			this.sub = props.sub as any;
		}
	}

	it("throws for polymorphic nested POJO values", async () => {
		index = await setup({ schema: RootDocPoly }, create);
		const store = index.store as SQLLiteIndex<RootDocPoly>;

		let error: unknown;
		try {
			await store.put(
				new RootDocPoly({
					id: "1",
					sub: { value: "x" } as any,
				}),
			);
		} catch (e) {
			error = e;
		}

		expect(error).to.be.instanceOf(Error);
		expect((error as Error).message).to.include(
			'Ambiguous polymorphic nested value for field "sub"',
		);
	});
});
