import { field, option, variant, vec } from "@dao-xyz/borsh";
import { randomBytes } from "@peerbit/crypto";
import { ClosedError, Program } from "../src/program.js";

@variant(0)
export class Log {}

@variant("x1")
export class P1 extends Program {
	@field({ type: Uint8Array })
	id: Uint8Array;

	constructor() {
		super();
		this.id = randomBytes(32);
	}

	async open(): Promise<void> {}
}

export class EmbeddedStore {
	@field({ type: Log })
	log: Log;

	constructor(properties?: { log: Log }) {
		if (properties) {
			this.log = properties.log;
		}
	}
}
export class ExtendedEmbeddedStore extends EmbeddedStore {
	constructor(properties?: { log: Log }) {
		super(properties);
	}
}
@variant("p2")
export class P2 extends Program {
	@field({ type: option(vec(ExtendedEmbeddedStore)) })
	log?: ExtendedEmbeddedStore[];

	@field({ type: P1 })
	child: P1;

	constructor(log: Log) {
		super();
		this.log = [new ExtendedEmbeddedStore({ log: log })];
		this.child = new P1();
	}

	async open(): Promise<void> {
		await this.child.open();
	}
}

@variant("p3")
export class P3 extends Program {
	@field({ type: Uint8Array })
	id: Uint8Array;

	constructor() {
		super();
		this.id = randomBytes(32);
	}
	static TOPIC = "abc";

	async open(): Promise<void> {
		this.node.services.pubsub.subscribe(P3.TOPIC);
	}

	async close(from?: Program | undefined): Promise<boolean> {
		await this.node.services.pubsub.unsubscribe(P3.TOPIC);
		return super.close(from);
	}

	async setup(): Promise<void> {}

	getTopics(): string[] {
		return [P3.TOPIC];
	}
}

@variant("p4")
export class P4 extends Program {
	@field({ type: P2 })
	child: P2;

	constructor() {
		super();
		this.child = new P2(new Log());
	}

	async open(): Promise<void> {
		await this.child.open();
	}
}

@variant("test-program-with-topics")
export class TestProgramWithTopics extends Program {
	openInvoked = false;
	async open(): Promise<void> {
		this.openInvoked = true;
		await this.node.services.pubsub.subscribe("a");
		await this.node.services.pubsub.subscribe("b");
	}
	async close(from?: Program): Promise<boolean> {
		await this.node.services.pubsub.unsubscribe("a");
		await this.node.services.pubsub.unsubscribe("b");
		return super.close(from);
	}

	getTopics(): string[] {
		if (this.closed) {
			throw new ClosedError("Program is closed");
		}
		return ["a", "b"];
	}
}

@variant("test-shared_nested")
export class TestNestedProgram extends Program {
	openInvoked = false;
	async open(): Promise<void> {
		this.openInvoked = true;
	}

	getTopics(): string[] {
		if (this.closed) {
			throw new ClosedError("Program is closed");
		}
		return ["test-shared_nested"];
	}
}

@variant("test-shared")
export class TestProgram extends Program<{ dontOpenNested?: boolean }> {
	@field({ type: "u32" })
	id: number;

	@field({ type: TestNestedProgram })
	nested: TestNestedProgram;

	constructor(
		id: number = 0,
		nested: TestNestedProgram = new TestNestedProgram(),
	) {
		super();
		this.id = id;
		this.nested = nested;
	}

	async open(args?: { dontOpenNested?: boolean }): Promise<void> {
		if (args?.dontOpenNested) {
			this.nested.closed = true;
			return;
		}
		return this.nested.open();
	}
}

@variant("test-shared-loose-parent-ref")
export class TestParenteRefernceProgram extends Program {
	@field({ type: "u32" })
	id: number;

	@field({ type: TestNestedProgram })
	nested: TestNestedProgram;

	constructor(
		id: number = 0,
		nested: TestNestedProgram = new TestNestedProgram(),
	) {
		super();
		this.id = id;
		this.nested = nested;
	}

	async open(): Promise<void> {
		await this.node.open(this.nested, { parent: this as Program });
	}
}
