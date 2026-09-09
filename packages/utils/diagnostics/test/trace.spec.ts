import { expect } from "chai";
import {
	type DiagnosticEvent,
	createDiagnosticTrace,
	diagnosticNow,
} from "../src/index.js";

describe("bounded diagnostic traces", () => {
	it("does not read time or allocate an invocation ID when disabled", () => {
		const first = createDiagnosticTrace(() => {}, "test", "disabled")!;
		const descriptor = Object.getOwnPropertyDescriptor(
			globalThis,
			"performance",
		);
		let clockReads = 0;
		try {
			Object.defineProperty(globalThis, "performance", {
				configurable: true,
				value: {
					now: () => {
						clockReads++;
						return 1;
					},
				},
			});
			expect(createDiagnosticTrace(undefined, "test", "disabled")).equal(
				undefined,
			);
			expect(clockReads).equal(0);
		} finally {
			if (descriptor)
				Object.defineProperty(globalThis, "performance", descriptor);
			else delete (globalThis as { performance?: unknown }).performance;
		}
		const second = createDiagnosticTrace(() => {}, "test", "disabled")!;
		expect(Number(second.id.split(":").at(-1))).equal(
			Number(first.id.split(":").at(-1)) + 1,
		);
		expect(second.now).equal(diagnosticNow);
		first.finish({ name: "done" });
		second.finish({ name: "done" });
	});

	it("caps 256 detail events and reserves exactly one terminal event", () => {
		const events: DiagnosticEvent[] = [];
		const trace = createDiagnosticTrace(
			(event) => events.push(event),
			"test",
			"cap",
		)!;
		for (let i = 0; i < 300; i++) trace.emit({ name: "detail", count: i });
		trace.finish({
			name: "done",
			details: {
				v: 999,
				emittedEvents: 999,
				droppedEvents: 999,
				elapsedMs: -1,
			},
		});
		trace.emit({ name: "too-late" });
		trace.finish({ name: "second-terminal" });
		expect(events).to.have.length(257);
		expect(events.slice(0, 256).map((event) => event.count)).deep.equal(
			Array.from({ length: 256 }, (_, i) => i),
		);
		expect(events[256].name).equal("done");
		expect(events[256].details).include({
			v: 1,
			emittedEvents: 257,
			droppedEvents: 44,
		});
		expect(events[256].details!.elapsedMs).to.be.a("number").and.at.least(0);
		for (const event of events) {
			expect(event.details!.v).equal(1);
			expect(event.details!.elapsedMs).to.be.a("number").and.at.least(0);
		}
	});

	it("snapshots scalar details and leaves caller inputs untouched", () => {
		const events: DiagnosticEvent[] = [];
		const trace = createDiagnosticTrace(
			(event) => events.push(event),
			"default",
			"snapshot",
		)!;
		const details = {
			text: "before",
			count: 1,
			enabled: true,
			absent: undefined as undefined,
		};
		const event = { name: "exact.name", traceId: "caller", details };
		trace.emit(event);
		details.text = "after";
		expect(events[0]).not.equal(event);
		expect(events[0].details).not.equal(details);
		expect(events[0].details).include({
			text: "before",
			count: 1,
			enabled: true,
			absent: undefined,
			v: 1,
		});
		expect(events[0]).include({
			name: "exact.name",
			component: "default",
			traceId: trace.id,
		});
		expect(event.traceId).equal("caller");
		const frozen = Object.freeze({
			name: "frozen",
			component: "explicit",
			details: Object.freeze({ result: "ok" }),
		});
		trace.finish(frozen);
		expect(events[1]).include({
			name: "frozen",
			component: "explicit",
			traceId: trace.id,
		});
		expect(frozen.details).deep.equal({ result: "ok" });
	});

	it("does not copy nonscalar detail references supplied outside the type contract", () => {
		const events: DiagnosticEvent[] = [];
		const trace = createDiagnosticTrace(
			(event) => events.push(event),
			"test",
			"scalar",
		)!;
		trace.emit({
			name: "detail",
			details: { object: {}, array: [], value: "ok" } as any,
		});
		expect(events[0].details).include({ value: "ok", v: 1 });
		expect(events[0].details).not.to.have.property("object");
		expect(events[0].details).not.to.have.property("array");
		trace.finish({ name: "done" });
	});

	it("finishes before invoking its terminal sink, including reentrant calls", () => {
		const events: DiagnosticEvent[] = [];
		const trace = createDiagnosticTrace(
			(event) => {
				events.push(event);
				trace.emit({ name: "reentrant-detail" });
				trace.finish({ name: "reentrant-terminal" });
			},
			"test",
			"terminal",
		)!;
		trace.finish({ name: "done" });
		expect(events).to.have.length(1);
		expect(events[0].details).include({
			emittedEvents: 1,
			droppedEvents: 0,
			v: 1,
		});
	});

	it("contains synchronous sink failures without reopening the terminal", () => {
		let calls = 0;
		const sentinel = new Error("sync diagnostic failure");
		const trace = createDiagnosticTrace(
			() => {
				calls++;
				throw sentinel;
			},
			"test",
			"throw",
		)!;
		expect(() => trace.emit({ name: "detail" })).not.to.throw();
		expect(() => trace.finish({ name: "done" })).not.to.throw();
		trace.emit({ name: "late" });
		trace.finish({ name: "late-done" });
		expect(calls).equal(2);
	});

	it("owns asynchronous sink rejections without awaiting the sink", async () => {
		let calls = 0;
		let release!: () => void;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		const sentinel = new Error("async diagnostic failure");
		const trace = createDiagnosticTrace(
			async () => {
				calls++;
				await gate;
				throw sentinel;
			},
			"test",
			"async",
		)!;
		try {
			expect(trace.emit({ name: "detail" })).equal(undefined);
			expect(trace.finish({ name: "done" })).equal(undefined);
			expect(calls).equal(2);
		} finally {
			release();
			// Yield an event-loop turn so strict unhandled-rejection mode observes
			// a missing catch; do not attach a test-side catch to either sink result.
			await new Promise<void>((resolve) => setTimeout(resolve, 0));
		}
	});
});
