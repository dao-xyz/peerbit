import { expect } from "chai";
import {
	type DiagnosticEvent,
	diagnosticStart,
	emitDiagnosticDuration,
} from "../src/index.js";

describe("diagnostics", () => {
	it("does not take a timestamp when disabled", () => {
		expect(diagnosticStart(undefined)).to.equal(0);
	});

	it("emits duration events", () => {
		const events: DiagnosticEvent[] = [];
		const startedAt = diagnosticStart((event) => events.push(event));

		emitDiagnosticDuration((event) => events.push(event), startedAt, {
			name: "test.phase",
			count: 1,
		});

		expect(events).to.have.length(1);
		expect(events[0].name).to.equal("test.phase");
		expect(events[0].count).to.equal(1);
		expect(events[0].durationMs).to.be.a("number");
	});
});
