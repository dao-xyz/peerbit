import { field, variant } from "@dao-xyz/borsh";
import { delay } from "@peerbit/time";
import { expect } from "chai";
import {
	ClosedError,
	MissingAddressError,
	Program,
	type ProgramClient,
	getProgramFromVariant,
} from "../src/program.js";
import { getValuesWithType } from "../src/utils.js";
import { EmbeddedStore, Log, P2, P3, P4, TestProgram } from "./samples.js";
import { creatMockPeer } from "./utils.js";

describe("getValuesWithType", () => {
	it("can stop at type", () => {
		const log = new Log();
		const p = new P2(log);

		let stores = getValuesWithType(p, Log, EmbeddedStore);
		expect(stores).to.be.empty;
		stores = getValuesWithType(p, Log);
		expect(stores).to.deep.equal([log]);
	});
});

describe("program", () => {
	describe("lifecycle", () => {
		let peer: ProgramClient;
		beforeEach(async () => {
			peer = await creatMockPeer();
		});
		afterEach(async () => {
			await peer.stop();
		});

		describe("open/close", () => {
			it("with args", async () => {
				@variant("p5")
				class P5 extends Program<{ number: number }> {
					number: number | undefined;
					async open(args?: { number: number } | undefined): Promise<void> {
						this.number = args?.number;
					}
				}

				const p5 = await peer.open(new P5(), { args: { number: 123 } });
				expect(p5.number).equal(123);
			});

			it("fails to init without schema", async () => {
				class NoVariant extends Program {
					async open(): Promise<void> {}
				}
				await expect(peer.open(new NoVariant())).rejectedWith(
					'Expecting class to be decorated with a string variant. Example:\n\'import { variant } "@dao-xyz/borsh"\n@variant("example-db")\nclass NoVariant { ...',
				);
			});

			it("fails to init without variant", async () => {
				class NoVariant extends Program {
					@field({ type: "u8" })
					number: number;

					constructor() {
						super();
						this.number = 123;
					}
					async open(): Promise<void> {}
				}

				await expect(peer.open(new NoVariant())).rejectedWith(
					'Expecting class to be decorated with a string variant. Example:\n\'import { variant } "@dao-xyz/borsh"\n@variant("example-db")\nclass NoVariant { ...',
				);

				// Remove NoVariant for globals to prevent sideeffects
				const idx = (Program.prototype as any)[1000].findIndex(
					(x: any) => x === NoVariant,
				);
				((Program.prototype as any)[1000] as Array<() => void>).splice(idx, 1);
			});

			it("static", async () => {
				const p2 = new P2(new Log());
				const cid = await p2.save(peer.services.blocks);
				const p2Loaded = await P2.open(cid, peer);
				expect(p2Loaded).to.be.instanceOf(P2);
			});

			it("can re-open from dropped", async () => {
				const p = new P3();
				await peer.open(p);
				expect(p.closed).to.be.false;
				await p.drop();
				expect(p.closed).to.be.true;
				await peer.open(p);
				expect(p.closed).to.be.false;
			});

			it("reject when dropping after close", async () => {
				const p = new P3();
				await peer.open(p);
				expect(p.closed).to.be.false;
				await p.close();
				expect(p.closed).to.be.true;
				await expect(p.drop()).rejectedWith(ClosedError);
			});

			it("does not close closed", async () => {
				const instance = new TestProgram();
				let openEvents = 0;
				instance.events.addEventListener("open", () => {
					openEvents++;
				});
				instance.nested.events.addEventListener("open", () => {
					openEvents++;
				});

				await peer.open(instance, { args: { dontOpenNested: true } });
				expect(instance.closed).to.be.false;
				expect(instance.nested.closed).to.be.true;
				expect(openEvents).to.equal(1);

				let subProgramCloseCalls = 0;
				const subProgramClose = instance.nested.close.bind(instance.nested);
				instance.nested.close = async () => {
					subProgramCloseCalls++;
					return subProgramClose();
				};
				expect(instance.closed).to.be.false;
				await instance.close();
				expect(instance.closed).to.be.true;
				expect(subProgramCloseCalls).to.equal(0);
			});

			it("throwing trying to dropping closed sub-program ", async () => {
				const instance = new TestProgram();
				await peer.open(instance, { args: { dontOpenNested: true } });
				expect(instance.closed).to.be.false;
				expect(instance.nested.closed).to.be.true;
				await expect(instance.drop()).rejectedWith(ClosedError);
			});

			it("can re-open from closed", async () => {
				const p = new P4();

				let closeEvents: Program[] = [];
				let subCloseEvents: Program[] = [];

				p.events.addEventListener("close", (ev) => {
					closeEvents.push(ev.detail);
				});

				p.child.events.addEventListener("close", (ev) => {
					subCloseEvents.push(ev.detail);
				});

				await peer.open(p);

				expect(p.closed).to.be.false;
				await p.close();

				await delay(3000);
				expect(closeEvents).to.deep.equal([p, p.child, p.child.child]);
				expect(subCloseEvents).to.deep.equal([p.child, p.child.child]);
				expect(p.closed).to.be.true;

				closeEvents = [];
				subCloseEvents = [];
				await peer.open(p);
				expect(p.closed).to.be.false;
				await p.close();

				expect(closeEvents).to.deep.equal([p, p.child, p.child.child]);
				expect(subCloseEvents).to.deep.equal([p.child, p.child.child]);
				expect(p.closed).to.be.true;
			});

			it("can resolve programs", () => {
				const log = new Log();
				const p = new P2(log);

				// programs
				const programs = p.programs;
				expect(programs).to.have.length(1);
				expect(programs[0]).equal(p.child);
			});

			it("open/closes children", async () => {
				let p = new P4();

				await peer.open(p);
				expect(p.closed).to.be.false;

				expect(p.child.closed).to.be.false;
				await p.close();
				expect(p.closed).to.be.true;
				expect(p.child.closed).to.be.true;
			});
		});

		describe("clear", () => {
			it("clears stores and programs on clear", async () => {
				const log = new Log();
				const p = new P2(log);
				p.allPrograms;
				expect(p["_allPrograms"]).to.exist;
				await p["_clear"]();
				expect(p["_allPrograms"]).equal(undefined);
			});

			it("invokes clear on close", async () => {
				const log = new Log();
				const p = new P2(log);

				await peer.open(p);

				expect(p.closed).to.be.false;
				let cleared = false;
				p["_clear"] = () => {
					cleared = true;
				};
				await p.close();
				expect(cleared).to.be.true;
			});

			it("does not invoke clear on close non-initialized", async () => {
				const log = new Log();
				const p = new P2(log);
				expect(p.closed).to.be.true;
				let cleared = false;
				p["_clear"] = () => {
					cleared = true;
				};
				await p.close();
				expect(cleared).to.be.false;
			});

			it("invokes clear on drop", async () => {
				const log = new Log();
				const p = new P2(log);

				await peer.open(p);
				expect(p.closed).to.be.false;
				let cleared = false;
				p["_clear"] = () => {
					cleared = true;
				};
				await p.drop();
				expect(p.closed).to.be.true;
				expect(cleared).to.be.true;
			});
		});

		describe("subprogram", () => {
			it("rootAddress", async () => {
				const p = new P4();
				try {
					p.child.child.rootAddress;
					throw new Error("Should not be able to access rootAddress");
				} catch (error) {
					expect(error).to.be.instanceOf(MissingAddressError);
				}
				await peer.open(p);
				expect(p.rootAddress).equal(p.address);
				expect(p.child.rootAddress).equal(p.address);
				expect(p.child.child.rootAddress).equal(p.address);
			});

			it("subprogram will not close if opened outside a program", async () => {
				const p = new P3();
				const p2 = new P3();

				await peer.open(p);
				await peer.open(p2);

				let p3 = await peer.open(new P3(), { parent: p });
				let closeCounter = 0;

				// Open it outside a program (call init on p3)
				await peer.open(p3);

				expect(p.children).to.have.length(1);
				expect(p3.children).equal(undefined);
				expect(p3.parents).to.have.members([undefined, p]);

				await peer.open(p3, { parent: p2 });
				expect(p.children).to.have.length(1);
				expect(p2.children).to.have.length(1);
				expect(p3.children).equal(undefined);
				expect(p3.parents).to.have.members([undefined, p, p2]);

				await p2.close();
				expect(p3.parents).to.have.members([undefined, p]);
				expect(closeCounter).equal(0);
				expect(p3.closed).to.be.false;

				await p.close();
				expect(p3.parents).to.have.members([undefined]);
				expect(closeCounter).equal(0);
				expect(p3.closed).to.be.false;
			});

			it("subprogram will close if no dependency", async () => {
				const p = new P3();
				const p2 = new P3();

				const closeEvents1: Map<string, number> = new Map();

				p.events.addEventListener("close", (p) => {
					closeEvents1.set(
						p.detail.address,
						(closeEvents1.get(p.detail.address) || 0) + 1,
					);
				});

				const closeEvents2: Map<string, number> = new Map();

				p2.events.addEventListener("close", (p) => {
					closeEvents2.set(
						p.detail.address,
						(closeEvents2.get(p.detail.address) || 0) + 1,
					);
				});

				await peer.open(p);

				await peer.open(p2);

				let p3 = await peer.open(new P3(), { parent: p });
				expect(p3.parents).to.deep.equal([p]);

				const closeEvents3: Map<string, number> = new Map();
				p3.events.addEventListener("close", (p) => {
					closeEvents3.set(
						p.detail.address,
						(closeEvents3.get(p.detail.address) || 0) + 1,
					);
				});

				expect(p.children).to.have.length(1);
				expect(p3.children).equal(undefined);
				expect(p3.parents).to.have.members([p]);
				expect(p3.closed).to.be.false;

				await peer.open(p3, { parent: p2 });
				expect(p.children).to.have.length(1);
				expect(p2.children).to.have.length(1);
				expect(p3.children).equal(undefined);
				expect(p3.parents).to.have.members([p, p2]);
				expect(p3.closed).to.be.false;

				await p2.close();
				expect(p3.parents).to.have.members([p]);
				expect(p3.closed).to.be.false;
				expect(closeEvents1.size).equal(0);
				expect([...closeEvents2.keys()]).to.deep.equal([p2.address]);
				expect(closeEvents3.size).equal(0);

				await p.close();
				expect(p3.parents).to.have.members([]);
				expect(closeEvents3.size).equal(1);
				expect(p3.closed).to.be.true;
				expect([...closeEvents1.keys()]).to.deep.equal([p.address, p3.address]);
				expect([...closeEvents2.keys()]).to.deep.equal([p2.address]);
				expect([...closeEvents3.keys()]).to.deep.equal([p3.address]);
			});

			it("can drop", async () => {
				let p = new P4();
				await peer.open(p);
				const closeEvents: Program[] = [];
				const dropEvents: Program[] = [];
				expect(p.closed).to.be.false;
				expect(p.child.closed).to.be.false;
				expect(p.child.child.closed).to.be.false;
				p.events.addEventListener("close", (ev) => {
					closeEvents.push(ev.detail);
				});
				p.events.addEventListener("drop", (ev) => {
					dropEvents.push(ev.detail);
				});

				await p.drop();
				expect(p.closed).to.be.true;
				expect(p.child.closed).to.be.true;
				expect(p.child.child.closed).to.be.true;
				expect(closeEvents).to.be.empty;
				expect(dropEvents).to.deep.equal([p, p.child, p.child.child]);
			});
		});
	});

	describe("getProgram", () => {
		@variant("test_get_a")
		class A extends Program {
			async open(args?: any): Promise<void> {}
		}

		@variant("test_get_b")
		class B extends Program {
			async open(args?: any): Promise<void> {}
		}

		it("can resolve by variant", () => {
			expect(getProgramFromVariant("test_get_a")).equal(A);
			expect(getProgramFromVariant("test_get_b")).equal(B);
		});

		it("will return undefined if missing", () => {
			expect(getProgramFromVariant("missing")).equal(undefined);
		});
	});
});
