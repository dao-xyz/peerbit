import { delay } from "@peerbit/time";
import { expect, use } from "chai";
import chaiAsPromised from "chai-as-promised";
import { Handler } from "../src/handler.js";
import {
	ClosedError,
	Program,
	type ProgramClient,
	ProgramHandler,
	TerminalOperationNotStartedError,
} from "../src/index.js";
import {
	TestNestedProgram,
	TestParenteRefernceProgram,
	TestProgram,
	TestSameAddressSiblingsProgram,
} from "./samples.js";
import { creatMockPeer } from "./utils.js";

use(chaiAsPromised);

describe(`shared`, () => {
	let client: ProgramClient;
	const reuseRoutes: {
		name: string;
		open: (program: TestProgram) => Promise<TestProgram>;
	}[] = [
		{
			name: "same-instance",
			open: (program) =>
				client.open(program, { existing: "reuse" }) as Promise<TestProgram>,
		},
		{
			name: "address",
			open: (program) =>
				client.open(program.address, {
					existing: "reuse",
				}) as Promise<TestProgram>,
		},
		{
			name: "closed clone",
			open: (program) =>
				client.open(program.clone(), {
					existing: "reuse",
				}) as Promise<TestProgram>,
		},
		{
			name: "already-open clone",
			open: async (program) => {
				const clone = program.clone();
				await clone.calculateAddress();
				clone.closed = false;
				return client.open(clone, { existing: "reuse" });
			},
		},
	];

	beforeEach(async () => {
		client = await creatMockPeer();
	});

	afterEach(async () => {
		await client.stop();
	});

	it("open same store twice will share instance", async () => {
		const db1 = await client.open(new TestProgram());
		expect(await client.open(db1)).equal(db1);
	});

	it("singleflights concurrent base close calls with exact promise identity", async () => {
		const program = await client.open(new TestProgram(142));
		const childClose = program.nested.close.bind(program.nested);
		let childCloseCalls = 0;
		let parentCloseEvents = 0;
		let childCloseEvents = 0;
		program.nested.close = (from) => {
			childCloseCalls += 1;
			return childClose(from);
		};
		program.events.addEventListener("close", (event) => {
			if (event.detail === program) parentCloseEvents += 1;
		});
		program.nested.events.addEventListener("close", () => {
			childCloseEvents += 1;
		});

		const first = program.close();
		const second = program.close();
		expect(second).to.equal(first);
		expect(await first).to.be.true;
		expect(childCloseCalls).to.equal(1);
		expect(parentCloseEvents).to.equal(1);
		expect(childCloseEvents).to.equal(1);
	});

	it("singleflights the full async subclass close before invoking it", async () => {
		const program = await client.open(new TestNestedProgram(146));
		const pubsub = client.services.pubsub;
		const unsubscribe = pubsub.unsubscribe.bind(pubsub);
		let unsubscribeCalls = 0;
		pubsub.unsubscribe = (...args) => {
			unsubscribeCalls += 1;
			return unsubscribe(...args);
		};

		const first = program.close();
		const second = program.close();
		expect(second).to.equal(first);
		expect(await first).to.be.true;
		expect(unsubscribeCalls).to.equal(1);
		pubsub.unsubscribe = unsubscribe;
	});

	it("invokes the first outer close synchronously so its fence is immediate", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestProgram(166);
		const baseClose = program.close.bind(program);
		let invoked = false;
		let releaseCleanup!: () => void;
		const cleanupGate = new Promise<void>((resolve) => {
			releaseCleanup = resolve;
		});
		program.close = async (from) => {
			invoked = true;
			const closed = await baseClose(from);
			await cleanupGate;
			return closed;
		};

		await handler.open(program);
		const closing = program.close();
		expect(invoked).to.be.true;
		expect(program.acceptsParentAttachments).to.be.false;
		releaseCleanup();
		expect(await closing).to.be.true;
		await handler.stop();
	});

	it("leaves an explicitly unstarted terminal rejection retryable", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestProgram(167);
		const baseDrop = program.drop.bind(program);
		let attempts = 0;
		program.drop = async (from) => {
			attempts += 1;
			if (attempts === 1) {
				throw new TerminalOperationNotStartedError(
					"synthetic terminal precondition rejection",
				);
			}
			return baseDrop(from);
		};

		await handler.open(program);
		await expect(program.drop()).to.be.rejectedWith(
			"synthetic terminal precondition rejection",
		);
		expect(program.closed).to.be.false;
		expect(await program.drop()).to.be.true;
		expect(attempts).to.equal(2);
		await handler.stop();
	});

	it("leaves an invalid parent release retryable with the correct owner", async () => {
		const handler = new ProgramHandler({ client });
		const program = await handler.open(new TestProgram(170));
		const wrongParent = new TestProgram(171);

		await expect(program.close(wrongParent)).to.be.rejectedWith(
			"Could not find from in parents",
		);
		expect(program.closed).to.be.false;
		expect(await program.close()).to.be.true;
		await handler.stop();
	});

	it("rejects callback-reentrant stop without deadlocking cleanup", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestNestedProgram(172);
		let callbackAttempts = 0;
		await handler.open(program, {
			onClose: async () => {
				callbackAttempts += 1;
				if (callbackAttempts === 1) await handler.stop();
			},
		});

		await expect(program.close()).to.be.rejectedWith(
			"Program lifecycle callbacks cannot wait for their own handler to stop",
		);
		expect(await program.close()).to.be.true;
		expect(callbackAttempts).to.equal(2);
		await handler.stop();
	});

	it("rejects callback-reentrant close without sharing its own promise", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestNestedProgram(173);
		let callbackAttempts = 0;
		await handler.open(program, {
			onClose: async () => {
				callbackAttempts += 1;
				if (callbackAttempts === 1) await program.close();
			},
		});

		await expect(program.close()).to.be.rejectedWith(
			"Program lifecycle callbacks cannot wait for their own terminal operation",
		);
		expect(await program.close()).to.be.true;
		expect(callbackAttempts).to.equal(2);
		await handler.stop();
	});

	it("allows a terminal callback to close an unrelated program", async () => {
		const handler = new ProgramHandler({ client });
		const other = await handler.open(new TestNestedProgram(423));
		const program = new TestNestedProgram(424);
		await handler.open(program, {
			onClose: async (closed) => {
				if (closed === program) await other.close();
			},
		});

		expect(await program.close()).to.be.true;
		expect(program.closed).to.be.true;
		expect(other.closed).to.be.true;
		await handler.stop();
	});

	it("rejects a child callback waiting for its active parent close", async () => {
		const handler = new ProgramHandler({ client });
		const parent = new TestProgram(431);
		let callbackAttempts = 0;
		await handler.open(parent, {
			onClose: async (closed) => {
				if (closed !== parent.nested) return;
				callbackAttempts += 1;
				if (callbackAttempts === 1) await parent.close();
			},
		});

		await expect(parent.close()).to.be.rejectedWith(
			"Program lifecycle callbacks cannot wait for their own terminal operation",
		);
		expect(await parent.close()).to.be.true;
		expect(callbackAttempts).to.equal(2);
		await handler.stop();
	});

	it("rejects callback-reentrant stop during open without deadlocking admission", async () => {
		const handler = new ProgramHandler({ client });
		await expect(
			handler.open(new TestProgram(174), {
				onBeforeOpen: async () => handler.stop(),
			}),
		).to.be.rejectedWith(
			"Program lifecycle callbacks cannot wait for their own handler to stop",
		);
		await handler.stop();
	});

	it("rejects stop invoked synchronously by a program open override", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestNestedProgram(458);
		program.open = async () => {
			await handler.stop();
		};

		await expect(handler.open(program)).to.be.rejectedWith(
			"Program lifecycle callbacks cannot wait for their own handler to stop",
		);
		await handler.stop();
	});

	it("rejects stop invoked synchronously by a terminal override", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestNestedProgram(459);
		const baseClose = program.close.bind(program);
		let stopFromClose = false;
		program.close = async (from) => {
			if (stopFromClose) await handler.stop();
			return baseClose(from);
		};
		await handler.open(program);
		stopFromClose = true;

		await expect(program.close()).to.be.rejectedWith(
			"Program lifecycle callbacks cannot wait for their own handler to stop",
		);
		stopFromClose = false;
		expect(await program.close()).to.be.true;
		await handler.stop();
	});

	it("lets an in-flight drop subsume close with one terminal operation", async () => {
		const program = await client.open(new TestProgram(143));
		const childDrop = program.nested.drop.bind(program.nested);
		let markChildDropStarted!: () => void;
		const childDropStarted = new Promise<void>((resolve) => {
			markChildDropStarted = resolve;
		});
		let releaseChildDrop!: () => void;
		const childDropGate = new Promise<void>((resolve) => {
			releaseChildDrop = resolve;
		});
		let childDropCalls = 0;
		program.nested.drop = async (from) => {
			childDropCalls += 1;
			markChildDropStarted();
			await childDropGate;
			return childDrop(from);
		};
		const rootAddress = program.address;
		const blocksRm = client.services.blocks.rm.bind(client.services.blocks);
		let rootDeleteCalls = 0;
		client.services.blocks.rm = (address) => {
			if (address === rootAddress) rootDeleteCalls += 1;
			return blocksRm(address);
		};

		const dropping = program.drop();
		await childDropStarted;
		const duplicateDrop = program.drop();
		const closing = program.close();
		expect(duplicateDrop).to.equal(dropping);
		expect(closing).to.equal(dropping);
		releaseChildDrop();
		expect(await dropping).to.be.true;
		expect(childDropCalls).to.equal(1);
		expect(rootDeleteCalls).to.equal(1);
	});

	it("serializes drop behind close and reports that close won", async () => {
		const program = await client.open(new TestProgram(144));
		const childClose = program.nested.close.bind(program.nested);
		let markChildCloseStarted!: () => void;
		const childCloseStarted = new Promise<void>((resolve) => {
			markChildCloseStarted = resolve;
		});
		let releaseChildClose!: () => void;
		const childCloseGate = new Promise<void>((resolve) => {
			releaseChildClose = resolve;
		});
		let childCloseCalls = 0;
		program.nested.close = async (from) => {
			childCloseCalls += 1;
			markChildCloseStarted();
			await childCloseGate;
			return childClose(from);
		};
		const rootAddress = program.address;
		const blocksRm = client.services.blocks.rm.bind(client.services.blocks);
		let rootDeleteCalls = 0;
		client.services.blocks.rm = (address) => {
			if (address === rootAddress) rootDeleteCalls += 1;
			return blocksRm(address);
		};

		const closing = program.close();
		await childCloseStarted;
		const dropping = program.drop();
		expect(dropping).not.to.equal(closing);
		releaseChildClose();
		expect(await closing).to.be.true;
		await expect(dropping).to.be.rejectedWith(ClosedError);
		expect(childCloseCalls).to.equal(1);
		expect(rootDeleteCalls).to.equal(0);
	});

	it("rejects parent reuse while attachment is fenced and permits a reopen", async () => {
		const program = await client.open(new TestProgram());
		const parent = await client.open(new TestProgram(1));
		program.preventNewParents();

		await expect(client.open(program, { parent })).to.be.rejectedWith(
			"Program is terminating and cannot accept another parent",
		);
		expect(program.parents).to.deep.equal([undefined]);
		expect(parent.children).not.to.include(program);

		await program.close();
		const reopened = await client.open(program, { parent });
		expect(reopened).to.equal(program);
		expect(reopened.parents).to.deep.equal([parent]);
	});

	it("keeps a dynamic parent alive when an address-reused root releases first", async () => {
		const parent = await client.open(new TestProgram(168));
		const program = await client.open(new TestProgram(169), { parent });
		expect(program.parents).to.deep.equal([parent]);

		const root = await client.open(program.address, { existing: "reuse" });
		expect(root).to.equal(program);
		expect(program.parents).to.deep.equal([parent, undefined]);

		expect(await root.close()).to.be.false;
		expect(program.closed).to.be.false;
		expect(program.parents).to.deep.equal([parent]);
		expect(parent.children).to.include(program);

		await parent.close();
		expect(program.closed).to.be.true;
	});

	for (const route of reuseRoutes) {
		it(`automatically fences ${route.name} reuse while child close drains`, async () => {
			const program = await client.open(new TestProgram());
			const originalClose = program.nested.close.bind(program.nested);
			let markChildCloseStarted!: () => void;
			const childCloseStarted = new Promise<void>((resolve) => {
				markChildCloseStarted = resolve;
			});
			let releaseChildClose!: () => void;
			const childCloseGate = new Promise<void>((resolve) => {
				releaseChildClose = resolve;
			});
			program.nested.close = async (from) => {
				markChildCloseStarted();
				await childCloseGate;
				return originalClose(from);
			};

			const closing = program.close();
			await childCloseStarted;
			try {
				await expect(route.open(program)).to.be.rejectedWith(
					"Program is terminating and cannot accept another parent",
				);
				expect(program.closed).to.be.false;
			} finally {
				releaseChildClose();
			}
			await closing;
			expect(program.closed).to.be.true;
		});

		it(`registers an undefined root attachment for ${route.name} reuse`, async () => {
			const parent = await client.open(new TestProgram(100));
			const program = await client.open(new TestProgram(101), { parent });
			expect(program.parents).to.deep.equal([parent]);

			const reused = await route.open(program);
			expect(reused).to.equal(program);
			expect(program.parents).to.deep.equal([parent, undefined]);

			await parent.close();
			expect(program.closed).to.be.false;
			expect(program.parents).to.deep.equal([undefined]);
			await program.close();
		});

		it(`rejects ${route.name} reuse while attachment is fenced`, async () => {
			const program = await client.open(new TestProgram());
			program.preventNewParents();

			await expect(route.open(program)).to.be.rejectedWith(
				"Program is terminating and cannot accept another parent",
			);
			expect(program.parents).to.deep.equal([undefined]);
		});

		it(`retains ${route.name} identity through post-super close cleanup`, async () => {
			const program = new TestProgram(137);
			const baseClose = program.close.bind(program);
			let markPostSuperCleanupStarted!: () => void;
			const postSuperCleanupStarted = new Promise<void>((resolve) => {
				markPostSuperCleanupStarted = resolve;
			});
			let releasePostSuperCleanup!: () => void;
			const postSuperCleanupGate = new Promise<void>((resolve) => {
				releasePostSuperCleanup = resolve;
			});
			program.close = async (from) => {
				const closed = await baseClose(from);
				markPostSuperCleanupStarted();
				await postSuperCleanupGate;
				return closed;
			};

			await client.open(program);
			const closing = program.close();
			await postSuperCleanupStarted;
			try {
				expect(program.closed).to.be.true;
				await expect(route.open(program)).to.be.rejectedWith(
					"Program is terminating and cannot accept another parent",
				);
			} finally {
				releasePostSuperCleanup();
			}
			await closing;
		});
	}

	it("retains address identity through post-super drop cleanup", async () => {
		const program = new TestProgram(138);
		const baseDrop = program.drop.bind(program);
		let markPostSuperCleanupStarted!: () => void;
		const postSuperCleanupStarted = new Promise<void>((resolve) => {
			markPostSuperCleanupStarted = resolve;
		});
		let releasePostSuperCleanup!: () => void;
		const postSuperCleanupGate = new Promise<void>((resolve) => {
			releasePostSuperCleanup = resolve;
		});
		program.drop = async (from) => {
			const dropped = await baseDrop(from);
			markPostSuperCleanupStarted();
			await postSuperCleanupGate;
			return dropped;
		};

		await client.open(program);
		const dropping = program.drop();
		await postSuperCleanupStarted;
		try {
			expect(program.closed).to.be.true;
			await expect(
				client.open(program.address, { existing: "reuse" }),
			).to.be.rejectedWith(
				"Program is terminating and cannot accept another parent",
			);
		} finally {
			releasePostSuperCleanup();
		}
		await dropping;
	});

	it("fences embedded child reuse during post-super cleanup", async () => {
		const handler = new ProgramHandler({ client });
		const nested = new TestNestedProgram(401);
		const baseClose = nested.close.bind(nested);
		let markPostSuperCleanupStarted!: () => void;
		const postSuperCleanupStarted = new Promise<void>((resolve) => {
			markPostSuperCleanupStarted = resolve;
		});
		let releasePostSuperCleanup!: () => void;
		const postSuperCleanupGate = new Promise<void>((resolve) => {
			releasePostSuperCleanup = resolve;
		});
		nested.close = async (from) => {
			const closed = await baseClose(from);
			markPostSuperCleanupStarted();
			await postSuperCleanupGate;
			return closed;
		};

		const firstParent = await handler.open(new TestProgram(402, nested));
		const closing = firstParent.close();
		await postSuperCleanupStarted;
		try {
			await expect(
				handler.open(new TestProgram(403, nested)),
			).to.be.rejectedWith("finishing");
			await expect(
				handler.open(nested.clone(), { existing: "reuse" }),
			).to.be.rejectedWith("finishing");
			await expect(
				handler.open(nested.address, { existing: "reuse" }),
			).to.be.rejectedWith("finishing");
			await expect(
				handler.open(new TestProgram(435, nested.clone())),
			).to.be.rejectedWith("finishing");
		} finally {
			releasePostSuperCleanup();
		}
		await closing;

		const secondParent = await handler.open(new TestProgram(404, nested));
		expect(secondParent.closed).to.be.false;
		expect(nested.closed).to.be.false;
		expect(nested.parents).to.deep.equal([secondParent]);
		await handler.stop();
	});

	it("promotes an embedded reuse to a monitored root owner", async () => {
		const handler = new ProgramHandler({ client });
		const parent = await handler.open(new TestProgram(436));
		const child = parent.nested;

		const reused = await handler.open(child.clone(), { existing: "reuse" });
		expect(reused).to.equal(child);
		expect(handler.items.get(child.address)).to.equal(child);
		expect(child.parents).to.include(undefined);

		expect(await parent.close()).to.be.true;
		expect(child.closed).to.be.false;
		await handler.stop();
		expect(child.closed).to.be.true;
	});

	it("does not resurrect a dropped child block from a rejected clone open", async () => {
		const handler = new ProgramHandler({ client });
		const parent = new TestProgram(428);
		const child = parent.nested;
		const baseChildDrop = child.drop.bind(child);
		let markPostSuperCleanupStarted!: () => void;
		const postSuperCleanupStarted = new Promise<void>((resolve) => {
			markPostSuperCleanupStarted = resolve;
		});
		let releasePostSuperCleanup!: () => void;
		const postSuperCleanupGate = new Promise<void>((resolve) => {
			releasePostSuperCleanup = resolve;
		});
		child.drop = async (from) => {
			const dropped = await baseChildDrop(from);
			markPostSuperCleanupStarted();
			await postSuperCleanupGate;
			return dropped;
		};

		await handler.open(parent);
		const childAddress = child.address;
		await child.save(client.services.blocks, { skipOnAddress: false });
		expect(await client.services.blocks.has(childAddress)).to.be.true;
		const dropping = parent.drop();
		await postSuperCleanupStarted;
		try {
			expect(await client.services.blocks.has(childAddress)).to.be.false;
			await expect(
				handler.open(child.clone(), { existing: "reuse" }),
			).to.be.rejectedWith("finishing");
			expect(await client.services.blocks.has(childAddress)).to.be.false;
		} finally {
			releasePostSuperCleanup();
		}
		expect(await dropping).to.be.true;
		expect(await client.services.blocks.has(childAddress)).to.be.false;
		await handler.stop();
	});

	it("fences embedded child reuse before base cleanup starts", async () => {
		const handler = new ProgramHandler({ client });
		const nested = new TestNestedProgram(408);
		const baseClose = nested.close.bind(nested);
		let markCleanupStarted!: () => void;
		const cleanupStarted = new Promise<void>((resolve) => {
			markCleanupStarted = resolve;
		});
		let releaseCleanup!: () => void;
		const cleanupGate = new Promise<void>((resolve) => {
			releaseCleanup = resolve;
		});
		nested.close = async (from) => {
			markCleanupStarted();
			await cleanupGate;
			return baseClose(from);
		};

		const firstParent = await handler.open(new TestProgram(409, nested));
		const closing = firstParent.close();
		await cleanupStarted;
		try {
			await expect(
				handler.open(new TestProgram(410, nested)),
			).to.be.rejectedWith("finishing");
			await expect(
				handler.open(new TestProgram(437, nested.clone())),
			).to.be.rejectedWith("finishing");
		} finally {
			releaseCleanup();
		}
		await closing;

		const secondParent = await handler.open(new TestProgram(411, nested));
		expect(nested.parents).to.deep.equal([secondParent]);
		await handler.stop();
	});

	for (const gateAt of ["save", "beforeOpen"] as const) {
		it(`reserves an embedded address through ${gateAt}`, async () => {
			const handler = new ProgramHandler({ client });
			const first = await handler.open(new TestProgram(438));
			const child = first.nested;
			const second = new TestProgram(gateAt === "save" ? 439 : 440, child);
			let markGateStarted!: () => void;
			const gateStarted = new Promise<void>((resolve) => {
				markGateStarted = resolve;
			});
			let releaseGate!: () => void;
			const gate = new Promise<void>((resolve) => {
				releaseGate = resolve;
			});

			const originalSave = second.save.bind(second);
			if (gateAt === "save") {
				second.save = async (...args: Parameters<TestProgram["save"]>) => {
					if (args[1]?.skipOnAddress === false) {
						markGateStarted();
						await gate;
					}
					return originalSave(...args);
				};
			}

			const opening = handler.open(second, {
				onBeforeOpen:
					gateAt === "beforeOpen"
						? async (opened) => {
								if (opened !== second) return;
								markGateStarted();
								await gate;
							}
						: undefined,
			});
			await gateStarted;
			try {
				await expect(first.drop()).to.be.rejectedWith(
					"open generation owns its address",
				);
				expect(first.closed).to.be.false;
				expect(child.closed).to.be.false;
			} finally {
				releaseGate();
			}
			expect(await opening).to.equal(second);
			expect(await first.drop()).to.be.true;
			expect(child.closed).to.be.false;
			expect(child.parents).to.deep.equal([second]);
			await handler.stop();
		});
	}

	it("does not expose a reserved child before its owning open completes", async () => {
		const handler = new ProgramHandler({ client });
		const root = new TestProgram(454);
		const child = root.nested;
		const baseOpen = root.open.bind(root);
		let markRootOpenStarted!: () => void;
		const rootOpenStarted = new Promise<void>((resolve) => {
			markRootOpenStarted = resolve;
		});
		let releaseRootOpen!: () => void;
		const rootOpenGate = new Promise<void>((resolve) => {
			releaseRootOpen = resolve;
		});
		root.open = async (args) => {
			markRootOpenStarted();
			await rootOpenGate;
			return baseOpen(args);
		};

		const rootOpening = handler.open(root);
		await rootOpenStarted;
		const waiters = [
			handler.open(child),
			handler.open(child.clone(), { existing: "reuse" }),
			handler.open(child.address, { existing: "reuse" }),
		];
		let waitersSettled = false;
		void Promise.all(waiters).then(() => {
			waitersSettled = true;
		});
		await delay(25);
		expect(waitersSettled).to.be.false;
		expect(child.openInvoked).to.be.false;

		releaseRootOpen();
		expect(await rootOpening).to.equal(root);
		for (const opened of await Promise.all(waiters)) {
			expect(opened).to.equal(child);
		}
		expect(child.openInvoked).to.be.true;
		await handler.stop();
	});

	it("allows concurrent roots with distinct same-address children", async () => {
		const handler = new ProgramHandler({ client });
		const seed = new TestNestedProgram(441);
		const first = new TestProgram(442, seed.clone());
		const second = new TestProgram(443, seed.clone());
		let markSaveStarted!: () => void;
		const saveStarted = new Promise<void>((resolve) => {
			markSaveStarted = resolve;
		});
		let releaseSave!: () => void;
		const saveGate = new Promise<void>((resolve) => {
			releaseSave = resolve;
		});
		const originalSave = first.save.bind(first);
		first.save = async (...args: Parameters<TestProgram["save"]>) => {
			if (args[1]?.skipOnAddress === false) {
				markSaveStarted();
				await saveGate;
			}
			return originalSave(...args);
		};

		const firstOpening = handler.open(first);
		await saveStarted;
		const secondOpening = handler.open(second);
		try {
			expect(await secondOpening).to.equal(second);
			expect(second.closed).to.be.false;
			expect(second.nested.closed).to.be.false;
		} finally {
			releaseSave();
		}
		expect(await firstOpening).to.equal(first);
		await handler.stop();
	});

	it("retains a promoted root when a distinct same-address child cleanup fails", async () => {
		const handler = new ProgramHandler({ client });
		const seed = new TestNestedProgram(470);
		const first = new TestProgram(471, seed.clone());
		const second = new TestProgram(472, seed.clone());
		let markSaveStarted!: () => void;
		const saveStarted = new Promise<void>((resolve) => {
			markSaveStarted = resolve;
		});
		let releaseSave!: () => void;
		const saveGate = new Promise<void>((resolve) => {
			releaseSave = resolve;
		});
		const originalSave = first.save.bind(first);
		first.save = async (...args: Parameters<TestProgram["save"]>) => {
			if (args[1]?.skipOnAddress === false) {
				markSaveStarted();
				await saveGate;
			}
			return originalSave(...args);
		};
		const childClose = second.nested.close.bind(second.nested);
		const cleanupError = new Error("synthetic colliding child cleanup failure");
		let childCloseAttempts = 0;
		second.nested.close = async (from) => {
			const closed = await childClose(from);
			childCloseAttempts += 1;
			if (childCloseAttempts === 1) throw cleanupError;
			return closed;
		};

		const firstOpening = handler.open(first);
		await saveStarted;
		const secondOpening = handler.open(second);
		try {
			await secondOpening;
		} finally {
			releaseSave();
		}
		await firstOpening;
		expect(first.nested).not.to.equal(second.nested);
		expect(first.nested.address.toString()).to.equal(
			second.nested.address.toString(),
		);

		// Promote the first identity to a root owner while the distinct clone
		// remains embedded under the second root.
		expect(await handler.open(first.nested)).to.equal(first.nested);
		expect(handler.items.get(first.nested.address)).to.equal(first.nested);
		await expect(second.close()).to.be.rejectedWith(cleanupError.message);
		expect(handler.items.get(first.nested.address)).to.equal(first.nested);

		await handler.stop();
		expect(childCloseAttempts).to.equal(2);
		expect(first.nested.closed).to.be.true;
		expect(second.nested.closed).to.be.true;
		expect(handler.items.size).to.equal(0);
	});

	it("releases a colliding retained identity after replacement cleanup", async () => {
		const handler = new ProgramHandler({ client });
		const seed = new TestNestedProgram(476);
		const first = new TestProgram(477, seed.clone());
		const second = new TestProgram(478, seed.clone());
		let markSaveStarted!: () => void;
		const saveStarted = new Promise<void>((resolve) => {
			markSaveStarted = resolve;
		});
		let releaseSave!: () => void;
		const saveGate = new Promise<void>((resolve) => {
			releaseSave = resolve;
		});
		const originalSave = first.save.bind(first);
		first.save = async (...args: Parameters<TestProgram["save"]>) => {
			if (args[1]?.skipOnAddress === false) {
				markSaveStarted();
				await saveGate;
			}
			return originalSave(...args);
		};
		const childClose = second.nested.close.bind(second.nested);
		const cleanupError = new Error(
			"synthetic replaced colliding child cleanup failure",
		);
		second.nested.close = async (from) => {
			await childClose(from);
			throw cleanupError;
		};

		const firstOpening = handler.open(first);
		await saveStarted;
		const secondOpening = handler.open(second);
		try {
			await secondOpening;
		} finally {
			releaseSave();
		}
		await firstOpening;
		expect(await handler.open(first.nested)).to.equal(first.nested);
		expect(handler.items.get(first.nested.address)).to.equal(first.nested);

		await expect(second.nested.close(second)).to.be.rejectedWith(
			cleanupError.message,
		);
		expect(second.nested.closed).to.be.true;
		expect(handler.items.get(first.nested.address)).to.equal(first.nested);

		let replacementCalls = 0;
		second.nested.close = async (from) => {
			replacementCalls += 1;
			expect(from).to.equal(second);
			return true;
		};
		await handler.stop();
		expect(replacementCalls).to.equal(1);
		expect(handler.items.size).to.equal(0);
		handler.start();
		await handler.stop();
	});

	it("retains a promoted root across a colliding child rollback failure", async () => {
		const handler = new ProgramHandler({ client });
		const seed = new TestNestedProgram(473);
		const first = new TestProgram(474, seed.clone());
		const second = new TestProgram(475, seed.clone());
		let markSecondBeforeOpen!: () => void;
		const secondBeforeOpen = new Promise<void>((resolve) => {
			markSecondBeforeOpen = resolve;
		});
		let releaseSecondBeforeOpen!: () => void;
		const secondBeforeOpenGate = new Promise<void>((resolve) => {
			releaseSecondBeforeOpen = resolve;
		});
		const openError = new Error("synthetic colliding root open failure");
		const cleanupError = new Error(
			"synthetic colliding rollback cleanup failure",
		);
		const childClose = second.nested.close.bind(second.nested);
		let childCloseAttempts = 0;
		second.nested.close = async (from) => {
			const closed = await childClose(from);
			childCloseAttempts += 1;
			if (childCloseAttempts === 1) throw cleanupError;
			return closed;
		};

		expect(await handler.open(first)).to.equal(first);
		expect(first.nested).not.to.equal(second.nested);
		// Promote the first child identity to an independently owned live root.
		expect(await handler.open(first.nested)).to.equal(first.nested);
		expect(handler.items.get(first.nested.address)).to.equal(first.nested);

		const secondOpening = handler.open(second, {
			onBeforeOpen: async (opened) => {
				if (opened !== second) return;
				markSecondBeforeOpen();
				await secondBeforeOpenGate;
				throw openError;
			},
		});
		await secondBeforeOpen;
		try {
			expect(first.nested.address.toString()).to.equal(
				second.nested.address.toString(),
			);
			// The distinct clone has been admitted and opened under the in-flight root,
			// without displacing the promoted identity at the shared address.
			expect(second.nested.closed).to.be.false;
			expect(handler.items.get(first.nested.address)).to.equal(first.nested);
		} finally {
			releaseSecondBeforeOpen();
		}
		await expect(secondOpening).to.be.rejectedWith(openError.message);
		expect(childCloseAttempts).to.equal(1);
		expect(second.nested.closed).to.be.true;
		expect(handler.items.get(first.nested.address)).to.equal(first.nested);

		await handler.stop();
		expect(childCloseAttempts).to.equal(2);
		expect(first.nested.closed).to.be.true;
		expect(second.nested.closed).to.be.true;
		expect(handler.items.size).to.equal(0);
	});

	it("supports same-address siblings in one opening graph", async () => {
		const handler = new ProgramHandler({ client });
		const root = new TestSameAddressSiblingsProgram(468, 469);
		await Promise.all([
			root.first.calculateAddress(),
			root.second.calculateAddress(),
		]);
		expect(root.first).not.to.equal(root.second);
		expect(root.first.address.toString()).to.equal(
			root.second.address.toString(),
		);

		expect(await handler.open(root)).to.equal(root);
		expect(root.nestedOpenResult).to.equal(root.first);
		expect(root.first.closed).to.be.false;
		expect(root.second.closed).to.be.false;
		expect(await root.close()).to.be.true;
		expect(root.first.closed).to.be.true;
		expect(root.second.closed).to.be.true;
		await handler.stop();
	});

	it("reconciles a shared child's released inverse owner edge", async () => {
		const handler = new ProgramHandler({ client });
		const child = new TestNestedProgram(444);
		const first = await handler.open(new TestProgram(445, child));
		const second = await handler.open(new TestProgram(446, child));

		expect(await child.close(first)).to.be.false;
		expect(child.parents).to.deep.equal([second]);
		expect(first.children).not.to.include(child);
		expect(await second.close()).to.be.true;
		expect(child.closed).to.be.true;

		const reopened = await handler.open(child.clone(), { existing: "reuse" });
		expect(reopened.closed).to.be.false;
		await handler.stop();
	});

	it("blocks a direct child drop while another root reserves it", async () => {
		const handler = new ProgramHandler({ client });
		const first = await handler.open(new TestProgram(447));
		const child = first.nested;
		await child.save(client.services.blocks, { skipOnAddress: false });
		const second = new TestProgram(448, child);
		let markSaveStarted!: () => void;
		const saveStarted = new Promise<void>((resolve) => {
			markSaveStarted = resolve;
		});
		let releaseSave!: () => void;
		const saveGate = new Promise<void>((resolve) => {
			releaseSave = resolve;
		});
		const originalSave = second.save.bind(second);
		second.save = async (...args: Parameters<TestProgram["save"]>) => {
			if (args[1]?.skipOnAddress === false) {
				markSaveStarted();
				await saveGate;
			}
			return originalSave(...args);
		};

		const opening = handler.open(second);
		await saveStarted;
		try {
			await expect(child.drop(first)).to.be.rejectedWith(
				"open generation owns its address",
			);
			expect(child.closed).to.be.false;
			expect(await client.services.blocks.has(child.address)).to.be.true;
		} finally {
			releaseSave();
		}
		expect(await opening).to.equal(second);
		expect(await child.drop(first)).to.be.false;
		expect(await client.services.blocks.has(child.address)).to.be.true;
		await handler.stop();
	});

	it("blocks cloned descendants during a direct post-super child drop", async () => {
		const handler = new ProgramHandler({ client });
		const child = new TestNestedProgram(449);
		const baseDrop = child.drop.bind(child);
		let markPostSuperStarted!: () => void;
		const postSuperStarted = new Promise<void>((resolve) => {
			markPostSuperStarted = resolve;
		});
		let releasePostSuper!: () => void;
		const postSuperGate = new Promise<void>((resolve) => {
			releasePostSuper = resolve;
		});
		child.drop = async (from) => {
			const dropped = await baseDrop(from);
			markPostSuperStarted();
			await postSuperGate;
			return dropped;
		};
		const first = await handler.open(new TestProgram(450, child));

		const dropping = child.drop(first);
		await postSuperStarted;
		try {
			await expect(
				handler.open(new TestProgram(451, child.clone())),
			).to.be.rejectedWith("operation is finishing");
		} finally {
			releasePostSuper();
		}
		expect(await dropping).to.be.true;
		expect(first.children).not.to.include(child);

		const reopened = await handler.open(new TestProgram(452, child.clone()));
		expect(reopened.nested.closed).to.be.false;
		await handler.stop();
	});

	it("reconciles a direct terminal child's inverse owner edge", async () => {
		const handler = new ProgramHandler({ client });
		const first = await handler.open(new TestProgram(453));
		const child = first.nested;

		expect(await child.close(first)).to.be.true;
		expect(child.parents).to.be.empty;
		expect(first.children).not.to.include(child);

		const reopened = await handler.open(child.clone(), { existing: "reuse" });
		expect(reopened.closed).to.be.false;
		await handler.stop();
	});

	it("reserves descendant addresses while a final close is queued", async () => {
		const handler = new ProgramHandler({ client });
		const owner = await handler.open(new TestProgram(429));
		const parent = new TestProgram(430);
		const child = parent.nested;
		const baseParentClose = parent.close.bind(parent);
		let markOwnerReleaseStarted!: () => void;
		const ownerReleaseStarted = new Promise<void>((resolve) => {
			markOwnerReleaseStarted = resolve;
		});
		let releaseOwnerClose!: () => void;
		const ownerCloseGate = new Promise<void>((resolve) => {
			releaseOwnerClose = resolve;
		});
		let gated = false;
		parent.close = async (from) => {
			if (from === owner && !gated) {
				gated = true;
				markOwnerReleaseStarted();
				await ownerCloseGate;
			}
			return baseParentClose(from);
		};

		await handler.open(parent);
		await handler.open(parent, { parent: owner });
		const ownerRelease = parent.close(owner);
		await ownerReleaseStarted;
		const finalClose = parent.close();
		try {
			expect(child.acceptsParentAttachments).to.be.true;
			await expect(
				handler.open(child.clone(), { existing: "reuse" }),
			).to.be.rejectedWith("finishing");
			await expect(
				handler.open(child.address, { existing: "reuse" }),
			).to.be.rejectedWith("finishing");
		} finally {
			releaseOwnerClose();
		}
		expect(await ownerRelease).to.be.false;
		expect(await finalClose).to.be.true;
		await handler.stop();
	});

	it("fences embedded child reuse after pre-super cleanup rejects", async () => {
		const handler = new ProgramHandler({ client });
		const nested = new TestNestedProgram(417);
		const baseClose = nested.close.bind(nested);
		const cleanupError = new Error("synthetic embedded pre-super failure");
		let attempts = 0;
		nested.close = async (from) => {
			attempts += 1;
			if (attempts === 1) throw cleanupError;
			return baseClose(from);
		};

		const firstParent = await handler.open(new TestProgram(418, nested));
		await expect(firstParent.close()).to.be.rejectedWith(cleanupError.message);
		expect(nested.acceptsParentAttachments).to.be.false;
		await expect(handler.open(new TestProgram(419, nested))).to.be.rejectedWith(
			"cleanup",
		);

		expect(await firstParent.close()).to.be.true;
		const secondParent = await handler.open(new TestProgram(420, nested));
		expect(nested.parents).to.deep.equal([secondParent]);
		await handler.stop();
	});

	it("retains rejected post-super cleanup until stop retries it", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestProgram(139);
		const baseClose = program.close.bind(program);
		const cleanupError = new Error("synthetic post-super cleanup failure");
		let cleanupAttempts = 0;
		program.close = async (from) => {
			const closed = await baseClose(from);
			cleanupAttempts += 1;
			if (cleanupAttempts === 1) {
				throw cleanupError;
			}
			return closed;
		};

		await handler.open(program);
		let failure: unknown;
		try {
			await program.close();
		} catch (error) {
			failure = error;
		}
		expect(failure).to.equal(cleanupError);
		expect(program.closed).to.be.true;
		expect(handler.items.get(program.address)).to.equal(program);
		await expect(
			handler.open(program.address, { existing: "reuse" }),
		).to.be.rejectedWith("failed terminal cleanup");

		await handler.stop();
		expect(cleanupAttempts).to.equal(2);
		expect(handler.items.size).to.equal(0);
	});

	it("monitors a terminal method replaced after open during stop", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestProgram(485);
		const baseClose = program.close.bind(program);
		await handler.open(program);
		const cleanupError = new Error(
			"synthetic post-open replacement cleanup failure",
		);
		let cleanupAttempts = 0;
		program.close = async (from) => {
			const closed = await baseClose(from);
			cleanupAttempts += 1;
			if (cleanupAttempts === 1) throw cleanupError;
			return closed;
		};

		await expect(handler.stop()).to.be.rejectedWith(cleanupError.message);
		expect(handler.items.get(program.address)).to.equal(program);
		expect(program.closed).to.be.true;

		await handler.stop();
		expect(cleanupAttempts).to.equal(2);
		expect(handler.items.size).to.equal(0);
		handler.start();
		await handler.stop();
	});

	it("unwraps a post-open replacement that delegates to its captured wrapper", async () => {
		const handler = new ProgramHandler({ client });
		const program = await handler.open(new TestProgram(486));
		const capturedWrapper = program.close;
		program.close = capturedWrapper.bind(program);

		let timeout: ReturnType<typeof setTimeout> | undefined;
		try {
			await Promise.race([
				handler.stop(),
				new Promise<never>((_, reject) => {
					timeout = setTimeout(
						() => reject(new Error("delegating replacement cleanup timed out")),
						1_000,
					);
				}),
			]);
		} finally {
			if (timeout) clearTimeout(timeout);
		}
		expect(program.closed).to.be.true;
		expect(handler.items.size).to.equal(0);
		handler.start();
		await handler.stop();
	});

	it("rejects an after-yield captured wrapper cycle without deadlocking stop", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestProgram(487);
		const baseClose = program.close.bind(program);
		await handler.open(program);
		const capturedWrapper = program.close;
		program.close = async (...args) => {
			await Promise.resolve();
			return capturedWrapper(...args);
		};

		const outcome = await Promise.race([
			handler.stop().then(
				() => "stopped" as const,
				(error: unknown) => error,
			),
			delay(1_000).then(() => "timeout" as const),
		]);
		expect(outcome).to.be.instanceOf(TerminalOperationNotStartedError);
		expect(program.closed).to.be.false;
		expect(handler.items.get(program.address)).to.equal(program);

		program.close = baseClose;
		await handler.stop();
		expect(program.closed).to.be.true;
		expect(handler.items.size).to.equal(0);
		handler.start();
		await handler.stop();
	});

	it("rejects synchronous cross-operation terminal delegation before mutation", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestProgram(488);
		const baseClose = program.close.bind(program);
		await handler.open(program);
		let attempts = 0;
		program.close = async (from) => {
			attempts += 1;
			await program.drop(from);
			throw new Error("unreachable cleanup failure");
		};

		const failure = await handler.stop().then(
			(): undefined => undefined,
			(error: unknown) => error,
		);
		expect(failure).to.be.instanceOf(TerminalOperationNotStartedError);
		expect(attempts).to.equal(1);
		expect(program.closed).to.be.false;
		expect(handler.items.get(program.address)).to.equal(program);

		program.close = baseClose;
		await handler.stop();
		expect(program.closed).to.be.true;
		expect(handler.items.size).to.equal(0);
		handler.start();
		await handler.stop();
	});

	it("rejects captured-wrapper delegation with a different owner before mutation", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestProgram(489);
		const baseClose = program.close.bind(program);
		await handler.open(program);
		const parentA = await handler.open(new TestProgram(490));
		const parentB = await handler.open(new TestProgram(491));
		await handler.open(program, { parent: parentA, existing: "reuse" });
		await handler.open(program, { parent: parentB, existing: "reuse" });
		const capturedWrapper = program.close;
		let attempts = 0;
		program.close = async (from) => {
			if (from === undefined) {
				attempts += 1;
				await capturedWrapper(parentB);
				throw new Error("unreachable wrong-owner cleanup failure");
			}
			return baseClose(from);
		};

		const failure = await handler.stop().then(
			(): undefined => undefined,
			(error: unknown) => error,
		);
		expect(failure).to.be.instanceOf(TerminalOperationNotStartedError);
		expect(attempts).to.equal(1);
		expect(program.closed).to.be.false;
		expect(program.parents).to.deep.equal([undefined]);
		expect(parentA.children).not.to.include(program);
		expect(parentB.children).not.to.include(program);

		program.close = baseClose;
		await handler.stop();
		expect(program.closed).to.be.true;
		expect(handler.items.size).to.equal(0);
		handler.start();
		await handler.stop();
	});

	it("retries base Program deletion after drop committed closed", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestProgram(141);
		await handler.open(program);
		const rootAddress = program.address;
		const childAddress = program.nested.address;
		const blocksRm = client.services.blocks.rm.bind(client.services.blocks);
		const cleanupError = new Error("synthetic block deletion failure");
		let rootDeleteAttempts = 0;
		let childDeleteAttempts = 0;
		client.services.blocks.rm = (address) => {
			if (address === childAddress) childDeleteAttempts += 1;
			if (address === rootAddress) {
				rootDeleteAttempts += 1;
				if (rootDeleteAttempts === 1) throw cleanupError;
			}
			return blocksRm(address);
		};

		await expect(program.drop()).to.be.rejectedWith(cleanupError.message);
		expect(program.closed).to.be.true;
		expect(handler.items.get(program.address)).to.equal(program);

		await handler.stop();
		expect(rootDeleteAttempts).to.equal(2);
		expect(childDeleteAttempts).to.equal(1);
		expect(handler.items.size).to.equal(0);
	});

	it("retries the exact drop after cleanup fails before base drop starts", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestProgram(179);
		const baseDrop = program.drop.bind(program);
		const cleanupError = new Error("synthetic pre-super drop failure");
		let attempts = 0;
		program.drop = async (from) => {
			attempts += 1;
			if (attempts === 1) throw cleanupError;
			return baseDrop(from);
		};

		await handler.open(program);
		const address = program.address;
		await expect(program.drop()).to.be.rejectedWith(cleanupError.message);
		expect(program.closed).to.be.false;
		expect(await program.drop()).to.be.true;
		expect(attempts).to.equal(2);
		expect(await client.services.blocks.has(address)).to.be.false;
		await handler.stop();
	});

	it("fences same-instance admission across handlers until drop retry", async () => {
		const firstHandler = new ProgramHandler({ client });
		const secondHandler = new ProgramHandler({ client });
		const program = new TestProgram(416);
		const baseDrop = program.drop.bind(program);
		const cleanupError = new Error("synthetic cross-handler drop failure");
		let attempts = 0;
		program.drop = async (from) => {
			attempts += 1;
			if (attempts === 1) throw cleanupError;
			return baseDrop(from);
		};

		await firstHandler.open(program);
		await expect(program.drop()).to.be.rejectedWith(cleanupError.message);
		expect(program.closed).to.be.false;
		expect(program.acceptsParentAttachments).to.be.false;
		await expect(secondHandler.open(program)).to.be.rejectedWith(
			"Program is terminating and cannot accept another parent",
		);

		expect(await program.drop()).to.be.true;
		expect(attempts).to.equal(2);
		await firstHandler.stop();

		const reopened = await secondHandler.open(program);
		expect(reopened).to.equal(program);
		expect(reopened.closed).to.be.false;
		await secondHandler.stop();
	});

	it("retries a fenced pre-super drop without downgrading deletion", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestProgram(182);
		const baseDrop = program.drop.bind(program);
		const cleanupError = new Error("synthetic fenced pre-super drop failure");
		let attempts = 0;
		program.drop = async (from) => {
			attempts += 1;
			if (attempts === 1) {
				program.preventNewParents();
				throw cleanupError;
			}
			return baseDrop(from);
		};

		await handler.open(program);
		const address = program.address;
		await expect(program.drop()).to.be.rejectedWith(cleanupError.message);
		expect(program.acceptsParentAttachments).to.be.false;
		expect(await program.drop()).to.be.true;
		expect(attempts).to.equal(2);
		expect(await client.services.blocks.has(address)).to.be.false;
		await handler.stop();
	});

	it("releases a provisional child lease when drop rejects before starting", async () => {
		const program = await client.open(new TestProgram(407), {
			args: { dontOpenNested: true },
		});

		await expect(program.drop()).to.be.rejectedWith(ClosedError);
		const nested = await client.open(program.nested);
		expect(nested.closed).to.be.false;

		expect(await program.drop()).to.be.true;
		expect(await nested.close()).to.be.true;
	});

	it("reconciles the parent edge after retrying base drop deletion", async () => {
		const handler = new ProgramHandler({ client });
		const parent = await handler.open(new TestProgram(180));
		const child = await handler.open(new TestProgram(181), { parent });
		const address = child.address;
		const blocksRm = client.services.blocks.rm.bind(client.services.blocks);
		const cleanupError = new Error("synthetic child block deletion failure");
		let attempts = 0;
		client.services.blocks.rm = (candidate) => {
			if (candidate === address) {
				attempts += 1;
				if (attempts === 1) throw cleanupError;
			}
			return blocksRm(candidate);
		};

		await expect(child.drop(parent)).to.be.rejectedWith(cleanupError.message);
		expect(child.parents).to.be.empty;
		expect(parent.children).to.include(child);
		expect(await child.drop(parent)).to.be.true;
		expect(attempts).to.equal(2);
		expect(parent.children).not.to.include(child);
		expect(await client.services.blocks.has(address)).to.be.false;

		handler.items.delete(parent.address);
		await handler.stop();
		await parent.close();
	});

	it("retries rejected post-super drop cleanup without dropping twice", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestProgram(147);
		const baseDrop = program.drop.bind(program);
		const cleanupError = new Error("synthetic post-super drop failure");
		let cleanupAttempts = 0;
		program.drop = async (from) => {
			const dropped = await baseDrop(from);
			cleanupAttempts += 1;
			if (cleanupAttempts === 1) throw cleanupError;
			return dropped;
		};

		await handler.open(program);
		await expect(program.drop()).to.be.rejectedWith(cleanupError.message);
		expect(program.closed).to.be.true;
		await handler.stop();
		expect(cleanupAttempts).to.equal(2);
		expect(handler.items.size).to.equal(0);
	});

	it("retries a nested post-super drop through a parent close", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestProgram(148);
		const childDrop = program.nested.drop.bind(program.nested);
		const cleanupError = new Error("synthetic nested post-super drop failure");
		let cleanupAttempts = 0;
		program.nested.drop = async (from) => {
			const dropped = await childDrop(from);
			cleanupAttempts += 1;
			if (cleanupAttempts === 1) throw cleanupError;
			return dropped;
		};

		await handler.open(program);
		await expect(program.drop()).to.be.rejectedWith(cleanupError.message);
		expect(program.closed).to.be.false;
		expect(program.nested.closed).to.be.true;
		await handler.stop();
		expect(cleanupAttempts).to.equal(2);
		expect(program.closed).to.be.true;
		expect(handler.items.size).to.equal(0);
	});

	it("retries a retained child whose outer close failed after super", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestProgram(145);
		const childClose = program.nested.close.bind(program.nested);
		const cleanupError = new Error("synthetic nested post-super failure");
		let childCloseAttempts = 0;
		let childCloseEvents = 0;
		let parentCloseEvents = 0;
		program.nested.close = async (from) => {
			const closed = await childClose(from);
			childCloseAttempts += 1;
			if (childCloseAttempts === 1) throw cleanupError;
			return closed;
		};
		program.nested.events.addEventListener("close", () => {
			childCloseEvents += 1;
		});
		program.events.addEventListener("close", (event) => {
			if (event.detail === program) parentCloseEvents += 1;
		});

		await handler.open(program);
		await expect(program.close()).to.be.rejectedWith(cleanupError.message);
		expect(program.closed).to.be.false;
		expect(program.nested.closed).to.be.true;
		expect(program.children).to.include(program.nested);
		expect(childCloseEvents).to.equal(1);
		expect(parentCloseEvents).to.equal(1);

		await handler.stop();
		expect(childCloseAttempts).to.equal(2);
		expect(childCloseEvents).to.equal(1);
		expect(parentCloseEvents).to.equal(1);
		expect(program.children).not.to.include(program.nested);
		expect(program.closed).to.be.true;
	});

	it("releases a parent lease after direct child recovery removes its edge", async () => {
		const handler = new ProgramHandler({ client });
		const parent = await handler.open(new TestProgram(412));
		const child = new TestProgram(413);
		const baseClose = child.close.bind(child);
		const cleanupError = new Error("synthetic direct child recovery failure");
		let cleanupAttempts = 0;
		child.close = async (from) => {
			const closed = await baseClose(from);
			cleanupAttempts += 1;
			if (cleanupAttempts === 1) throw cleanupError;
			return closed;
		};

		await handler.open(child, { parent });
		await expect(parent.close()).to.be.rejectedWith(cleanupError.message);
		expect(await child.close(parent)).to.be.true;
		expect(parent.children).not.to.include(child);
		expect(await parent.close()).to.be.true;

		const reopened = await handler.open(child);
		expect(reopened).to.equal(child);
		expect(reopened.closed).to.be.false;
		await handler.stop();
	});

	it("retries a committed non-terminal child release without releasing the next owner", async () => {
		const handler = new ProgramHandler({ client });
		const parentA = await handler.open(new TestProgram(149));
		const parentB = await handler.open(new TestProgram(150));
		const child = new TestProgram(151);
		const childClose = child.close.bind(child);
		const cleanupError = new Error("synthetic owner A post-super failure");
		let cleanupAttempts = 0;
		child.close = async (from) => {
			const closed = await childClose(from);
			cleanupAttempts += 1;
			if (cleanupAttempts === 1) throw cleanupError;
			return closed;
		};

		await handler.open(child, { parent: parentA });
		await handler.open(child, { parent: parentB });
		await expect(parentA.close()).to.be.rejectedWith(cleanupError.message);
		expect(child.parents).to.deep.equal([parentB]);
		expect(parentA.children).to.include(child);

		expect(await parentA.close()).to.be.true;
		expect(cleanupAttempts).to.equal(2);
		expect(parentA.closed).to.be.true;
		expect(child.closed).to.be.false;
		expect(child.parents).to.deep.equal([parentB]);
		expect(parentA.children).not.to.include(child);
		await handler.stop();
	});

	it("accepts replacement cleanup for a committed non-terminal release", async () => {
		const handler = new ProgramHandler({ client });
		const parentA = await handler.open(new TestProgram(479));
		const parentB = await handler.open(new TestProgram(480));
		const child = new TestProgram(481);
		const childClose = child.close.bind(child);
		const cleanupError = new Error(
			"synthetic replaced non-terminal owner cleanup failure",
		);
		let cleanupAttempts = 0;
		child.close = async (from) => {
			const closed = await childClose(from);
			cleanupAttempts += 1;
			if (cleanupAttempts === 1) throw cleanupError;
			return closed;
		};

		await handler.open(child, { parent: parentA });
		await handler.open(child, { parent: parentB });
		await expect(child.close(parentA)).to.be.rejectedWith(cleanupError.message);
		expect(child.closed).to.be.false;
		expect(child.parents).to.deep.equal([parentB]);

		let replacementCalls = 0;
		let remainingOwnerCalls = 0;
		const remainingOwnerError = new Error(
			"synthetic replacement remaining-owner cleanup failure",
		);
		child.close = async (from) => {
			if (from === parentA) {
				replacementCalls += 1;
				return false;
			}
			const closed = await childClose(from);
			remainingOwnerCalls += 1;
			if (remainingOwnerCalls === 1) throw remainingOwnerError;
			return closed;
		};
		const firstStopError = await handler.stop().then(
			(): undefined => undefined,
			(error: unknown) => error,
		);
		expect(String(firstStopError)).to.contain(remainingOwnerError.message);
		expect(remainingOwnerCalls).to.equal(2);
		expect(child.closed).to.be.true;
		// The first stop keeps draining independent retained items after preserving
		// the original error, so the child's exact cleanup retry can succeed in the
		// same pass. The next stop only finishes the parent that surfaced that error.
		await handler.stop();
		expect(replacementCalls).to.equal(1);
		expect(remainingOwnerCalls).to.equal(2);
		expect(child.closed).to.be.true;
		expect(parentA.children).not.to.include(child);
		expect(parentB.children).not.to.include(child);
		expect(handler.items.size).to.equal(0);
		handler.start();
		await handler.stop();
		await parentA.close();
		await parentB.close();
	});

	it("does not clear a newer queued owner failure with an older retry", async () => {
		const handler = new ProgramHandler({ client });
		const parentA = await handler.open(new TestProgram(482));
		const parentB = await handler.open(new TestProgram(483));
		const child = new TestProgram(484);
		const childClose = child.close.bind(child);
		const oldAError = new Error("synthetic old owner A cleanup failure");
		const newBError = new Error("synthetic new owner B cleanup failure");
		let ownerAAttempts = 0;
		let ownerBAttempts = 0;
		let markOwnerARetryStarted!: () => void;
		const ownerARetryStarted = new Promise<void>((resolve) => {
			markOwnerARetryStarted = resolve;
		});
		let releaseOwnerARetry!: () => void;
		const ownerARetryGate = new Promise<void>((resolve) => {
			releaseOwnerARetry = resolve;
		});
		child.close = async (from) => {
			const closed = await childClose(from);
			if (from === parentA) {
				ownerAAttempts += 1;
				if (ownerAAttempts === 1) throw oldAError;
				if (ownerAAttempts === 2) {
					markOwnerARetryStarted();
					await ownerARetryGate;
				}
			} else if (from === parentB) {
				ownerBAttempts += 1;
				if (ownerBAttempts === 1) throw newBError;
			}
			return closed;
		};

		await handler.open(child, { parent: parentA });
		await handler.open(child, { parent: parentB });
		await expect(child.close(parentA)).to.be.rejectedWith(oldAError.message);
		expect(child.parents).to.deep.equal([parentB]);
		handler.items.delete(parentA.address);
		handler.items.delete(parentB.address);

		const stopping = handler.stop();
		await ownerARetryStarted;
		const closingB = child.close(parentB);
		releaseOwnerARetry();
		await expect(closingB).to.be.rejectedWith(newBError.message);
		await expect(stopping).to.be.rejectedWith(
			"did not make ownership progress",
		);

		await handler.stop();
		expect(ownerAAttempts).to.equal(2);
		expect(ownerBAttempts).to.equal(2);
		expect(child.closed).to.be.true;
		expect(parentA.children).not.to.include(child);
		expect(parentB.children).not.to.include(child);
		handler.start();
		await handler.stop();
		await parentA.close();
		await parentB.close();
	});

	it("retries a committed terminal child cleanup with the original owner", async () => {
		const handler = new ProgramHandler({ client });
		const parent = await handler.open(new TestProgram(156));
		const child = new TestProgram(157);
		const childClose = child.close.bind(child);
		const cleanupError = new Error("synthetic terminal owner cleanup failure");
		const cleanupOwners: (TestProgram | undefined)[] = [];
		child.close = async (from) => {
			const closed = await childClose(from);
			cleanupOwners.push(from as TestProgram | undefined);
			if (cleanupOwners.length === 1) throw cleanupError;
			return closed;
		};

		await handler.open(child, { parent });
		await expect(child.close(parent)).to.be.rejectedWith(cleanupError.message);
		expect(child.closed).to.be.true;
		expect(child.parents).to.be.empty;
		expect(parent.children).to.include(child);

		handler.items.delete(parent.address);
		await handler.stop();
		expect(cleanupOwners).to.deep.equal([parent, parent]);
		expect(parent.children).not.to.include(child);
		await parent.close();
	});

	it("serializes duplicate owner releases instead of coalescing them", async () => {
		const handler = new ProgramHandler({ client });
		const parent = await handler.open(new TestProgram(158));
		const child = new TestProgram(159);
		const childClose = child.close.bind(child);
		let cleanupCalls = 0;
		child.close = async (from) => {
			cleanupCalls += 1;
			await delay(1);
			return childClose(from);
		};

		await handler.open(child, { parent });
		await handler.open(child, { parent });
		expect(child.parents).to.deep.equal([parent, parent]);
		expect(
			parent.children.filter((candidate) => candidate === child),
		).to.have.length(2);

		const first = child.close(parent);
		const second = child.close(parent);
		expect(second).not.to.equal(first);
		expect(await first).to.be.false;
		expect(await second).to.be.true;
		expect(cleanupCalls).to.equal(2);
		expect(child.parents).to.be.empty;
		expect(parent.children).not.to.include(child);

		handler.items.delete(parent.address);
		await handler.stop();
		await parent.close();
	});

	it("serializes immediate duplicate owner releases after synchronous base progress", async () => {
		const handler = new ProgramHandler({ client });
		const parent = await handler.open(new TestProgram(175));
		const child = new TestProgram(176);

		await handler.open(child, { parent });
		await handler.open(child, { parent });
		const first = child.close(parent);
		const second = child.close(parent);

		expect(second).not.to.equal(first);
		expect(await first).to.be.false;
		expect(await second).to.be.true;
		expect(child.parents).to.be.empty;
		expect(parent.children).not.to.include(child);
		handler.items.delete(parent.address);
		await handler.stop();
		await parent.close();
	});

	it("keeps duplicate ownership edges aligned after a later close fails", async () => {
		const handler = new ProgramHandler({ client });
		const parent = await handler.open(new TestProgram(405));
		const child = new TestProgram(406);
		const baseClose = child.close.bind(child);
		const cleanupError = new Error("synthetic second owner close failure");
		let closeAttempts = 0;
		child.close = async (from) => {
			closeAttempts += 1;
			if (closeAttempts === 2) throw cleanupError;
			return baseClose(from);
		};

		await handler.open(child, { parent });
		await handler.open(child, { parent });
		handler.items.delete(parent.address);
		await expect(handler.stop()).to.be.rejectedWith(cleanupError.message);
		expect(child.closed).to.be.false;
		expect(child.parents).to.deep.equal([parent]);
		expect(
			parent.children.filter((candidate) => candidate === child),
		).to.have.length(1);

		await handler.stop();
		expect(child.closed).to.be.true;
		expect(parent.children).not.to.include(child);
		await parent.close();
	});

	it("restores wrapped terminal methods before another handler reopens an instance", async () => {
		const firstHandler = new ProgramHandler({ client });
		const secondHandler = new ProgramHandler({ client });
		const program = new TestProgram(177);
		const originalClose = program.close;
		const originalDrop = program.drop;

		await firstHandler.open(program);
		expect(program.close).not.to.equal(originalClose);
		expect(await program.close()).to.be.true;
		expect(program.close).to.equal(originalClose);
		expect(program.drop).to.equal(originalDrop);

		await secondHandler.open(program);
		expect(program.close).not.to.equal(originalClose);
		expect(await program.close()).to.be.true;
		expect(program.close).to.equal(originalClose);
		expect(program.drop).to.equal(originalDrop);
		await firstHandler.stop();
		await secondHandler.stop();
	});

	it("does not spend a duplicate owner reference past failed cleanup", async () => {
		const handler = new ProgramHandler({ client });
		const parent = await handler.open(new TestProgram(163));
		const child = new TestProgram(164);
		const childClose = child.close.bind(child);
		const cleanupError = new Error("synthetic duplicate owner cleanup failure");
		let cleanupAttempts = 0;
		child.close = async (from) => {
			const closed = await childClose(from);
			cleanupAttempts += 1;
			if (cleanupAttempts === 1) throw cleanupError;
			return closed;
		};

		await handler.open(child, { parent });
		await handler.open(child, { parent });
		await expect(parent.close()).to.be.rejectedWith(cleanupError.message);
		expect(cleanupAttempts).to.equal(1);
		expect(child.closed).to.be.false;
		expect(child.parents).to.deep.equal([parent]);
		expect(
			parent.children.filter((candidate) => candidate === child),
		).to.have.length(2);

		expect(await parent.close()).to.be.true;
		expect(cleanupAttempts).to.equal(3);
		expect(child.closed).to.be.true;
		expect(child.parents).to.be.empty;
		expect(parent.children).not.to.include(child);

		await handler.stop();
	});

	it("rejects a parent close when a child reports no ownership progress", async () => {
		const handler = new ProgramHandler({ client });
		const parent = await handler.open(new TestProgram(168));
		const child = new TestProgram(169);
		const childClose = child.close.bind(child);
		let allowProgress = false;
		let childCloseCalls = 0;
		child.close = (from) => {
			childCloseCalls += 1;
			return allowProgress ? childClose(from) : Promise.resolve(false);
		};

		await handler.open(child, { parent });
		await handler.open(child, { parent });
		await expect(parent.close()).to.be.rejectedWith(
			"without reaching its base terminal operation",
		);
		expect(childCloseCalls).to.equal(1);
		expect(parent.closed).to.be.false;
		expect(child.closed).to.be.false;
		expect(child.parents).to.deep.equal([parent, parent]);
		expect(
			parent.children.filter((candidate) => candidate === child),
		).to.have.length(2);

		allowProgress = true;
		expect(await parent.close()).to.be.true;
		expect(childCloseCalls).to.equal(3);
		expect(child.closed).to.be.true;
		expect(parent.children).not.to.include(child);
		await handler.stop();
	});

	it("rejects inverse-only child cleanup that retains parent ownership", async () => {
		const handler = new ProgramHandler({ client });
		const parent = await handler.open(new TestProgram(485));
		const child = new TestProgram(486);
		const childClose = child.close.bind(child);
		child.close = async (from) => {
			const childIndex = from?.children.indexOf(child) ?? -1;
			if (childIndex !== -1) from!.children.splice(childIndex, 1);
			return false;
		};

		await handler.open(child, { parent });
		await expect(parent.close()).to.be.rejectedWith(
			"without reaching its base terminal operation",
		);
		expect(parent.closed).to.be.false;
		expect(child.closed).to.be.false;
		expect(child.parents).to.deep.equal([parent]);
		expect(parent.children).to.include(child);

		child.close = childClose;
		await handler.stop();
	});

	it("singleflights a queued release that will become terminal", async () => {
		const handler = new ProgramHandler({ client });
		const parentA = await handler.open(new TestProgram(160));
		const parentB = await handler.open(new TestProgram(161));
		const child = new TestProgram(162);
		const childClose = child.close.bind(child);
		let cleanupCalls = 0;
		child.close = async (from) => {
			cleanupCalls += 1;
			await delay(1);
			return childClose(from);
		};

		await handler.open(child, { parent: parentA });
		await handler.open(child, { parent: parentB });
		const releaseA = child.close(parentA);
		const releaseB = child.close(parentB);
		const duplicateB = child.close(parentB);

		expect(releaseB).not.to.equal(releaseA);
		expect(duplicateB).to.equal(releaseB);
		expect(await releaseA).to.be.false;
		expect(await releaseB).to.be.true;
		expect(cleanupCalls).to.equal(2);
		expect(child.parents).to.be.empty;
		expect(parentA.children).not.to.include(child);
		expect(parentB.children).not.to.include(child);

		await handler.stop();
	});

	it("waits for and retries a failing outer close during stop", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestProgram(140);
		const baseClose = program.close.bind(program);
		const cleanupError = new Error("synthetic in-flight cleanup failure");
		let cleanupAttempts = 0;
		let markFirstCleanupStarted!: () => void;
		const firstCleanupStarted = new Promise<void>((resolve) => {
			markFirstCleanupStarted = resolve;
		});
		let releaseFirstCleanup!: () => void;
		const firstCleanupGate = new Promise<void>((resolve) => {
			releaseFirstCleanup = resolve;
		});
		program.close = async (from) => {
			const closed = await baseClose(from);
			cleanupAttempts += 1;
			if (cleanupAttempts === 1) {
				markFirstCleanupStarted();
				await firstCleanupGate;
				throw cleanupError;
			}
			return closed;
		};

		await handler.open(program);
		const closing = program.close();
		await firstCleanupStarted;
		let stopSettled = false;
		const stopping = handler.stop().then(() => {
			stopSettled = true;
		});
		await delay(25);
		expect(stopSettled).to.be.false;
		releaseFirstCleanup();

		let closeFailure: unknown;
		try {
			await closing;
		} catch (error) {
			closeFailure = error;
		}
		expect(closeFailure).to.equal(cleanupError);
		await stopping;
		expect(cleanupAttempts).to.equal(2);
		expect(handler.items.size).to.equal(0);
	});

	it("serializes an address reload after evicting a stale closed cache entry", async () => {
		const handler = new ProgramHandler({ client });
		const stale = await handler.open(new TestProgram(102));
		const address = stale.address;
		await stale.close();
		handler.items.set(address, stale);
		const parent = await handler.open(new TestProgram(103));

		const originalGet = client.services.blocks.get.bind(client.services.blocks);
		let markLoadStarted!: () => void;
		const loadStarted = new Promise<void>((resolve) => {
			markLoadStarted = resolve;
		});
		let releaseLoad!: () => void;
		const loadGate = new Promise<void>((resolve) => {
			releaseLoad = resolve;
		});
		let matchingLoads = 0;
		client.services.blocks.get = async (cid, options) => {
			if (cid === address) {
				matchingLoads += 1;
				markLoadStarted();
				await loadGate;
			}
			return originalGet(cid, options);
		};

		let rootOpen: Promise<TestProgram> | undefined;
		let parentOpen: Promise<TestProgram> | undefined;
		try {
			rootOpen = handler.open(address, {
				existing: "reuse",
			}) as Promise<TestProgram>;
			await loadStarted;
			let parentResolved = false;
			parentOpen = (
				handler.open(address, { parent }) as Promise<TestProgram>
			).then((program) => {
				parentResolved = true;
				return program;
			});

			await delay(25);
			expect(parentResolved).to.be.false;
			releaseLoad();

			const [rootResult, parentResult] = await Promise.all([
				rootOpen,
				parentOpen,
			]);
			expect(rootResult).to.equal(parentResult);
			expect(rootResult.closed).to.be.false;
			expect(handler.items.get(address)).to.equal(rootResult);
			expect(rootResult.parents).to.have.length(2);
			expect(rootResult.parents).to.have.members([undefined, parent]);
			expect(
				parent.children.filter((child) => child === rootResult),
			).to.have.length(1);
			expect(matchingLoads).to.equal(1);
		} finally {
			releaseLoad();
			await Promise.allSettled(
				[rootOpen, parentOpen].filter(
					(open): open is Promise<TestProgram> => open !== undefined,
				),
			);
			client.services.blocks.get = originalGet;
			await handler.stop();
		}
	});

	it("serializes a closed-clone reopen after evicting a stale closed cache entry", async () => {
		const handler = new ProgramHandler({ client });
		const stale = await handler.open(new TestProgram(104));
		const address = stale.address;
		await stale.close();
		handler.items.set(address, stale);
		const parent = await handler.open(new TestProgram(105));
		const candidate = stale.clone();

		let markOpenStarted!: () => void;
		const openStarted = new Promise<void>((resolve) => {
			markOpenStarted = resolve;
		});
		let releaseOpen!: () => void;
		const openGate = new Promise<void>((resolve) => {
			releaseOpen = resolve;
		});
		let openCalls = 0;
		const originalOpen = candidate.open.bind(candidate);
		candidate.open = async (args) => {
			openCalls += 1;
			markOpenStarted();
			await openGate;
			return originalOpen(args);
		};

		let rootOpen: Promise<TestProgram> | undefined;
		let parentOpen: Promise<TestProgram> | undefined;
		try {
			rootOpen = handler.open(candidate, { existing: "reuse" });
			const firstOutcome = await Promise.race([
				openStarted.then(() => "started" as const),
				rootOpen.then(() => "resolved" as const),
			]);
			expect(firstOutcome).to.equal("started");

			let parentResolved = false;
			parentOpen = (
				handler.open(address, { parent }) as Promise<TestProgram>
			).then((program) => {
				parentResolved = true;
				return program;
			});
			await delay(25);
			expect(parentResolved).to.be.false;
			releaseOpen();

			const [rootResult, parentResult] = await Promise.all([
				rootOpen,
				parentOpen,
			]);
			expect(rootResult).to.equal(candidate);
			expect(rootResult).to.equal(parentResult);
			expect(rootResult.closed).to.be.false;
			expect(handler.items.get(address)).to.equal(rootResult);
			expect(rootResult.parents).to.have.length(2);
			expect(rootResult.parents).to.have.members([undefined, parent]);
			expect(
				parent.children.filter((child) => child === rootResult),
			).to.have.length(1);
			expect(openCalls).to.equal(1);
		} finally {
			releaseOpen();
			await Promise.allSettled(
				[rootOpen, parentOpen].filter(
					(open): open is Promise<TestProgram> => open !== undefined,
				),
			);
			await handler.stop();
		}
	});

	it("can open different dbs concurrently", async () => {
		let timeout = 3000;
		let t0 = +new Date();
		const nonExisting = client.open(
			"zb2rhXREnAbm5Twtm2ahJM7QKT6FoQGNksWv5jp7o5W6BQ7ax",
			{ timeout },
		);
		expect(await client.open(new TestProgram())).to.exist;
		let t1 = +new Date();

		expect(t1 - t0).lessThan(timeout); // Because db1 will be opened concurrently
		await expect(nonExisting).rejectedWith(
			"Failed to resolve program with address: zb2rhXREnAbm5Twtm2ahJM7QKT6FoQGNksWv5jp7o5W6BQ7ax",
		);
	});

	it("open same store twice by address will throw error", async () => {
		const db1 = await client.open(new TestProgram());
		await expect(client.open(db1.address)).rejectedWith(
			`Program at ${db1.address} is already open`,
		);
	});

	it("reuse open by address returns existing instance", async () => {
		const db1 = await client.open(new TestProgram());
		const db2 = await client.open(db1.address, { existing: "reuse" });
		expect(db2).equal(db1);
	});

	it("replace open by address closes old and reopens", async () => {
		const db1 = await client.open(new TestProgram());
		const address = db1.address;

		const db2 = await client.open(address, { existing: "replace" });

		expect(db1.closed).to.be.true;
		expect(db2.closed).to.be.false;
		expect(db2.address).to.equal(address);
		expect(db2).to.not.equal(db1);
	});

	it("honors reject for an already-open parent-owned program", async () => {
		const parent = await client.open(new TestProgram(123));
		const original = await client.open(new TestProgram(124), { parent });
		const candidate = original.clone();
		const parentChildrenBefore = [...parent.children];

		await expect(
			client.open(candidate, { parent, existing: "reject" }),
		).to.be.rejectedWith(`Program at ${original.address} is already open`);
		expect(original.closed).to.be.false;
		expect(parent.children).to.deep.equal(parentChildrenBefore);
		expect(parent.children).not.to.include(candidate);
		expect(candidate.closed).to.be.true;
	});

	it("replaces a sole parent-owned program without leaving a stale child edge", async () => {
		const parent = await client.open(new TestProgram(125));
		const original = await client.open(new TestProgram(126), { parent });
		const candidate = original.clone();
		const unrelatedChildren = parent.children.filter(
			(child) => child !== original,
		);

		const replacement = await client.open(candidate, {
			parent,
			existing: "replace",
		});

		expect(replacement).to.equal(candidate);
		expect(original.closed).to.be.true;
		expect(original.parents).to.deep.equal([]);
		expect(
			parent.children.filter((child) => child !== candidate),
		).to.deep.equal(unrelatedChildren);
		expect(parent.children).not.to.include(original);
		expect(
			parent.children.filter((child) => child === candidate),
		).to.have.length(1);
		expect(candidate.parents).to.deep.equal([parent]);

		await parent.drop();
		expect(candidate.closed).to.be.true;
	});

	it("rejects parent replacement while another parent still owns the program", async () => {
		const firstParent = await client.open(new TestProgram(127));
		const secondParent = await client.open(new TestProgram(128));
		const original = await client.open(new TestProgram(129), {
			parent: firstParent,
		});
		await client.open(original, { parent: secondParent });
		const candidate = original.clone();
		const firstParentChildrenBefore = [...firstParent.children];
		const secondParentChildrenBefore = [...secondParent.children];

		await expect(
			client.open(candidate, {
				parent: firstParent,
				existing: "replace",
			}),
		).to.be.rejectedWith("cannot be replaced while it has other owners");
		expect(original.closed).to.be.false;
		expect(original.parents).to.have.members([firstParent, secondParent]);
		expect(firstParent.children).to.deep.equal(firstParentChildrenBefore);
		expect(secondParent.children).to.deep.equal(secondParentChildrenBefore);
		expect(firstParent.children).not.to.include(candidate);
		expect(secondParent.children).not.to.include(candidate);
	});

	for (const route of ["address", "closed clone"] as const) {
		it(`does not replace through ${route} while another owner remains`, async () => {
			const parent = await client.open(new TestProgram(106));
			const original = await client.open(new TestProgram(107), { parent });
			await client.open(original);
			const parentsBefore = [...original.parents];
			const parentChildrenBefore = [...parent.children];
			const candidate = original.clone();

			const replacement =
				route === "address"
					? client.open(original.address, { existing: "replace" })
					: client.open(candidate, { existing: "replace" });
			await expect(replacement).to.be.rejectedWith(
				"cannot be replaced while it has other owners",
			);

			expect(original.closed).to.be.false;
			expect(original.parents).to.deep.equal(parentsBefore);
			expect(parent.children).to.deep.equal(parentChildrenBefore);
			expect(
				parent.children.filter((child) => child === original),
			).to.have.length(1);
			expect(
				await client.open(original.address, { existing: "reuse" }),
			).to.equal(original);
			if (route === "closed clone") {
				expect(candidate.closed).to.be.true;
			}
		});
	}

	it("does not replace when close reports a non-terminal result", async () => {
		const original = await client.open(new TestProgram(108));
		const candidate = original.clone();
		const originalClose = original.close.bind(original);
		original.close = async () => false;
		try {
			await expect(
				client.open(candidate, { existing: "replace" }),
			).to.be.rejectedWith("close was not terminal");

			expect(original.closed).to.be.false;
			expect(original.parents).to.deep.equal([undefined]);
			expect(candidate.closed).to.be.true;
			expect(
				await client.open(original.address, { existing: "reuse" }),
			).to.equal(original);
		} finally {
			original.close = originalClose;
		}
	});

	it("rolls back a failed initialization and permits a clean retry", async () => {
		const handler = new ProgramHandler({ client });
		const parent = await handler.open(new TestProgram(109));
		const candidate = new TestProgram(110);
		const originalError = new Error("synthetic open failure");
		const parentsBefore = candidate.parents;
		const childrenBefore = candidate.children;
		const parentChildrenBefore = [...parent.children];
		const originalOpen = candidate.open.bind(candidate);
		const originalClose = candidate.close.bind(candidate);
		const closeOwners: Array<TestProgram | undefined> = [];
		let attempts = 0;
		candidate.open = async (args) => {
			attempts += 1;
			if (attempts === 1) {
				throw originalError;
			}
			await originalOpen(args);
		};
		candidate.close = async (from) => {
			closeOwners.push(from as TestProgram | undefined);
			return originalClose(from);
		};

		try {
			let failure: unknown;
			try {
				await handler.open(candidate, { parent });
			} catch (error) {
				failure = error;
			}
			expect(failure).to.equal(originalError);
			expect(handler.items.has(candidate.address)).to.be.false;
			expect(candidate.closed).to.be.true;
			expect(candidate.parents).to.deep.equal(parentsBefore);
			expect(candidate.children).to.deep.equal(childrenBefore);
			expect(candidate.nested.closed).to.be.true;
			expect(parent.children).to.deep.equal(parentChildrenBefore);
			expect(closeOwners).to.deep.equal([parent]);

			const retried = await handler.open(candidate, { parent });
			expect(retried).to.equal(candidate);
			expect(retried.closed).to.be.false;
			expect(attempts).to.equal(2);
			expect(handler.items.get(candidate.address)).to.equal(candidate);
			expect(candidate.parents).to.deep.equal([parent]);
			expect(
				parent.children.filter((child) => child === candidate),
			).to.have.length(1);
		} finally {
			await handler.stop();
		}
	});

	it("reconciles a legacy owner release without terminal proof during failed-open rollback", async () => {
		const handler = new Handler<TestNestedProgram>({
			client,
			identity: client.identity,
			load: async () => undefined,
			shouldMonitor: (program) => program instanceof Program,
			getDependencies: (program) => program.allPrograms,
		});
		const parent = await handler.open(new TestNestedProgram(9553));
		const candidate = new TestNestedProgram(9554);
		const originalAfterOpen = candidate.afterOpen.bind(candidate);
		const openError = new Error("synthetic legacy afterOpen failure");
		candidate.afterOpen = async () => {
			await originalAfterOpen();
			throw openError;
		};

		try {
			await expect(handler.open(candidate, { parent })).to.be.rejectedWith(
				openError.message,
			);
			expect(candidate.closed).to.be.true;
			expect(candidate.parents ?? []).to.deep.equal([]);
			expect(parent.children ?? []).not.to.include(candidate);
			expect(handler.items.has(candidate.address)).to.be.false;
			await handler.stop();
			handler.start();
			await handler.stop();
		} finally {
			await handler.stop().catch((): void => undefined);
		}
	});

	for (const baseline of ["absent", "empty"] as const) {
		it(`borrows the root rollback owner without changing an ${baseline} baseline`, async () => {
			const handler = new ProgramHandler({ client });
			const candidate = new TestProgram(baseline === "absent" ? 501 : 502);
			if (baseline === "absent") {
				delete (candidate as unknown as { parents?: unknown[] }).parents;
				delete (candidate as unknown as { children?: unknown[] }).children;
			} else {
				candidate.parents = [];
				candidate.children = [];
			}
			const originalClose = candidate.close.bind(candidate);
			const cleanupError = new Error(`synthetic ${baseline} pre-base cleanup`);
			const forwardCounts: number[] = [];
			let closeAttempts = 0;
			candidate.open = async () => {
				throw new Error(`synthetic ${baseline} open failure`);
			};
			candidate.close = async (from) => {
				forwardCounts.push(
					candidate.parents?.filter((owner) => owner === from).length ?? 0,
				);
				closeAttempts += 1;
				if (closeAttempts <= 2) throw cleanupError;
				return originalClose(from);
			};

			await expect(handler.open(candidate)).to.be.rejectedWith(
				`synthetic ${baseline} open failure`,
			);
			expect(
				Object.prototype.hasOwnProperty.call(candidate, "parents"),
			).to.equal(baseline === "empty");
			expect(
				Object.prototype.hasOwnProperty.call(candidate, "children"),
			).to.equal(baseline === "empty");
			expect(candidate.parents ?? []).to.deep.equal([]);
			expect(candidate.children ?? []).to.deep.equal([]);
			await expect(handler.open(candidate)).to.be.rejectedWith(
				"rollback cleanup",
			);

			await expect(handler.stop()).to.be.rejectedWith(cleanupError.message);
			expect(
				Object.prototype.hasOwnProperty.call(candidate, "parents"),
			).to.equal(baseline === "empty");
			expect(
				Object.prototype.hasOwnProperty.call(candidate, "children"),
			).to.equal(baseline === "empty");
			expect(candidate.parents ?? []).to.deep.equal([]);
			expect(candidate.children ?? []).to.deep.equal([]);
			expect(() => handler.start()).to.throw("fully drained");

			await handler.stop();
			expect(candidate.closed).to.be.true;
			expect(forwardCounts).to.deep.equal([1, 1, 1]);
			handler.start();
			await handler.stop();
		});
	}

	it("does not manufacture a root lease after external rollback cleanup", async () => {
		const handler = new ProgramHandler({ client });
		const candidate = new TestProgram(531);
		delete (candidate as unknown as { parents?: unknown[] }).parents;
		delete (candidate as unknown as { children?: unknown[] }).children;
		const originalClose = candidate.close.bind(candidate);
		const cleanupError = new Error("synthetic external root cleanup");
		let calls = 0;
		candidate.open = async () => {
			throw new Error("synthetic external root open failure");
		};
		candidate.close = async (from) => {
			calls += 1;
			if (calls <= 2) throw cleanupError;
			return originalClose(from);
		};

		await expect(handler.open(candidate)).to.be.rejectedWith(
			"synthetic external root open failure",
		);
		await expect(handler.stop()).to.be.rejectedWith(cleanupError.message);
		expect(calls).to.equal(2);

		// Complete the retained monitored operation outside stop(). The next stop
		// must observe the closed, root-detached graph and avoid adding a fresh
		// undefined-parent lease merely to prove the cleanup again.
		expect(await candidate.close(undefined)).to.be.true;
		expect(calls).to.equal(3);
		expect(candidate.closed).to.be.true;
		expect(candidate.parents ?? []).to.deep.equal([]);

		await handler.stop();
		expect(calls).to.equal(3);
		handler.start();
		await handler.stop();
	});

	it("restores duplicate and distinct rollback-owner baselines after repeated pre-base failures", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestProgram(503);
		const other = new TestProgram(504);
		const sibling = new TestProgram(505);
		await Promise.all([
			owner.calculateAddress(),
			other.calculateAddress(),
			sibling.calculateAddress(),
		]);
		owner.closed = false;
		other.closed = false;
		const candidate = new TestProgram(506);
		candidate.parents = [owner, other, owner];
		candidate.children = [];
		owner.children = [candidate, sibling, candidate];
		other.children = [candidate];
		const parentBaseline = [...candidate.parents];
		const ownerChildrenBaseline = [...owner.children];
		const otherChildrenBaseline = [...other.children];
		const originalClose = candidate.close.bind(candidate);
		const cleanupError = new Error(
			"synthetic duplicate-owner pre-base cleanup",
		);
		const observed: Array<{ forward: number; inverse: number }> = [];
		let attempts = 0;
		candidate.open = async () => {
			throw new Error("synthetic duplicate-owner open failure");
		};
		candidate.close = async (from) => {
			if (from === owner) {
				observed.push({
					forward: candidate.parents.filter((parent) => parent === owner)
						.length,
					inverse: owner.children.filter((child) => child === candidate).length,
				});
			}
			attempts += 1;
			if (attempts <= 2) throw cleanupError;
			return originalClose(from);
		};

		await expect(handler.open(candidate, { parent: owner })).to.be.rejectedWith(
			"synthetic duplicate-owner open failure",
		);
		expect(candidate.parents).to.deep.equal(parentBaseline);
		expect(owner.children).to.deep.equal(ownerChildrenBaseline);
		expect(other.children).to.deep.equal(otherChildrenBaseline);

		await expect(handler.stop()).to.be.rejectedWith(cleanupError.message);
		expect(candidate.parents).to.deep.equal(parentBaseline);
		expect(owner.children).to.deep.equal(ownerChildrenBaseline);
		expect(other.children).to.deep.equal(otherChildrenBaseline);

		await handler.stop();
		expect(candidate.closed).to.be.true;
		expect(owner.children).to.deep.equal([sibling]);
		expect(other.children).to.deep.equal([]);
		expect(observed.slice(0, 3)).to.deep.equal([
			{ forward: 3, inverse: 3 },
			{ forward: 3, inverse: 3 },
			{ forward: 3, inverse: 3 },
		]);
	});

	it("keeps a no-progress false rollback pending but does not replay an exactly released lease", async () => {
		for (const makesProgress of [false, true]) {
			const handler = new ProgramHandler({ client });
			const owner = new TestProgram(makesProgress ? 507 : 508);
			await owner.calculateAddress();
			owner.closed = false;
			owner.children = [];
			const candidate = new TestProgram(makesProgress ? 509 : 510);
			candidate.parents = [owner];
			candidate.children = [];
			owner.children.push(candidate);
			const originalClose = candidate.close.bind(candidate);
			const forwardCounts: number[] = [];
			let calls = 0;
			candidate.open = async () => {
				throw new Error("synthetic false rollback open failure");
			};
			candidate.close = async (from) => {
				calls += 1;
				forwardCounts.push(
					candidate.parents.filter((parent) => parent === owner).length,
				);
				if (calls === 1 && !makesProgress) return false;
				return originalClose(from);
			};

			await expect(
				handler.open(candidate, { parent: owner }),
			).to.be.rejectedWith("synthetic false rollback open failure");
			expect(candidate.parents).to.deep.equal([owner]);
			expect(
				owner.children.filter((child) => child === candidate),
			).to.have.length(1);
			await expect(
				handler.open(candidate, { parent: owner }),
			).to.be.rejectedWith("rollback cleanup");

			await handler.stop();
			expect(candidate.closed).to.be.true;
			expect(forwardCounts[0]).to.equal(2);
			// A no-progress result borrows the generation once more. An exact false
			// release is already consumed, so stop starts from the one baseline owner.
			expect(forwardCounts[1]).to.equal(makesProgress ? 1 : 2);
			expect(
				owner.children.filter((child) => child === candidate),
			).to.have.length(0);
		}
	});

	it("retries a committed rollback without manufacturing its released owner", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestProgram(511);
		const other = new TestProgram(512);
		await Promise.all([owner.calculateAddress(), other.calculateAddress()]);
		owner.closed = false;
		other.closed = false;
		owner.children = [];
		other.children = [];
		const candidate = new TestProgram(513);
		candidate.parents = [owner, other];
		candidate.children = [];
		owner.children.push(candidate);
		other.children.push(candidate);
		const originalClose = candidate.close.bind(candidate);
		const cleanupError = new Error("synthetic rollback post-super cleanup");
		const forwardCounts: number[] = [];
		const inverseCounts: number[] = [];
		let calls = 0;
		candidate.open = async () => {
			throw new Error("synthetic committed rollback open failure");
		};
		candidate.close = async (from) => {
			if (from === owner) {
				forwardCounts.push(
					candidate.parents.filter((parent) => parent === owner).length,
				);
				inverseCounts.push(
					owner.children.filter((child) => child === candidate).length,
				);
			}
			const closed = await originalClose(from);
			calls += 1;
			if (calls === 1) throw cleanupError;
			return closed;
		};

		await expect(handler.open(candidate, { parent: owner })).to.be.rejectedWith(
			"synthetic committed rollback open failure",
		);
		expect(candidate.parents).to.deep.equal([owner, other]);
		expect(
			owner.children.filter((child) => child === candidate),
		).to.have.length(1);

		await handler.stop();
		expect(candidate.closed).to.be.true;
		// The failed base call saw baseline + generation. Its committed retry sees
		// only the restored baseline; a count of two here would spend it twice.
		expect(forwardCounts.slice(0, 2)).to.deep.equal([2, 1]);
		expect(inverseCounts.slice(0, 3)).to.deep.equal([2, 1, 1]);
		expect(
			owner.children.filter((child) => child === candidate),
		).to.have.length(0);
		expect(
			other.children.filter((child) => child === candidate),
		).to.have.length(0);
	});

	it("retries a private rollback owner before closing its monitored ancestor", async () => {
		const handler = new ProgramHandler({ client });
		const owner = await handler.open(new TestProgram(514));
		const candidate = new TestProgram(515);
		candidate.parents = [owner];
		candidate.children = [];
		owner.children.push(candidate);
		const originalClose = candidate.close.bind(candidate);
		const cleanupError = new Error(
			"synthetic monitored-owner pre-base cleanup",
		);
		let calls = 0;
		candidate.open = async () => {
			throw new Error("synthetic monitored-owner open failure");
		};
		candidate.close = async (from) => {
			calls += 1;
			if (calls === 1) throw cleanupError;
			return originalClose(from);
		};

		await expect(handler.open(candidate, { parent: owner })).to.be.rejectedWith(
			"synthetic monitored-owner open failure",
		);
		expect(candidate.parents).to.deep.equal([owner]);
		expect(
			owner.children.filter((child) => child === candidate),
		).to.have.length(1);

		await handler.stop();
		expect(candidate.closed).to.be.true;
		expect(owner.closed).to.be.true;
		expect(calls).to.equal(3);
	});

	it("does not resurrect rollback edges already consumed by ancestor cleanup", async () => {
		const handler = new ProgramHandler({ client });
		const owner = await handler.open(new TestProgram(525));
		const candidate = new TestProgram(526);
		candidate.parents = [owner];
		candidate.children = [];
		owner.children.push(candidate);
		const originalClose = candidate.close.bind(candidate);
		const cleanupError = new Error(
			"synthetic ancestor-consumed pre-base cleanup",
		);
		let calls = 0;
		candidate.open = async () => {
			throw new Error("synthetic ancestor-consumed open failure");
		};
		candidate.close = async (from) => {
			calls += 1;
			if (calls <= 2) throw cleanupError;
			return originalClose(from);
		};

		await expect(handler.open(candidate, { parent: owner })).to.be.rejectedWith(
			"synthetic ancestor-consumed open failure",
		);
		await expect(handler.stop()).to.be.rejectedWith(cleanupError.message);

		// The private-owner retry failed, but the same stop closed the monitored
		// ancestor. Its baseline edge then closed and detached the candidate.
		expect(calls).to.equal(3);
		expect(candidate.closed).to.be.true;
		expect(owner.closed).to.be.true;
		expect(candidate.parents).to.deep.equal([]);
		expect(owner.children).to.deep.equal([]);
		const parentsAfterAncestorCleanup = candidate.parents;
		const childrenAfterAncestorCleanup = owner.children;

		await handler.stop();
		expect(calls).to.equal(3);
		expect(candidate.parents).to.equal(parentsAfterAncestorCleanup);
		expect(owner.children).to.equal(childrenAfterAncestorCleanup);
		expect(candidate.parents).to.deep.equal([]);
		expect(owner.children).to.deep.equal([]);
		handler.start();
		await handler.stop();
	});

	it("keeps a closed rollback child quarantined while its owner edges remain", async () => {
		const handler = new ProgramHandler({ client });
		const owner = await handler.open(new TestProgram(527));
		const candidate = new TestProgram(528);
		candidate.parents = [owner];
		candidate.children = [];
		owner.children.push(candidate);
		const originalClose = candidate.close.bind(candidate);
		const cleanupError = new Error("synthetic retained-owner pre-base cleanup");
		let calls = 0;
		candidate.open = async () => {
			throw new Error("synthetic retained-owner open failure");
		};
		candidate.close = async () => {
			calls += 1;
			if (calls <= 2) throw cleanupError;
			// A hostile/replaced operation must not be accepted merely because it
			// reports terminal success. It deliberately retains both graph edges.
			candidate.closed = true;
			return true;
		};

		await expect(handler.open(candidate, { parent: owner })).to.be.rejectedWith(
			"synthetic retained-owner open failure",
		);
		await expect(handler.stop()).to.be.rejectedWith(cleanupError.message);
		expect(calls).to.equal(3);
		expect(candidate.closed).to.be.true;
		expect(candidate.parents).to.deep.equal([owner]);
		expect(owner.children).to.include(candidate);

		try {
			await expect(handler.stop()).to.be.rejectedWith(
				"without reaching its base terminal operation",
			);
			// The direct residual retry and the still-live owner's graph drain both
			// reject the forged terminal result without releasing either edge.
			expect(calls).to.equal(5);
			expect(candidate.parents).to.deep.equal([owner]);
			expect(owner.children).to.include(candidate);
			expect(() => handler.start()).to.throw("fully drained");
		} finally {
			candidate.close = originalClose;
			candidate.closed = false;
			await handler.stop();
		}
		expect(candidate.parents).to.deep.equal([]);
		expect(owner.children).not.to.include(candidate);
		handler.start();
		await handler.stop();
	});

	it("reconciles every baseline owner after a hostile terminal rollback close", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestNestedProgram(532);
		const other = new TestNestedProgram(533);
		const candidate = new TestNestedProgram(534);
		await Promise.all([
			owner.calculateAddress(),
			other.calculateAddress(),
			candidate.calculateAddress(),
		]);
		owner.closed = false;
		other.closed = false;
		candidate.parents = [owner, other];
		candidate.children = [];
		owner.children = [candidate];
		other.children = [candidate];
		const originalClose = candidate.close.bind(candidate);
		let calls = 0;
		candidate.open = async () => {
			throw new Error("synthetic multi-owner rollback failure");
		};
		candidate.close = async (from) => {
			calls += 1;
			if (calls === 1) {
				// Release exactly the Handler-added owner occurrence. The two baseline
				// owners remain live and are restored for stop().
				return originalClose(from);
			}
			// A hostile replacement reports terminal success after erasing every
			// forward edge, but deliberately leaves every inverse owner occurrence.
			candidate.parents.splice(0, candidate.parents.length);
			candidate.closed = true;
			return true;
		};

		await expect(handler.open(candidate, { parent: owner })).to.be.rejectedWith(
			"synthetic multi-owner rollback failure",
		);
		expect(calls).to.equal(1);
		expect(candidate.parents).to.deep.equal([owner, other]);
		expect(owner.children).to.deep.equal([candidate]);
		expect(other.children).to.deep.equal([candidate]);

		await expect(handler.stop()).to.be.rejectedWith(
			"without reaching its base terminal operation",
		);
		expect(calls).to.equal(2);
		expect(candidate.closed).to.be.true;
		expect(candidate.parents).to.deep.equal([owner, other]);
		expect(owner.children).to.deep.equal([candidate]);
		expect(other.children).to.deep.equal([candidate]);
		expect(() => handler.start()).to.throw("fully drained");

		candidate.close = originalClose;
		candidate.closed = false;
		await handler.stop();
		expect(candidate.parents).to.deep.equal([]);
		expect(owner.children).to.deep.equal([]);
		expect(other.children).to.deep.equal([]);
		handler.start();
		await handler.stop();
	});

	it("records a transient owner attached inside a pending rollback retry", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestNestedProgram(9101);
		const transientOwner = new TestNestedProgram(9102);
		const candidate = new TestNestedProgram(9103);
		await Promise.all([
			owner.calculateAddress(),
			transientOwner.calculateAddress(),
			candidate.calculateAddress(),
		]);
		owner.closed = false;
		transientOwner.closed = false;
		candidate.parents = [owner];
		candidate.children = [];
		owner.children = [candidate];
		transientOwner.children = [];
		const originalClose = candidate.close.bind(candidate);
		let calls = 0;
		candidate.open = async () => {
			throw new Error("synthetic transient-owner open failure");
		};
		candidate.close = async (from) => {
			calls += 1;
			if (calls === 1) {
				throw new Error("synthetic transient-owner pre-base cleanup failure");
			}
			if (calls === 2) {
				// The retry has already captured its pre-call owners. Attach another
				// owner before the retry finally restores the baseline forward shape.
				candidate.parents.push(transientOwner);
				transientOwner.children.push(candidate);
			}
			return originalClose(from);
		};

		await expect(handler.open(candidate, { parent: owner })).to.be.rejectedWith(
			"synthetic transient-owner open failure",
		);
		await handler.stop();
		expect(calls).to.equal(3);
		expect(candidate.closed).to.be.true;
		expect(candidate.parents).to.deep.equal([]);
		expect(owner.children).to.deep.equal([]);
		expect(transientOwner.children).to.deep.equal([]);
		handler.start();
		await handler.stop();
	});

	it("records a transient owner attached inside the initial rollback close", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestNestedProgram(9301);
		const transientOwner = new TestNestedProgram(9302);
		const candidate = new TestNestedProgram(9303);
		await Promise.all([
			owner.calculateAddress(),
			transientOwner.calculateAddress(),
			candidate.calculateAddress(),
		]);
		owner.closed = false;
		transientOwner.closed = false;
		candidate.parents = [owner];
		candidate.children = [];
		owner.children = [candidate];
		transientOwner.children = [];
		const originalClose = candidate.close.bind(candidate);
		let calls = 0;
		candidate.open = async () => {
			throw new Error("synthetic initial-owner open failure");
		};
		candidate.close = async (from) => {
			calls += 1;
			if (calls === 1) {
				candidate.parents.push(transientOwner);
				transientOwner.children.push(candidate);
			}
			return originalClose(from);
		};

		await expect(handler.open(candidate, { parent: owner })).to.be.rejectedWith(
			"synthetic initial-owner open failure",
		);
		expect(candidate.parents).to.deep.equal([owner]);
		expect(transientOwner.children).to.deep.equal([]);
		await handler.stop();
		expect(calls).to.equal(2);
		expect(candidate.closed).to.be.true;
		expect(candidate.parents).to.deep.equal([]);
		expect(owner.children).to.deep.equal([]);
		expect(transientOwner.children).to.deep.equal([]);
		handler.start();
		await handler.stop();
	});

	it("records a transient owner attached while nested rollback cleanup awaits", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestProgram(9401);
		const transientOwner = new TestProgram(9402);
		const candidate = new TestProgram(9403);
		await Promise.all([
			owner.calculateAddress(),
			transientOwner.calculateAddress(),
			candidate.calculateAddress(),
		]);
		owner.closed = false;
		transientOwner.closed = false;
		candidate.parents = [owner];
		candidate.children = [];
		owner.children = [candidate];
		transientOwner.children = [];
		let markChildClose!: () => void;
		const childCloseStarted = new Promise<void>((resolve) => {
			markChildClose = resolve;
		});
		let releaseChildClose!: () => void;
		const childCloseGate = new Promise<void>((resolve) => {
			releaseChildClose = resolve;
		});
		const originalChildClose = candidate.nested.close.bind(candidate.nested);
		let childCloseCalls = 0;
		candidate.nested.close = async (from) => {
			childCloseCalls += 1;
			markChildClose();
			await childCloseGate;
			return originalChildClose(from);
		};
		candidate.open = async () => {
			throw new Error("synthetic async-gap open failure");
		};

		const opening = handler.open(candidate, { parent: owner });
		await childCloseStarted;
		candidate.parents.push(transientOwner);
		transientOwner.children.push(candidate);
		releaseChildClose();
		await expect(opening).to.be.rejectedWith(
			"synthetic async-gap open failure",
		);
		await handler.stop();
		expect(childCloseCalls).to.equal(1);
		expect(candidate.closed).to.be.true;
		expect(candidate.parents).to.deep.equal([]);
		expect(owner.children).to.deep.equal([]);
		expect(transientOwner.children).to.deep.equal([]);
		handler.start();
		await handler.stop();
	});

	it("reconciles transient owner edges when nested beforeOpen fails while closed", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestProgram(9431);
		const transientOwner = new TestProgram(9432);
		const candidate = new TestProgram(9433);
		await Promise.all([
			owner.calculateAddress(),
			transientOwner.calculateAddress(),
			candidate.calculateAddress(),
		]);
		owner.closed = false;
		transientOwner.closed = false;
		owner.children = [];
		transientOwner.children = [];
		candidate.parents = [];
		candidate.children = [];
		let markNested!: () => void;
		const nestedStarted = new Promise<void>((resolve) => {
			markNested = resolve;
		});
		let releaseNested!: () => void;
		const nestedGate = new Promise<void>((resolve) => {
			releaseNested = resolve;
		});
		candidate.nested.beforeOpen = async () => {
			markNested();
			await nestedGate;
			throw new Error("synthetic pre-open nested failure");
		};

		const opening = handler.open(candidate, { parent: owner });
		await nestedStarted;
		candidate.parents.push(transientOwner);
		transientOwner.children.push(candidate);
		releaseNested();
		await expect(opening).to.be.rejectedWith(
			"synthetic pre-open nested failure",
		);
		expect(candidate.closed).to.be.true;
		expect(candidate.parents).to.deep.equal([]);
		expect(owner.children).to.deep.equal([]);
		expect(transientOwner.children).to.deep.equal([]);
		await handler.stop();
		handler.start();
		await handler.stop();
	});

	it("cleans a child attached inside the initial rollback close", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestNestedProgram(9451);
		const candidate = new TestNestedProgram(9452);
		const lateChild = new TestNestedProgram(9453);
		await Promise.all([
			owner.calculateAddress(),
			candidate.calculateAddress(),
			lateChild.calculateAddress(),
		]);
		owner.closed = false;
		owner.children = [candidate];
		candidate.parents = [owner];
		candidate.children = [];
		lateChild.closed = false;
		lateChild.parents = [];
		lateChild.children = [];
		let lateCloseCalls = 0;
		lateChild.close = async (from) => {
			lateCloseCalls += 1;
			const index = lateChild.parents.indexOf(from);
			if (index >= 0) lateChild.parents.splice(index, 1);
			lateChild.closed = lateChild.parents.length === 0;
			return lateChild.closed;
		};
		const originalClose = candidate.close.bind(candidate);
		let calls = 0;
		candidate.open = async () => {
			throw new Error("synthetic late-child open failure");
		};
		candidate.close = async (from) => {
			calls += 1;
			if (calls === 1) {
				candidate.children.push(lateChild);
				lateChild.parents.push(candidate);
			}
			return originalClose(from);
		};

		await expect(handler.open(candidate, { parent: owner })).to.be.rejectedWith(
			"synthetic late-child open failure",
		);
		await handler.stop();
		expect(lateCloseCalls).to.equal(1);
		expect(lateChild.closed).to.be.true;
		expect(lateChild.parents).to.deep.equal([]);
		expect(candidate.children).to.deep.equal([]);
		handler.start();
		await handler.stop();
	});

	it("does not resurrect a baseline child removed during failed open", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestNestedProgram(9461);
		const candidate = new TestNestedProgram(9462);
		const sibling = new TestNestedProgram(9463);
		await Promise.all([
			owner.calculateAddress(),
			candidate.calculateAddress(),
			sibling.calculateAddress(),
		]);
		owner.closed = false;
		owner.children = [];
		candidate.parents = [];
		candidate.children = [sibling];
		sibling.closed = false;
		sibling.parents = [candidate];
		sibling.children = [];
		let siblingCloseCalls = 0;
		sibling.close = async (from) => {
			siblingCloseCalls += 1;
			const parentIndex = sibling.parents.indexOf(from);
			if (parentIndex !== -1) sibling.parents.splice(parentIndex, 1);
			const childIndex = from!.children.indexOf(sibling);
			if (childIndex !== -1) from!.children.splice(childIndex, 1);
			sibling.closed = sibling.parents.length === 0;
			return sibling.closed;
		};
		let markOpen!: () => void;
		const openStarted = new Promise<void>((resolve) => {
			markOpen = resolve;
		});
		let releaseOpen!: () => void;
		const openGate = new Promise<void>((resolve) => {
			releaseOpen = resolve;
		});
		candidate.open = async () => {
			markOpen();
			await openGate;
			throw new Error("synthetic child-removal open failure");
		};

		const opening = handler.open(candidate, { parent: owner });
		await openStarted;
		expect(await sibling.close(candidate)).to.be.true;
		releaseOpen();
		await expect(opening).to.be.rejectedWith(
			"synthetic child-removal open failure",
		);
		expect(siblingCloseCalls).to.equal(1);
		expect(sibling.closed).to.be.true;
		expect(sibling.parents).to.deep.equal([]);
		expect(candidate.children).not.to.include(sibling);
		await handler.stop();
		expect(candidate.children).not.to.include(sibling);
		handler.start();
		await handler.stop();
	});

	it("cleans a forward-only structural child hidden during rollback close", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestProgram(9471);
		const candidate = new TestProgram(9472);
		await Promise.all([owner.calculateAddress(), candidate.calculateAddress()]);
		owner.closed = false;
		owner.children = [];
		candidate.parents = [];
		candidate.children = [];
		const child = candidate.nested;
		const childClose = child.close.bind(child);
		let childCloseCalls = 0;
		child.close = async (from) => {
			childCloseCalls += 1;
			return childClose(from);
		};
		const candidateClose = candidate.close.bind(candidate);
		let candidateCloseCalls = 0;
		candidate.open = async () => {
			throw new Error("synthetic hidden rollback-child failure");
		};
		candidate.close = async (from) => {
			candidateCloseCalls += 1;
			if (candidateCloseCalls === 1) {
				const inverseIndex = candidate.children.indexOf(child);
				if (inverseIndex !== -1) candidate.children.splice(inverseIndex, 1);
			}
			return candidateClose(from);
		};

		await expect(handler.open(candidate, { parent: owner })).to.be.rejectedWith(
			"synthetic hidden rollback-child failure",
		);
		expect(childCloseCalls).to.equal(1);
		expect(child.closed).to.be.true;
		expect(child.parents).to.deep.equal([]);
		expect(candidate.children).to.deep.equal([]);
		await handler.stop();
		handler.start();
		await handler.stop();
	});

	it("cleans a forward-only structural child hidden before rollback snapshot", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestProgram(9481);
		const candidate = new TestProgram(9482);
		await Promise.all([owner.calculateAddress(), candidate.calculateAddress()]);
		owner.closed = false;
		owner.children = [];
		candidate.parents = [];
		candidate.children = [];
		const child = candidate.nested;
		const childClose = child.close.bind(child);
		let childCloseCalls = 0;
		child.close = async (from) => {
			childCloseCalls += 1;
			return childClose(from);
		};
		candidate.open = async () => {
			const inverseIndex = candidate.children.indexOf(child);
			if (inverseIndex !== -1) candidate.children.splice(inverseIndex, 1);
			throw new Error("synthetic pre-snapshot hidden-child failure");
		};

		await expect(handler.open(candidate, { parent: owner })).to.be.rejectedWith(
			"synthetic pre-snapshot hidden-child failure",
		);
		expect(childCloseCalls).to.equal(1);
		expect(child.closed).to.be.true;
		expect(child.parents).to.deep.equal([]);
		expect(candidate.children).to.deep.equal([]);
		await handler.stop();
		handler.start();
		await handler.stop();
	});

	it("retains a live orphan when extra-child cleanup over-releases its baseline", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestNestedProgram(9491);
		const candidate = new TestNestedProgram(9492);
		const child = new TestNestedProgram(9493);
		await Promise.all([
			owner.calculateAddress(),
			candidate.calculateAddress(),
			child.calculateAddress(),
		]);
		owner.closed = false;
		owner.children = [];
		candidate.parents = [];
		candidate.children = [child];
		child.closed = false;
		child.parents = [candidate];
		child.children = [];
		let childCloseCalls = 0;
		child.close = async (from) => {
			childCloseCalls += 1;
			if (childCloseCalls === 1) {
				child.parents = child.parents.filter((parent) => parent !== from);
				return false;
			}
			child.closed = true;
			return true;
		};
		candidate.open = async () => {
			candidate.children.push(child);
			child.parents.push(candidate);
			throw new Error("synthetic over-release child failure");
		};
		const candidateClose = candidate.close.bind(candidate);
		candidate.close = async (from) => {
			if (from !== owner) return candidateClose(from);
			candidate.parents.push(from);
			from?.children.push(candidate);
			const result = await candidateClose(from);
			expect(result).to.be.false;
			candidate.parents = candidate.parents.filter((parent) => parent !== from);
			const inverseIndex = from?.children.indexOf(candidate) ?? -1;
			if (inverseIndex !== -1) from!.children.splice(inverseIndex, 1);
			return result;
		};

		await expect(handler.open(candidate, { parent: owner })).to.be.rejectedWith(
			"synthetic over-release child failure",
		);
		expect(childCloseCalls).to.equal(1);
		expect(child.closed).to.be.false;
		expect(child.parents).to.deep.equal([]);
		expect(candidate.children).to.deep.equal([]);
		await handler.stop();
		expect(childCloseCalls).to.equal(2);
		expect(child.closed).to.be.true;
		handler.start();
		await handler.stop();
	});

	it("cleans a child attached after a pending rollback retry terminally closes", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestNestedProgram(9521);
		const candidate = new TestNestedProgram(9522);
		const lateChild = new TestNestedProgram(9523);
		await Promise.all([
			owner.calculateAddress(),
			candidate.calculateAddress(),
			lateChild.calculateAddress(),
		]);
		owner.closed = false;
		owner.children = [];
		candidate.parents = [];
		candidate.children = [];
		lateChild.closed = false;
		lateChild.parents = [];
		lateChild.children = [];
		let lateCloseCalls = 0;
		lateChild.close = async (from) => {
			lateCloseCalls += 1;
			const parentIndex = lateChild.parents.indexOf(from);
			if (parentIndex !== -1) lateChild.parents.splice(parentIndex, 1);
			lateChild.closed = lateChild.parents.length === 0;
			return lateChild.closed;
		};
		const baseClose = candidate.close.bind(candidate);
		let calls = 0;
		candidate.open = async () => {
			throw new Error("synthetic retry late-child open failure");
		};
		candidate.close = async (from) => {
			calls += 1;
			if (calls === 1) throw new Error("synthetic retry pre-base failure");
			const result = await baseClose(from);
			if (calls === 2) {
				candidate.children.push(lateChild);
				lateChild.parents.push(candidate);
			}
			return result;
		};

		await expect(handler.open(candidate, { parent: owner })).to.be.rejectedWith(
			"synthetic retry late-child open failure",
		);
		await handler.stop();
		expect(lateCloseCalls).to.equal(1);
		expect(lateChild.closed).to.be.true;
		expect(lateChild.parents).to.deep.equal([]);
		expect(candidate.children).to.deep.equal([]);
		handler.start();
		await handler.stop();
	});

	it("drains owner and child edges attached after an ordinary terminal close", async () => {
		const handler = new ProgramHandler({ client });
		const candidate = new TestNestedProgram(9531);
		const lateOwner = new TestNestedProgram(9532);
		const lateChild = new TestNestedProgram(9533);
		await Promise.all([
			candidate.calculateAddress(),
			lateOwner.calculateAddress(),
			lateChild.calculateAddress(),
		]);
		lateOwner.closed = false;
		lateOwner.children = [];
		lateChild.closed = false;
		lateChild.parents = [];
		lateChild.children = [];
		let lateCloseCalls = 0;
		lateChild.close = async (from) => {
			lateCloseCalls += 1;
			const parentIndex = lateChild.parents.indexOf(from);
			if (parentIndex !== -1) lateChild.parents.splice(parentIndex, 1);
			lateChild.closed = lateChild.parents.length === 0;
			return lateChild.closed;
		};
		const baseClose = candidate.close.bind(candidate);
		candidate.close = async (from) => {
			const result = await baseClose(from);
			(candidate.parents ??= []).push(lateOwner);
			lateOwner.children.push(candidate);
			(candidate.children ??= []).push(lateChild);
			lateChild.parents.push(candidate);
			return result;
		};

		await handler.open(candidate);
		await handler.stop();
		expect(candidate.closed).to.be.true;
		expect(candidate.parents).to.deep.equal([]);
		expect(candidate.children).to.deep.equal([]);
		expect(lateOwner.children).to.deep.equal([]);
		expect(lateCloseCalls).to.equal(1);
		expect(lateChild.closed).to.be.true;
		expect(lateChild.parents).to.deep.equal([]);
		handler.start();
		await handler.stop();
	});

	it("quarantines a terminal override that never reaches the base close", async () => {
		const handler = new ProgramHandler({ client });
		const candidate = new TestNestedProgram(9541);
		const baseClose = candidate.close.bind(candidate);
		const pubsub = client.services.pubsub;
		const unsubscribe = pubsub.unsubscribe.bind(pubsub);
		let unsubscribeCalls = 0;
		let onCloseCalls = 0;
		pubsub.unsubscribe = (...args) => {
			unsubscribeCalls += 1;
			return unsubscribe(...args);
		};
		await handler.open(candidate, {
			onClose: () => {
				onCloseCalls += 1;
			},
		});
		let fakeCloseCalls = 0;
		candidate.close = async () => {
			fakeCloseCalls += 1;
			candidate.closed = true;
			return true;
		};

		try {
			await expect(handler.stop()).to.be.rejectedWith(
				"without reaching its base terminal operation",
			);
			expect(fakeCloseCalls).to.equal(1);
			expect(unsubscribeCalls).to.equal(0);
			expect(onCloseCalls).to.equal(0);
			expect(candidate.parents).to.deep.equal([undefined]);
			expect(() => handler.start()).to.throw("fully drained");

			candidate.close = baseClose;
			candidate.closed = false;
			await handler.stop();
			expect(unsubscribeCalls).to.equal(1);
			expect(onCloseCalls).to.equal(1);
			handler.start();
			await handler.stop();
		} finally {
			pubsub.unsubscribe = unsubscribe;
		}
	});

	it("does not treat an already-closed base fast path as terminal proof", async () => {
		const handler = new ProgramHandler({ client });
		const candidate = new TestNestedProgram(9542);
		const baseClose = candidate.close.bind(candidate);
		const pubsub = client.services.pubsub;
		const unsubscribe = pubsub.unsubscribe.bind(pubsub);
		let unsubscribeCalls = 0;
		let onCloseCalls = 0;
		pubsub.unsubscribe = (...args) => {
			unsubscribeCalls += 1;
			return unsubscribe(...args);
		};
		await handler.open(candidate, {
			onClose: () => {
				onCloseCalls += 1;
			},
		});
		candidate.close = async (from) => {
			candidate.closed = true;
			return Program.prototype.close.call(candidate, from);
		};

		try {
			await expect(handler.stop()).to.be.rejectedWith(
				"without reaching its base terminal operation",
			);
			expect(unsubscribeCalls).to.equal(0);
			expect(onCloseCalls).to.equal(0);
			expect(candidate.parents).to.deep.equal([undefined]);
			expect(() => handler.start()).to.throw("fully drained");

			candidate.close = baseClose;
			candidate.closed = false;
			await handler.stop();
			expect(unsubscribeCalls).to.equal(1);
			expect(onCloseCalls).to.equal(1);
			handler.start();
			await handler.stop();
		} finally {
			pubsub.unsubscribe = unsubscribe;
		}
	});

	it("does not trust runtime-visible Handler terminal-proof shadows", async () => {
		const handler = new ProgramHandler({ client });
		const candidate = new TestNestedProgram(9555);
		const baseClose = candidate.close.bind(candidate);
		let forgedCalls = 0;
		let onCloseCalls = 0;
		await handler.open(candidate, {
			onClose: () => {
				onCloseCalls += 1;
			},
		});
		const forgedProtocol = {
			supports: () => {
				forgedCalls += 1;
				return false;
			},
			closed: () => {
				forgedCalls += 1;
				return true;
			},
			checkpoint: () => {
				forgedCalls += 1;
				return 0;
			},
			commit: () => {
				forgedCalls += 1;
				return {
					epoch: 0,
					version: 1,
					type: "close" as const,
					result: true,
					releasedParentReferences: 1,
				};
			},
			retry: async () => {
				forgedCalls += 1;
				return true;
			},
			retainCleanup: () => {
				forgedCalls += 1;
				return {};
			},
			releaseCleanup: () => {
				forgedCalls += 1;
			},
		};
		Object.assign(handler.properties as unknown as Record<string, unknown>, {
			terminalProtocol: forgedProtocol,
		});
		Object.assign(handler as unknown as Record<string, unknown>, {
			_terminalProtocolSupport: new WeakMap([[candidate, false]]),
			supportsTerminalBaseCommitProof: () => false,
			terminalCheckpoint: () => 0,
			terminalCommit: forgedProtocol.commit,
			runTerminalOperation: async () => {
				forgedCalls += 1;
				return true;
			},
		});
		candidate.close = async () => {
			candidate.closed = true;
			return true;
		};

		await expect(handler.stop()).to.be.rejectedWith(
			"without reaching its base terminal operation",
		);
		expect(forgedCalls).to.equal(0);
		expect(onCloseCalls).to.equal(0);
		expect(candidate.parents).to.deep.equal([undefined]);
		expect(() => handler.start()).to.throw("fully drained");

		candidate.close = baseClose;
		candidate.closed = false;
		await handler.stop();
		expect(onCloseCalls).to.equal(1);
		handler.start();
		await handler.stop();
	});

	it("does not expose a runtime terminal-progress or commit recorder", async () => {
		const handler = new ProgramHandler({ client });
		const candidate = new TestNestedProgram(9562);
		const baseClose = candidate.close.bind(candidate);
		let onCloseCalls = 0;
		await handler.open(candidate, {
			onClose: () => {
				onCloseCalls += 1;
			},
		});
		candidate.close = async (from) => {
			const runtime = candidate as unknown as Record<string, unknown>;
			const recordCommit = runtime.recordTerminalBaseCommit as
				| ((...args: unknown[]) => void)
				| undefined;
			const recordProgress = runtime.markTerminalBaseProgress as
				| ((...args: unknown[]) => void)
				| undefined;
			recordProgress?.("close", from, true, 1);
			recordCommit?.("close", from, true, 1);
			candidate.closed = true;
			return true;
		};

		await expect(handler.stop()).to.be.rejectedWith(
			"without reaching its base terminal operation",
		);
		expect(onCloseCalls).to.equal(0);
		expect(candidate.parents).to.deep.equal([undefined]);
		expect(() => handler.start()).to.throw("fully drained");

		candidate.close = baseClose;
		candidate.closed = false;
		await handler.stop();
		expect(onCloseCalls).to.equal(1);
		handler.start();
		await handler.stop();
	});

	it("uses captured Program terminal intrinsics instead of runtime-private shadows", async () => {
		const handler = new ProgramHandler({ client });
		const candidate = new TestNestedProgram(9556);
		let shadowCalls = 0;
		let publicClosedReads = 0;
		let onCloseCalls = 0;
		await handler.open(candidate, {
			onClose: () => {
				onCloseCalls += 1;
			},
		});
		const shadow = () => {
			shadowCalls += 1;
			return Promise.resolve(true);
		};
		Object.assign(candidate as unknown as Record<string, unknown>, {
			recordTerminalBaseCommit: shadow,
			performTerminalOperation: shadow,
			terminalOperation: shadow,
			end: shadow,
			processEnd: shadow,
			finishTerminalTail: shadow,
			consumeTerminalRetry: shadow,
			performDrop: shadow,
			_pendingTerminalTail: {
				type: "close",
				callbackCompleted: true,
				eventEmitted: true,
				hadParentReference: false,
			},
			_dropDeletePending: true,
		});
		Object.defineProperty(candidate, "closed", {
			configurable: true,
			get: () => {
				publicClosedReads += 1;
				return true;
			},
			set: () => {},
		});
		candidate.close = async (from) =>
			Program.prototype.close.call(candidate, from);

		await handler.stop();
		expect(shadowCalls).to.equal(0);
		expect(publicClosedReads).to.equal(0);
		expect(onCloseCalls).to.equal(1);
		delete (candidate as unknown as Record<string, unknown>).closed;
		expect(candidate.closed).to.be.true;
		handler.start();
		await handler.stop();
	});

	it("does not trust forged drop-resume state or runtime drop shadows", async () => {
		const handler = new ProgramHandler({ client });
		const candidate = new TestNestedProgram(9563);
		let attacking = true;
		let shadowCalls = 0;
		let onDropCalls = 0;
		candidate.drop = async (from) => {
			if (attacking) {
				Object.assign(candidate as unknown as Record<string, unknown>, {
					_dropDeletePending: true,
					performDrop: () => {
						shadowCalls += 1;
						return Promise.resolve(true);
					},
					end: () => {
						shadowCalls += 1;
						return Promise.resolve(true);
					},
					delete: () => {
						shadowCalls += 1;
						return Promise.resolve();
					},
				});
				candidate.closed = true;
			}
			return Program.prototype.drop.call(candidate, from);
		};
		await handler.open(candidate, {
			onDrop: () => {
				onDropCalls += 1;
			},
		});

		await expect(candidate.drop()).to.be.rejectedWith(
			"Program is closed, can not drop",
		);
		expect(shadowCalls).to.equal(0);
		expect(onDropCalls).to.equal(0);
		expect(() => handler.start()).to.not.throw();

		attacking = false;
		candidate.closed = false;
		expect(await candidate.drop()).to.be.true;
		expect(shadowCalls).to.equal(0);
		expect(onDropCalls).to.equal(1);
		await handler.stop();
		handler.start();
		await handler.stop();
	});

	it("does not mint proof from forged pending state or ownership accessors", async () => {
		const handler = new ProgramHandler({ client });
		const candidate = new TestNestedProgram(9557);
		const baseClose = candidate.close.bind(candidate);
		let onCloseCalls = 0;
		await handler.open(candidate, {
			onClose: () => {
				onCloseCalls += 1;
			},
		});
		const parents = candidate.parents;
		const originalFindIndex = parents.findIndex;
		candidate.close = async (from) => {
			Object.assign(candidate as unknown as Record<string, unknown>, {
				_pendingTerminalTail: {
					type: "close",
					from,
					callbackCompleted: true,
					eventEmitted: true,
					hadParentReference: false,
				},
				_dropDeletePending: true,
			});
			parents.findIndex = () => {
				candidate.closed = true;
				return -1;
			};
			return Program.prototype.close.call(candidate, from);
		};

		await expect(handler.stop()).to.be.rejectedWith(
			"without reaching its base terminal operation",
		);
		expect(onCloseCalls).to.equal(0);
		expect(candidate.parents).to.deep.equal([undefined]);
		expect(() => handler.start()).to.throw("fully drained");

		parents.findIndex = originalFindIndex;
		candidate.close = baseClose;
		candidate.closed = false;
		await handler.stop();
		expect(onCloseCalls).to.equal(1);
		handler.start();
		await handler.stop();
	});

	it("freezes terminal commits before an outer override can mutate ownership proof", async () => {
		const handler = new ProgramHandler({ client });
		const firstOwner = await handler.open(new TestNestedProgram(9558));
		const secondOwner = await handler.open(new TestNestedProgram(9559));
		const candidate = new TestNestedProgram(9560);
		await handler.open(candidate, { parent: firstOwner });
		await handler.open(candidate, { parent: firstOwner, existing: "reuse" });
		await handler.open(candidate, { parent: secondOwner, existing: "reuse" });
		const baseClose = candidate.close.bind(candidate);
		const checkpoint = Symbol.for("@peerbit/program/terminal-base-checkpoint");
		const commit = Symbol.for("@peerbit/program/terminal-base-commit");
		let mutationRejected = false;
		candidate.close = async (from) => {
			const before = (candidate as unknown as Record<symbol, () => number>)[
				checkpoint
			]();
			const result = await Program.prototype.close.call(candidate, from);
			const proof = (
				candidate as unknown as Record<
					symbol,
					(
						afterVersion: number,
						type: "close",
						owner?: Program,
					) => { releasedParentReferences: number } | undefined
				>
			)[commit](before, "close", from);
			expect(proof).to.exist;
			expect(Object.isFrozen(proof)).to.be.true;
			try {
				proof!.releasedParentReferences = 99;
			} catch (error) {
				mutationRejected = error instanceof TypeError;
			}
			return result;
		};

		expect(await candidate.close(firstOwner)).to.be.false;
		expect(mutationRejected).to.be.true;
		expect(
			candidate.parents.filter((owner) => owner === firstOwner),
		).to.have.length(1);
		expect(
			firstOwner.children.filter((child) => child === candidate),
		).to.have.length(1);
		expect(secondOwner.children).to.include(candidate);

		candidate.close = baseClose;
		await handler.stop();
		handler.start();
		await handler.stop();
	});

	it("closes and drops borsh-created clones through captured terminal intrinsics", async () => {
		const handler = new ProgramHandler({ client });
		const source = new TestNestedProgram(9561);
		const closingClone = source.clone();
		await handler.open(closingClone);
		expect(await closingClone.close()).to.be.true;
		expect(closingClone.closed).to.be.true;

		const droppingClone = source.clone();
		await handler.open(droppingClone);
		expect(await droppingClone.drop()).to.be.true;
		expect(droppingClone.closed).to.be.true;
		await handler.stop();
		handler.start();
		await handler.stop();
	});

	it("does not skip a live child through forged closed or pending getters", async () => {
		const handler = new ProgramHandler({ client });
		const parent = await handler.open(new TestProgram(9564));
		const child = parent.nested;
		let closedReads = 0;
		let pendingReads = 0;
		Object.defineProperties(child, {
			closed: {
				configurable: true,
				get: () => {
					closedReads += 1;
					return true;
				},
				set: () => {},
			},
			pendingTerminalOperation: {
				configurable: true,
				get: () => {
					pendingReads += 1;
					return undefined;
				},
			},
		});

		await handler.stop();
		expect(closedReads).to.equal(0);
		expect(pendingReads).to.equal(0);
		delete (child as unknown as Record<string, unknown>).closed;
		delete (child as unknown as Record<string, unknown>)
			.pendingTerminalOperation;
		expect(child.closed).to.be.true;
		handler.start();
		await handler.stop();
	});

	for (const shadowMode of [
		"disabled",
		"forged",
		"subclass-prototype-forged",
		"prototype-replaced",
		"base-prototype-poisoned",
	] as const) {
		it(`ignores ${shadowMode} instance terminal-proof symbols`, async () => {
			const handler = new ProgramHandler({ client });
			const candidate = new TestNestedProgram(
				shadowMode === "disabled"
					? 9547
					: shadowMode === "forged"
						? 9548
						: shadowMode === "subclass-prototype-forged"
							? 9550
							: shadowMode === "prototype-replaced"
								? 9551
								: 9552,
			);
			const baseClose = candidate.close.bind(candidate);
			const pubsub = client.services.pubsub;
			const unsubscribe = pubsub.unsubscribe.bind(pubsub);
			let unsubscribeCalls = 0;
			let onCloseCalls = 0;
			let forgedProtocolCalls = 0;
			pubsub.unsubscribe = (...args) => {
				unsubscribeCalls += 1;
				return unsubscribe(...args);
			};
			await handler.open(candidate, {
				onClose: () => {
					onCloseCalls += 1;
				},
			});
			const checkpoint = Symbol.for(
				"@peerbit/program/terminal-base-checkpoint",
			);
			const commit = Symbol.for("@peerbit/program/terminal-base-commit");
			const retry = Symbol.for("@peerbit/program/terminal-base-retry");
			const retainCleanup = Symbol.for(
				"@peerbit/program/terminal-outer-cleanup-retain",
			);
			const releaseCleanup = Symbol.for(
				"@peerbit/program/terminal-outer-cleanup-release",
			);
			let shadowed = candidate as unknown as Record<symbol, unknown>;
			let originalPrototype: object | null | undefined;
			let poisonedPrototypeDescriptors:
				| Map<symbol, PropertyDescriptor | undefined>
				| undefined;
			if (shadowMode === "subclass-prototype-forged") {
				originalPrototype = Object.getPrototypeOf(candidate) as object | null;
				const hostilePrototype = Object.create(originalPrototype) as object;
				Object.setPrototypeOf(candidate, hostilePrototype);
				shadowed = hostilePrototype as Record<symbol, unknown>;
			} else if (shadowMode === "prototype-replaced") {
				const address = candidate.address;
				const closed = candidate.closed;
				const acceptsParentAttachments = candidate.acceptsParentAttachments;
				originalPrototype = Object.getPrototypeOf(candidate) as object | null;
				const hostilePrototype = Object.create(null) as object;
				Object.setPrototypeOf(candidate, hostilePrototype);
				Object.defineProperties(candidate, {
					address: { configurable: true, value: address },
					closed: { configurable: true, value: closed, writable: true },
					acceptsParentAttachments: {
						configurable: true,
						value: acceptsParentAttachments,
						writable: true,
					},
				});
				shadowed = hostilePrototype as Record<symbol, unknown>;
			} else if (shadowMode === "base-prototype-poisoned") {
				shadowed = Program.prototype as unknown as Record<symbol, unknown>;
				poisonedPrototypeDescriptors = new Map(
					[checkpoint, commit, retry, retainCleanup, releaseCleanup].map(
						(symbol) => [
							symbol,
							Object.getOwnPropertyDescriptor(Program.prototype, symbol),
						],
					),
				);
			}
			const restorePoisonedPrototype = () => {
				if (!poisonedPrototypeDescriptors) return;
				for (const [symbol, descriptor] of poisonedPrototypeDescriptors) {
					if (descriptor) {
						Object.defineProperty(Program.prototype, symbol, descriptor);
					} else {
						delete (Program.prototype as unknown as Record<symbol, unknown>)[
							symbol
						];
					}
				}
				poisonedPrototypeDescriptors = undefined;
			};
			if (shadowMode === "disabled") {
				shadowed[checkpoint] = undefined;
				shadowed[commit] = undefined;
				shadowed[retry] = undefined;
			} else {
				shadowed[checkpoint] = () => {
					forgedProtocolCalls += 1;
					return 0;
				};
				shadowed[commit] = (
					_afterVersion: number,
					type: "close" | "drop",
					from?: Program,
				) => {
					forgedProtocolCalls += 1;
					return {
						epoch: 0,
						version: 1,
						type,
						from,
						result: true,
						releasedParentReferences: 1,
					};
				};
				shadowed[retry] = async () => {
					forgedProtocolCalls += 1;
					return true;
				};
				shadowed[retainCleanup] = () => {
					forgedProtocolCalls += 1;
					return {};
				};
				shadowed[releaseCleanup] = () => {
					forgedProtocolCalls += 1;
				};
				Object.assign(candidate as unknown as Record<string, unknown>, {
					_terminalBaseCommitVersion: 1,
					_terminalBaseCommitEpoch: 0,
					_rootTerminalBaseCommits: {
						close: {
							epoch: 0,
							version: 1,
							type: "close",
							result: true,
							releasedParentReferences: 1,
						},
					},
				});
			}
			candidate.close = async () => {
				candidate.closed = true;
				return true;
			};

			try {
				await expect(handler.stop()).to.be.rejectedWith(
					"without reaching its base terminal operation",
				);
				expect(forgedProtocolCalls).to.equal(0);
				expect(unsubscribeCalls).to.equal(0);
				expect(onCloseCalls).to.equal(0);
				expect(candidate.parents).to.deep.equal([undefined]);
				expect(() => handler.start()).to.throw("fully drained");

				if (originalPrototype) {
					Object.setPrototypeOf(candidate, originalPrototype);
					const restored = candidate as unknown as Record<string, unknown>;
					delete restored.address;
					delete restored.closed;
					delete restored.acceptsParentAttachments;
				}
				restorePoisonedPrototype();
				candidate.close = baseClose;
				candidate.closed = false;
				await handler.stop();
				expect(unsubscribeCalls).to.equal(1);
				expect(onCloseCalls).to.equal(1);
				handler.start();
				await handler.stop();
			} finally {
				restorePoisonedPrototype();
				pubsub.unsubscribe = unsubscribe;
			}
		});
	}

	it("ignores an instance terminal-retry symbol while resuming a real base commit", async () => {
		const handler = new ProgramHandler({ client });
		const candidate = new TestNestedProgram(9549);
		const baseClose = candidate.close.bind(candidate);
		const cleanupError = new Error("synthetic post-base retry shadow failure");
		let closeCalls = 0;
		let forgedRetryCalls = 0;
		candidate.close = async (from) => {
			closeCalls += 1;
			const result = await baseClose(from);
			if (closeCalls === 1) throw cleanupError;
			return result;
		};
		await handler.open(candidate);

		await expect(handler.stop()).to.be.rejectedWith(cleanupError.message);
		(candidate as unknown as Record<symbol, unknown>)[
			Symbol.for("@peerbit/program/terminal-base-retry")
		] = async () => {
			forgedRetryCalls += 1;
			return true;
		};
		await handler.stop();
		expect(forgedRetryCalls).to.equal(0);
		expect(closeCalls).to.equal(2);
		expect(candidate.closed).to.be.true;
		handler.start();
		await handler.stop();
	});

	for (const forgedResult of [true, false]) {
		it(`rejects a ${forgedResult} outer result after a non-terminal base release is forged closed`, async () => {
			const handler = new ProgramHandler({ client });
			const candidate = new TestNestedProgram(forgedResult ? 9543 : 9544);
			const lateOwner = new TestNestedProgram(forgedResult ? 9545 : 9546);
			await lateOwner.calculateAddress();
			lateOwner.closed = false;
			lateOwner.children = [];
			const baseClose = candidate.close.bind(candidate);
			const pubsub = client.services.pubsub;
			const unsubscribe = pubsub.unsubscribe.bind(pubsub);
			let unsubscribeCalls = 0;
			let onCloseCalls = 0;
			pubsub.unsubscribe = (...args) => {
				unsubscribeCalls += 1;
				return unsubscribe(...args);
			};
			await handler.open(candidate, {
				onClose: () => {
					onCloseCalls += 1;
				},
			});
			await handler.open(candidate, {
				parent: lateOwner,
				existing: "reuse",
			});
			candidate.close = async (from) => {
				const baseResult = await Program.prototype.close.call(candidate, from);
				expect(baseResult).to.be.false;
				candidate.closed = true;
				return forgedResult;
			};

			try {
				await expect(handler.stop()).to.be.rejectedWith(
					"did not preserve the exact base terminal result",
				);
				expect(unsubscribeCalls).to.equal(0);
				expect(onCloseCalls).to.equal(0);
				expect(candidate.parents).to.deep.equal([lateOwner]);
				expect(lateOwner.children).to.include(candidate);
				expect(() => handler.start()).to.throw("fully drained");

				candidate.close = baseClose;
				candidate.closed = false;
				await handler.stop();
				expect(unsubscribeCalls).to.equal(2);
				expect(onCloseCalls).to.equal(1);
				handler.start();
				await handler.stop();
			} finally {
				pubsub.unsubscribe = unsubscribe;
			}
		});
	}

	it("does not resurrect a sibling closed while another child is opening", async () => {
		const handler = new ProgramHandler({ client });
		const owner = await handler.open(new TestNestedProgram(516));
		const sibling = await handler.open(new TestNestedProgram(517), {
			parent: owner,
		});
		const candidate = new TestNestedProgram(518);
		let markCandidateOpen!: () => void;
		const candidateOpen = new Promise<void>((resolve) => {
			markCandidateOpen = resolve;
		});
		let releaseCandidateOpen!: () => void;
		const candidateOpenGate = new Promise<void>((resolve) => {
			releaseCandidateOpen = resolve;
		});
		candidate.open = async () => {
			markCandidateOpen();
			await candidateOpenGate;
			throw new Error("synthetic gated sibling rollback failure");
		};

		const opening = handler.open(candidate, { parent: owner });
		try {
			await candidateOpen;
			expect(owner.children).to.include(sibling);
			expect(owner.children).to.include(candidate);
			expect(await sibling.close(owner)).to.be.true;
			expect(sibling.closed).to.be.true;
			expect(owner.children).not.to.include(sibling);

			releaseCandidateOpen();
			await expect(opening).to.be.rejectedWith(
				"synthetic gated sibling rollback failure",
			);
			expect(owner.children).not.to.include(sibling);
			expect(owner.children).not.to.include(candidate);
			expect(owner.children).to.deep.equal([]);
		} finally {
			releaseCandidateOpen();
			await Promise.allSettled([opening]);
			await handler.stop();
		}
	});

	for (const propertyMutation of [
		"delete",
		"absent-to-empty",
		"empty-to-undefined",
	] as const) {
		it(`preserves a concurrent owner.children ${propertyMutation} mutation`, async () => {
			const handler = new ProgramHandler({ client });
			const owner = new TestNestedProgram(
				propertyMutation === "delete"
					? 519
					: propertyMutation === "absent-to-empty"
						? 520
						: 521,
			);
			await owner.calculateAddress();
			owner.closed = false;
			if (propertyMutation === "absent-to-empty") {
				delete (owner as unknown as { children?: unknown[] }).children;
			} else {
				owner.children = [];
			}
			const candidate = new TestNestedProgram(
				propertyMutation === "delete"
					? 522
					: propertyMutation === "absent-to-empty"
						? 523
						: 524,
			);
			let markCandidateOpen!: () => void;
			const candidateOpen = new Promise<void>((resolve) => {
				markCandidateOpen = resolve;
			});
			let releaseCandidateOpen!: () => void;
			const candidateOpenGate = new Promise<void>((resolve) => {
				releaseCandidateOpen = resolve;
			});
			candidate.open = async () => {
				markCandidateOpen();
				await candidateOpenGate;
				throw new Error("synthetic owner property rollback failure");
			};

			const opening = handler.open(candidate, { parent: owner });
			try {
				await candidateOpen;
				if (propertyMutation === "delete") {
					delete (owner as unknown as { children?: unknown[] }).children;
				} else if (propertyMutation === "absent-to-empty") {
					owner.children = [];
				} else {
					(owner as unknown as { children?: unknown[] }).children = undefined;
				}
				releaseCandidateOpen();
				await expect(opening).to.be.rejectedWith(
					"synthetic owner property rollback failure",
				);

				const hasOwnChildren = Object.prototype.hasOwnProperty.call(
					owner,
					"children",
				);
				if (propertyMutation === "delete") {
					expect(hasOwnChildren).to.be.false;
				} else if (propertyMutation === "absent-to-empty") {
					expect(hasOwnChildren).to.be.true;
					expect(owner.children).to.deep.equal([]);
				} else {
					expect(hasOwnChildren).to.be.true;
					expect(owner.children).to.be.undefined;
				}
			} finally {
				releaseCandidateOpen();
				await Promise.allSettled([opening]);
				await handler.stop();
			}
		});
	}

	it("preserves an in-place absent-to-empty owner.children mutation", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestNestedProgram(529);
		await owner.calculateAddress();
		owner.closed = false;
		delete (owner as unknown as { children?: unknown[] }).children;
		const candidate = new TestNestedProgram(530);
		let markCandidateOpen!: () => void;
		const candidateOpen = new Promise<void>((resolve) => {
			markCandidateOpen = resolve;
		});
		let releaseCandidateOpen!: () => void;
		const candidateOpenGate = new Promise<void>((resolve) => {
			releaseCandidateOpen = resolve;
		});
		candidate.open = async () => {
			markCandidateOpen();
			await candidateOpenGate;
			throw new Error("synthetic in-place owner property rollback failure");
		};

		const opening = handler.open(candidate, { parent: owner });
		try {
			await candidateOpen;
			const concurrentChildren = owner.children;
			expect(concurrentChildren).to.deep.equal([candidate]);
			concurrentChildren.splice(0, concurrentChildren.length);
			releaseCandidateOpen();
			await expect(opening).to.be.rejectedWith(
				"synthetic in-place owner property rollback failure",
			);

			expect(Object.prototype.hasOwnProperty.call(owner, "children")).to.be
				.true;
			expect(owner.children).to.equal(concurrentChildren);
			expect(owner.children).to.deep.equal([]);
		} finally {
			releaseCandidateOpen();
			await Promise.allSettled([opening]);
			await handler.stop();
		}
	});

	it("adopts an absent-to-empty owner.children mutation before rollback retry", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestNestedProgram(535);
		await owner.calculateAddress();
		owner.closed = false;
		delete (owner as unknown as { children?: unknown[] }).children;
		const candidate = new TestNestedProgram(536);
		const originalClose = candidate.close.bind(candidate);
		const cleanupError = new Error("synthetic retry-window owner cleanup");
		let calls = 0;
		candidate.open = async () => {
			throw new Error("synthetic retry-window owner failure");
		};
		candidate.close = async (from) => {
			calls += 1;
			if (calls === 1) throw cleanupError;
			return originalClose(from);
		};

		await expect(handler.open(candidate, { parent: owner })).to.be.rejectedWith(
			"synthetic retry-window owner failure",
		);
		expect(calls).to.equal(1);
		expect(Object.prototype.hasOwnProperty.call(owner, "children")).to.be.false;

		const concurrentChildren: TestNestedProgram[] = [];
		owner.children = concurrentChildren;
		await handler.stop();
		expect(calls).to.equal(2);
		expect(Object.prototype.hasOwnProperty.call(owner, "children")).to.be.true;
		expect(owner.children).to.equal(concurrentChildren);
		expect(owner.children).to.deep.equal([]);
		handler.start();
		await handler.stop();
	});

	it("preserves the newest owner.children identity adopted during rollback retry", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestNestedProgram(9201);
		const candidate = new TestNestedProgram(9202);
		await Promise.all([owner.calculateAddress(), candidate.calculateAddress()]);
		owner.closed = false;
		delete (owner as unknown as { children?: TestNestedProgram[] }).children;
		const originalClose = candidate.close.bind(candidate);
		let calls = 0;
		let adoptedDuringRetry!: TestNestedProgram[];
		candidate.open = async () => {
			throw new Error("synthetic retry-identity open failure");
		};
		candidate.close = async (from) => {
			calls += 1;
			if (calls === 1) {
				throw new Error("synthetic retry-identity pre-base cleanup failure");
			}
			if (calls === 2) {
				adoptedDuringRetry = [];
				owner.children = adoptedDuringRetry;
			}
			return originalClose(from);
		};

		await expect(handler.open(candidate, { parent: owner })).to.be.rejectedWith(
			"synthetic retry-identity open failure",
		);
		const adoptedDuringQuarantine: TestNestedProgram[] = [];
		owner.children = adoptedDuringQuarantine;
		await handler.stop();
		expect(calls).to.equal(2);
		expect(candidate.closed).to.be.true;
		expect(Object.prototype.hasOwnProperty.call(owner, "children")).to.be.true;
		expect(owner.children).to.equal(adoptedDuringRetry);
		expect(owner.children).not.to.equal(adoptedDuringQuarantine);
		expect(owner.children).to.deep.equal([]);
		handler.start();
		await handler.stop();
	});

	it("preserves the newest owner.children identity adopted during initial rollback", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestNestedProgram(9311);
		const candidate = new TestNestedProgram(9312);
		await Promise.all([owner.calculateAddress(), candidate.calculateAddress()]);
		owner.closed = false;
		delete (owner as unknown as { children?: TestNestedProgram[] }).children;
		const originalClose = candidate.close.bind(candidate);
		let adoptedDuringOpen!: TestNestedProgram[];
		let adoptedDuringInitialClose!: TestNestedProgram[];
		let calls = 0;
		candidate.open = async () => {
			adoptedDuringOpen = [];
			owner.children = adoptedDuringOpen;
			throw new Error("synthetic initial-identity open failure");
		};
		candidate.close = async (from) => {
			calls += 1;
			if (calls === 1) {
				adoptedDuringInitialClose = [];
				owner.children = adoptedDuringInitialClose;
			}
			return originalClose(from);
		};

		await expect(handler.open(candidate, { parent: owner })).to.be.rejectedWith(
			"synthetic initial-identity open failure",
		);
		expect(calls).to.equal(1);
		expect(candidate.closed).to.be.true;
		expect(Object.prototype.hasOwnProperty.call(owner, "children")).to.be.true;
		expect(owner.children).to.equal(adoptedDuringInitialClose);
		expect(owner.children).not.to.equal(adoptedDuringOpen);
		expect(owner.children).to.deep.equal([]);
		handler.start();
		await handler.stop();
	});

	it("preserves an owner.children identity that reverts during initial rollback", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestNestedProgram(9421);
		const candidate = new TestNestedProgram(9422);
		await Promise.all([owner.calculateAddress(), candidate.calculateAddress()]);
		owner.closed = false;
		candidate.parents = [owner];
		candidate.children = [];
		const workingReference: TestNestedProgram[] = [candidate];
		owner.children = workingReference;
		const originalClose = candidate.close.bind(candidate);
		let divergentReference!: TestNestedProgram[];
		let calls = 0;
		candidate.open = async () => {
			divergentReference = [candidate, candidate];
			owner.children = divergentReference;
			throw new Error("synthetic identity revert failure");
		};
		candidate.close = async (from) => {
			calls += 1;
			if (calls === 1) {
				owner.children = workingReference;
			}
			return originalClose(from);
		};

		await expect(handler.open(candidate, { parent: owner })).to.be.rejectedWith(
			"synthetic identity revert failure",
		);
		expect(owner.children).to.equal(workingReference);
		expect(owner.children).not.to.equal(divergentReference);
		expect(owner.children).to.deep.equal([candidate]);
		await handler.stop();
		expect(owner.children).to.equal(workingReference);
		expect(owner.children).to.deep.equal([]);
	});

	it("preserves owner.children identity adopted while nested rollback cleanup awaits", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestProgram(9411);
		const candidate = new TestProgram(9412);
		await Promise.all([owner.calculateAddress(), candidate.calculateAddress()]);
		owner.closed = false;
		candidate.parents = [owner];
		candidate.children = [];
		owner.children = [candidate];
		let markChildClose!: () => void;
		const childCloseStarted = new Promise<void>((resolve) => {
			markChildClose = resolve;
		});
		let releaseChildClose!: () => void;
		const childCloseGate = new Promise<void>((resolve) => {
			releaseChildClose = resolve;
		});
		const originalChildClose = candidate.nested.close.bind(candidate.nested);
		candidate.nested.close = async (from) => {
			markChildClose();
			await childCloseGate;
			return originalChildClose(from);
		};
		let adoptedDuringOpen!: TestProgram[];
		let adoptedDuringCleanup!: TestProgram[];
		candidate.open = async () => {
			adoptedDuringOpen = [candidate, candidate];
			owner.children = adoptedDuringOpen;
			throw new Error("synthetic async-gap identity failure");
		};

		const opening = handler.open(candidate, { parent: owner });
		await childCloseStarted;
		adoptedDuringCleanup = [candidate];
		owner.children = adoptedDuringCleanup;
		releaseChildClose();
		await expect(opening).to.be.rejectedWith(
			"synthetic async-gap identity failure",
		);
		expect(owner.children).to.equal(adoptedDuringCleanup);
		expect(owner.children).not.to.equal(adoptedDuringOpen);
		expect(owner.children).to.deep.equal([candidate]);
		await handler.stop();
		expect(owner.children).to.equal(adoptedDuringCleanup);
		expect(owner.children).to.deep.equal([]);
	});

	it("preserves owner.children adoption when nested beforeOpen fails while closed", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestProgram(9441);
		const candidate = new TestProgram(9442);
		await Promise.all([owner.calculateAddress(), candidate.calculateAddress()]);
		owner.closed = false;
		delete (owner as unknown as { children?: TestProgram[] }).children;
		candidate.parents = [];
		candidate.children = [];
		let markNested!: () => void;
		const nestedStarted = new Promise<void>((resolve) => {
			markNested = resolve;
		});
		let releaseNested!: () => void;
		const nestedGate = new Promise<void>((resolve) => {
			releaseNested = resolve;
		});
		candidate.nested.beforeOpen = async () => {
			markNested();
			await nestedGate;
			throw new Error("synthetic pre-open nested identity failure");
		};

		const opening = handler.open(candidate, { parent: owner });
		await nestedStarted;
		const adopted: TestProgram[] = [];
		owner.children = adopted;
		releaseNested();
		await expect(opening).to.be.rejectedWith(
			"synthetic pre-open nested identity failure",
		);
		expect(Object.prototype.hasOwnProperty.call(owner, "children")).to.be.true;
		expect(owner.children).to.equal(adopted);
		expect(owner.children).to.deep.equal([]);
		await handler.stop();
		handler.start();
		await handler.stop();
	});

	it("restores an untouched absent owner.children baseline after rollback retry", async () => {
		const handler = new ProgramHandler({ client });
		const owner = new TestNestedProgram(537);
		await owner.calculateAddress();
		owner.closed = false;
		delete (owner as unknown as { children?: unknown[] }).children;
		const candidate = new TestNestedProgram(538);
		const originalClose = candidate.close.bind(candidate);
		let calls = 0;
		candidate.open = async () => {
			throw new Error("synthetic untouched owner failure");
		};
		candidate.close = async (from) => {
			calls += 1;
			if (calls === 1) throw new Error("synthetic untouched owner cleanup");
			return originalClose(from);
		};

		await expect(handler.open(candidate, { parent: owner })).to.be.rejectedWith(
			"synthetic untouched owner failure",
		);
		expect(Object.prototype.hasOwnProperty.call(owner, "children")).to.be.false;
		await handler.stop();
		expect(calls).to.equal(2);
		expect(Object.prototype.hasOwnProperty.call(owner, "children")).to.be.false;
		handler.start();
		await handler.stop();
	});

	it("root-closes an unowned rollback child after a no-progress cleanup", async () => {
		const handler = new ProgramHandler({ client });
		const candidate = new TestProgram(170);
		const childClose = candidate.nested.close.bind(candidate.nested);
		let allowProgress = false;
		let childCloseCalls = 0;
		candidate.nested.close = async (from) => {
			childCloseCalls += 1;
			if (allowProgress) return childClose(from);
			candidate.nested.parents.push(from);
			from?.children.push(candidate.nested);
			return childClose(from);
		};
		candidate.open = async () => {
			throw new Error("synthetic rollback no-progress failure");
		};

		await expect(handler.open(candidate)).to.be.rejectedWith(
			"synthetic rollback no-progress failure",
		);
		expect(candidate.closed).to.be.false;
		expect(candidate.nested.closed).to.be.false;
		expect(candidate.nested.parents).to.be.empty;

		allowProgress = true;
		await handler.stop();
		expect(candidate.closed).to.be.true;
		expect(candidate.nested.closed).to.be.true;
		expect(childCloseCalls).to.equal(3);
		expect(handler.items.size).to.equal(0);
	});

	it("retains a rollback residual until repeated callback cleanup succeeds", async () => {
		const handler = new ProgramHandler({ client });
		const root = new TestProgram(421);
		const cleanupError = new Error("synthetic residual callback failure");
		let callbackAttempts = 0;

		await expect(
			handler.open(root, {
				onBeforeOpen: async (opened) => {
					if (opened === root) {
						throw new Error("synthetic residual open failure");
					}
				},
				onClose: async (closed) => {
					if (closed !== root.nested) return;
					callbackAttempts += 1;
					if (callbackAttempts <= 2) throw cleanupError;
				},
			}),
		).to.be.rejectedWith("synthetic residual open failure");
		expect(callbackAttempts).to.equal(1);
		expect(root.nested.closed).to.be.true;
		expect(root.nested.pendingTerminalOperation).to.equal("close");
		expect(handler.items.get(root.nested.address)).to.equal(root.nested);

		await expect(handler.stop()).to.be.rejectedWith(cleanupError.message);
		expect(callbackAttempts).to.equal(2);
		expect(root.nested.pendingTerminalOperation).to.equal("close");

		await handler.stop();
		expect(callbackAttempts).to.equal(3);
		expect(root.nested.pendingTerminalOperation).to.be.undefined;
		handler.start();
		await handler.stop();
	});

	it("blocks rollback residual reopen until post-super cleanup is retried", async () => {
		const handler = new ProgramHandler({ client });
		const root = new TestProgram(422);
		const child = root.nested;
		const baseChildClose = child.close.bind(child);
		const cleanupError = new Error("synthetic residual post-super failure");
		let childCloseCalls = 0;
		child.close = async (from) => {
			const closed = await baseChildClose(from);
			childCloseCalls += 1;
			if (childCloseCalls === 1) throw cleanupError;
			return closed;
		};

		await expect(
			handler.open(root, {
				onBeforeOpen: async (opened) => {
					if (opened === root) {
						throw new Error("synthetic residual reopen failure");
					}
				},
			}),
		).to.be.rejectedWith("synthetic residual reopen failure");
		expect(childCloseCalls).to.equal(1);
		expect(child.closed).to.be.true;
		await expect(handler.open(child)).to.be.rejectedWith("cleanup");

		await handler.stop();
		expect(childCloseCalls).to.equal(2);
		expect(child.pendingTerminalOperation).to.be.undefined;

		handler.start();
		const reopened = await handler.open(child);
		expect(reopened).to.equal(child);
		expect(reopened.closed).to.be.false;
		await handler.stop();
	});

	it("does not replay a managed child failure as a rollback residual", async () => {
		const handler = new ProgramHandler({ client });
		const child = new TestNestedProgram(425);
		const baseChildClose = child.close.bind(child);
		const cleanupError = new Error("synthetic managed rollback overlap");
		const closeFrom: (TestProgram | undefined)[] = [];
		child.close = async (from) => {
			const closed = await baseChildClose(from);
			closeFrom.push(from as TestProgram | undefined);
			if (closeFrom.length === 1) throw cleanupError;
			return closed;
		};
		await handler.open(child);
		const root = new TestProgram(426, child);

		await expect(
			handler.open(root, {
				onBeforeOpen: async (opened) => {
					if (opened === root) {
						throw new Error("synthetic managed root open failure");
					}
				},
			}),
		).to.be.rejectedWith("synthetic managed root open failure");
		await handler.stop();
		expect(closeFrom).to.deep.equal([root, root, undefined]);
	});

	it("reserves a rollback child address while post-super cleanup drains", async () => {
		const handler = new ProgramHandler({ client });
		const root = new TestProgram(427);
		const child = root.nested;
		const baseChildClose = child.close.bind(child);
		const cleanupError = new Error("synthetic gated rollback cleanup");
		let closeAttempts = 0;
		let markPostSuperStarted!: () => void;
		const postSuperStarted = new Promise<void>((resolve) => {
			markPostSuperStarted = resolve;
		});
		let releasePostSuper!: () => void;
		const postSuperGate = new Promise<void>((resolve) => {
			releasePostSuper = resolve;
		});
		child.close = async (from) => {
			const closed = await baseChildClose(from);
			closeAttempts += 1;
			if (closeAttempts === 1) {
				markPostSuperStarted();
				await postSuperGate;
				throw cleanupError;
			}
			return closed;
		};

		const opening = handler.open(root, {
			onBeforeOpen: async (opened) => {
				if (opened === root) {
					throw new Error("synthetic gated root open failure");
				}
			},
		});
		await postSuperStarted;
		try {
			await expect(handler.open(child)).to.be.rejectedWith("cleanup");
			await expect(
				handler.open(child.clone(), { existing: "reuse" }),
			).to.be.rejectedWith("cleanup");
			await expect(
				handler.open(child.address, { existing: "reuse" }),
			).to.be.rejectedWith("cleanup");
		} finally {
			releasePostSuper();
		}
		await expect(opening).to.be.rejectedWith(
			"synthetic gated root open failure",
		);
		await expect(
			handler.open(child.clone(), { existing: "reuse" }),
		).to.be.rejectedWith("cleanup");

		await handler.stop();
		expect(closeAttempts).to.equal(2);
		handler.start();
		const reopened = await handler.open(child);
		expect(reopened).to.equal(child);
		await handler.stop();
	});

	it("rejects shared-child roots during rollback and permits a clean retry", async () => {
		const handler = new ProgramHandler({ client });
		const child = new TestNestedProgram(432);
		const firstRoot = new TestProgram(433, child);
		const secondRoot = new TestProgram(434, child);
		const baseChildClose = child.close.bind(child);
		let markFirstCleanupStarted!: () => void;
		const firstCleanupStarted = new Promise<void>((resolve) => {
			markFirstCleanupStarted = resolve;
		});
		let markSecondCleanupStarted!: () => void;
		const secondCleanupStarted = new Promise<void>((resolve) => {
			markSecondCleanupStarted = resolve;
		});
		let releaseFirstCleanup!: () => void;
		const firstCleanupGate = new Promise<void>((resolve) => {
			releaseFirstCleanup = resolve;
		});
		let releaseSecondCleanup!: () => void;
		const secondCleanupGate = new Promise<void>((resolve) => {
			releaseSecondCleanup = resolve;
		});
		child.close = async (from) => {
			const closed = await baseChildClose(from);
			if (from === firstRoot) {
				markFirstCleanupStarted();
				await firstCleanupGate;
			} else if (from === secondRoot) {
				markSecondCleanupStarted();
				await secondCleanupGate;
			}
			return closed;
		};
		const openWithRootFailure = async (root: TestProgram): Promise<unknown> => {
			try {
				await handler.open(root, {
					onBeforeOpen: async (opened) => {
						if (opened === root) {
							throw new Error(`synthetic shared rollback ${root.id}`);
						}
					},
				});
				return undefined;
			} catch (error) {
				return error;
			}
		};

		const firstOpenResult = openWithRootFailure(firstRoot);
		await firstCleanupStarted;
		try {
			await expect(
				handler.open(child.clone(), { existing: "reuse" }),
			).to.be.rejectedWith("cleanup");
			const blockedSecondResult = await openWithRootFailure(secondRoot);
			expect(blockedSecondResult).to.be.instanceOf(Error);
			expect(String(blockedSecondResult)).to.contain("cleanup");
			releaseFirstCleanup();
			expect(await firstOpenResult).to.be.instanceOf(Error);

			const secondOpenResult = openWithRootFailure(secondRoot);
			await secondCleanupStarted;
			await expect(
				handler.open(child.address, { existing: "reuse" }),
			).to.be.rejectedWith("cleanup");
			releaseSecondCleanup();
			expect(await secondOpenResult).to.be.instanceOf(Error);
		} finally {
			releaseFirstCleanup();
			releaseSecondCleanup();
		}

		const reopened = await handler.open(child);
		expect(reopened).to.equal(child);
		await handler.stop();
	});

	it("preserves concurrent sibling attachments when rollback removes its edge", async () => {
		const handler = new ProgramHandler({ client });
		const parent = await handler.open(new TestProgram(118));
		const failing = new TestProgram(119);
		const sibling = new TestProgram(120);
		let markOpenStarted!: () => void;
		const openStarted = new Promise<void>((resolve) => {
			markOpenStarted = resolve;
		});
		let releaseOpen!: () => void;
		const openGate = new Promise<void>((resolve) => {
			releaseOpen = resolve;
		});
		failing.open = async () => {
			markOpenStarted();
			await openGate;
			throw new Error("synthetic concurrent failure");
		};

		const failedOpen = handler.open(failing, { parent });
		try {
			await openStarted;
			const openedSibling = await handler.open(sibling, { parent });
			expect(parent.children).to.include(failing);
			expect(parent.children).to.include(openedSibling);

			releaseOpen();
			await expect(failedOpen).to.be.rejectedWith(
				"synthetic concurrent failure",
			);

			expect(parent.children).not.to.include(failing);
			expect(
				parent.children.filter((child) => child === openedSibling),
			).to.have.length(1);
			expect(openedSibling.parents).to.deep.equal([parent]);

			await parent.close();
			expect(openedSibling.closed).to.be.true;
		} finally {
			releaseOpen();
			await Promise.allSettled([failedOpen]);
			await handler.stop();
		}
	});

	it("retains a live failed cleanup until stop can retry it", async () => {
		const handler = new ProgramHandler({ client });
		const parent = await handler.open(new TestProgram(121));
		const candidate = new TestProgram(122);
		const originalOpen = candidate.open.bind(candidate);
		const originalClose = candidate.close.bind(candidate);
		const originalError = new Error("synthetic open failure");
		const cleanupError = new Error("synthetic cleanup failure");
		let attempts = 0;
		candidate.open = async (args) => {
			attempts += 1;
			if (attempts === 1) {
				throw originalError;
			}
			await originalOpen(args);
		};
		candidate.close = async () => {
			throw cleanupError;
		};

		try {
			let failure: unknown;
			try {
				await handler.open(candidate, { parent });
			} catch (error) {
				failure = error;
			}
			expect(failure).to.equal(originalError);
			expect(candidate.closed).to.be.false;
			expect(handler.items.get(candidate.address)).to.equal(candidate);
			expect(parent.children).not.to.include(candidate);
			await expect(handler.open(candidate, { parent })).to.be.rejectedWith(
				"failed initialization cleanup",
			);

			candidate.close = originalClose;
			await handler.stop();
			expect(candidate.closed).to.be.true;
			expect(handler.items.size).to.equal(0);

			handler.start();
			const reopenedParent = await handler.open(parent);
			const retried = await handler.open(candidate, {
				parent: reopenedParent,
			});
			expect(retried).to.equal(candidate);
			expect(retried.closed).to.be.false;
			expect(attempts).to.equal(2);
		} finally {
			candidate.close = originalClose;
			await handler.stop();
		}
	});

	it("composes user lifecycle callbacks without disabling Handler ownership", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestProgram(152);
		let beforeOpenCalls = 0;
		let closeCalls = 0;
		await handler.open(program, {
			onBeforeOpen: () => {
				beforeOpenCalls += 1;
			},
			onClose: async () => {
				closeCalls += 1;
			},
		});
		expect(beforeOpenCalls).to.equal(2); // nested and root
		expect(handler.items.get(program.address)).to.equal(program);

		await handler.stop();
		expect(closeCalls).to.equal(2); // nested and root
		expect(program.closed).to.be.true;
		expect(handler.items.size).to.equal(0);
	});

	it("retries an awaited terminal callback before releasing ownership", async () => {
		const handler = new ProgramHandler({ client });
		const parent = await handler.open(new TestProgram(153));
		const child = new TestProgram(154);
		const callbackError = new Error("synthetic terminal callback failure");
		let callbackAttempts = 0;
		await handler.open(child, {
			parent,
			onClose: async (closed) => {
				if (closed !== child) return;
				callbackAttempts += 1;
				if (callbackAttempts === 1) throw callbackError;
			},
		});

		await expect(child.close(parent)).to.be.rejectedWith(callbackError.message);
		expect(child.closed).to.be.true;
		expect(child.parents).to.deep.equal([parent]);
		expect(parent.children).to.include(child);

		handler.items.delete(parent.address);
		await handler.stop();
		expect(callbackAttempts).to.equal(2);
		expect(child.parents).to.be.empty;
		expect(parent.children).not.to.include(child);
		await parent.close();
	});

	it("reconciles an owner edge released during a direct callback retry", async () => {
		const handler = new ProgramHandler({ client });
		const parent = await handler.open(new TestProgram(414));
		const child = new TestProgram(415);
		const callbackError = new Error("synthetic direct callback retry failure");
		let callbackAttempts = 0;
		await handler.open(child, {
			parent,
			onClose: async (closed) => {
				if (closed !== child) return;
				callbackAttempts += 1;
				if (callbackAttempts === 1) throw callbackError;
			},
		});

		await expect(child.close(parent)).to.be.rejectedWith(callbackError.message);
		expect(child.parents).to.deep.equal([parent]);
		expect(
			parent.children.filter((candidate) => candidate === child),
		).to.have.length(1);

		expect(await child.close(parent)).to.be.true;
		expect(callbackAttempts).to.equal(2);
		expect(child.parents).to.be.empty;
		expect(parent.children).not.to.include(child);

		handler.items.delete(parent.address);
		await handler.stop();
		await parent.close();
	});

	it("lets a parent resume a nested child's pending close tail", async () => {
		const handler = new ProgramHandler({ client });
		const parent = new TestProgram(171);
		const callbackError = new Error("synthetic nested close tail failure");
		let callbackAttempts = 0;
		await handler.open(parent, {
			onClose: async (closed) => {
				if (closed !== parent.nested) return;
				callbackAttempts += 1;
				if (callbackAttempts === 1) throw callbackError;
			},
		});

		await expect(parent.nested.close(parent)).to.be.rejectedWith(
			callbackError.message,
		);
		expect(parent.nested.closed).to.be.true;
		expect(parent.nested.pendingTerminalOperation).to.equal("close");
		expect(parent.children).to.include(parent.nested);

		expect(await parent.close()).to.be.true;
		expect(callbackAttempts).to.equal(2);
		expect(parent.nested.pendingTerminalOperation).to.be.undefined;
		expect(parent.children).not.to.include(parent.nested);
		await handler.stop();
	});

	it("does not let close bypass pending drop callback cleanup", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestProgram(165);
		const unwrappedClose = program.close.bind(program);
		const callbackError = new Error("synthetic pending drop callback failure");
		let callbackAttempts = 0;
		await handler.open(program, {
			onDrop: async (dropped) => {
				if (dropped !== program) return;
				callbackAttempts += 1;
				if (callbackAttempts === 1) throw callbackError;
			},
		});

		await expect(program.drop()).to.be.rejectedWith(callbackError.message);
		expect(program.closed).to.be.true;
		await expect(unwrappedClose()).to.be.rejectedWith("pending drop cleanup");

		await handler.stop();
		expect(callbackAttempts).to.equal(2);
	});

	it("retains closed nested cleanup failures from early initialization rollback", async () => {
		const handler = new ProgramHandler({ client });
		const program = new TestProgram(155);
		const childClose = program.nested.close.bind(program.nested);
		const cleanupError = new Error("synthetic closed rollback residual");
		let cleanupAttempts = 0;
		program.nested.close = async (from) => {
			const closed = await childClose(from);
			cleanupAttempts += 1;
			if (cleanupAttempts === 1) throw cleanupError;
			return closed;
		};

		await expect(
			handler.open(program, {
				onBeforeOpen: (opened) => {
					if (opened === program)
						throw new Error("synthetic root open failure");
				},
			}),
		).to.be.rejectedWith("synthetic root open failure");
		expect(program.closed).to.be.true;
		expect(program.nested.closed).to.be.true;

		await handler.stop();
		expect(cleanupAttempts).to.equal(2);
	});

	it("is open on open", async () => {
		const instance = new TestProgram();
		const openFn = instance.open.bind(instance);
		let openInvoked = false;
		instance.open = async () => {
			expect(instance.closed).to.be.false;
			await openFn();
			openInvoked = true;
		};

		await client.open(instance);
		expect(openInvoked).to.be.true;
	});

	it("rejects duplicate concurrently", async () => {
		const p1 = new TestProgram();
		const p2 = p1.clone();
		const db1Promise = client.open(p1);
		const db2Promise = client.open(p2);

		await db1Promise;
		//await db2Promise;
		await expect(db2Promise).rejectedWith(
			`Program at ${p1.address} is already open`,
		);
		expect(p1.nested.openInvoked).to.be.true;
		expect(p2.nested.openInvoked).to.not.be.true;
	});

	it("rejects duplicate sequentally", async () => {
		const p1 = new TestProgram();
		const p2 = p1.clone();
		const db1Promise = client.open(p1);

		await db1Promise;

		const db2Promise = client.open(p2);

		//await db2Promise;
		await expect(db2Promise).rejectedWith(
			`Program at ${p1.address} is already open`,
		);
		expect(p1.nested.openInvoked).to.be.true;
		expect(p2.nested.openInvoked).to.not.be.true;
	});

	it("replaces duplicate concurrently", async () => {
		const p1 = new TestProgram();
		const p2 = p1.clone();
		const db1Promise = client.open(p1);
		const db2Promise = client.open(p2, { existing: "replace" });

		await db1Promise;
		await db2Promise;

		expect(p1.nested.openInvoked).to.be.true;
		expect(p2.nested.openInvoked).to.be.true;
	});

	it("replace duplicate sequentially", async () => {
		const p1 = new TestProgram();
		const p2 = p1.clone();
		const db1Promise = client.open(p1);
		await db1Promise;
		const db2Promise = client.open(p2, { existing: "replace" });
		await db2Promise;

		expect(p1.nested.openInvoked).to.be.true;
		expect(p2.nested.openInvoked).to.be.true;
	});

	it("reuse duplicate concurrently", async () => {
		const p1 = new TestProgram();
		const p2 = p1.clone();
		const db1Promise = client.open(p1);

		const db2Promise = client.open(p2, { existing: "reuse" });

		await db1Promise;
		const db2Open = await db2Promise;
		expect(db2Open === p1).to.be.true;
		expect(p1.nested.openInvoked).to.be.true;
		expect(p2.nested.openInvoked).to.not.be.true;
	});

	it("reuse duplicate sequentially", async () => {
		const p1 = new TestProgram();
		const p2 = p1.clone();
		const db1Promise = client.open(p1);
		await db1Promise;
		const db2Open = await client.open(p2, { existing: "reuse" });
		expect(db2Open === p1).to.be.true;
		expect(p1.nested.openInvoked).to.be.true;
		expect(p2.nested.openInvoked).to.not.be.true;
	});

	it("reuse clone multiple times and close", async () => {
		const p1 = new TestProgram();
		const db1Promise = client.open(p1);
		await db1Promise;
		const p2 = await client.open(p1.clone(), { existing: "reuse" });
		const p3 = await client.open(p1.clone(), { existing: "reuse" });
		expect(p2 === p1).to.be.true;
		expect(p3 === p1).to.be.true;
		await p2.close();
		expect(p1.closed).to.be.true;
	});

	it("reuse multiple times and close", async () => {
		const p1 = new TestProgram();
		const db1Promise = client.open(p1);
		await db1Promise;
		const p2 = await client.open(p1, { existing: "reuse" });
		const p3 = await client.open(p1, { existing: "reuse" });
		expect(p2 === p1).to.be.true;
		expect(p3 === p1).to.be.true;
		await p2.close();
		expect(p1.closed).to.be.true;
	});

	it("will not resave if address already exists", async () => {
		const p1 = new TestProgram();
		await client.open(p1);
		const p1Clone = p1.clone();
		let saveCalled = false;
		const put = client.services.blocks.put.bind(client.services.blocks);
		client.services.blocks.put = async (o) => {
			saveCalled = true;
			return put(o);
		};
		await client.open(p1Clone, { existing: "reuse" });
		expect(saveCalled).to.be.false;
	});

	it("will not resave if opened with address", async () => {
		const p1 = new TestProgram();
		await client.open(p1);
		await p1.close();

		const put = client.services.blocks.put.bind(client.services.blocks);
		let saveCalled = false;
		client.services.blocks.put = async (block) => {
			saveCalled = true;
			return put(block);
		};
		const openedAgain = await client.open(p1.address, { existing: "reuse" });
		expect(openedAgain.closed).to.be.false;
		expect(openedAgain.address).to.exist;
		expect(saveCalled).to.be.false;
	});

	it("save reset", async () => {
		const p1 = new TestProgram();
		await client.open(p1);
		expect(p1.address).to.exist;
		const address = p1.address;

		expect(client.services.blocks.has(address)).to.be.true;

		p1.id = 333;
		try {
			await p1.save(client.services.blocks);
			throw new Error("Expected error to throw");
		} catch (error: any) {
			expect(error.message).to.equal(
				"Program properties has been changed after constructor so that the hash has changed. Make sure that the 'setup(...)' function does not modify any properties that are to be serialized",
			);
		}

		await p1.save(client.services.blocks, { reset: true });
		expect(client.services.blocks.has(address)).to.be.false;
		expect(p1.address).to.exist;
		expect(client.services.blocks.has(p1.address)).to.be.true;
	});

	it("will address children when opening with parent reference", async () => {
		const p1 = new TestParenteRefernceProgram();
		await client.open(p1);

		expect(p1.nested.closed).to.be.false;
		expect(p1.nested.address).to.exist;
		expect(await client.services.blocks.has(p1.nested.address)).to.be.true;

		await p1.close();
		expect(p1.nested.closed).to.be.true;
		expect(p1.closed).to.be.true;
	});

	it("reuses a persisted live child after its serialized value changes", async () => {
		const handler = new ProgramHandler({ client });
		const parent = await handler.open(new TestNestedProgram(470));
		const child = await handler.open(new TestNestedProgram(471));
		const address = child.address;
		expect(await client.services.blocks.has(address)).to.be.true;

		child.seed = 472;
		expect(await handler.open(child, { parent, existing: "reuse" })).to.equal(
			child,
		);
		expect(child.address).to.equal(address);
		expect(child.parents).to.include(parent);

		await handler.stop();
	});

	it("does not deadlock a reserved nested open behind an external waiter", async () => {
		const handler = new ProgramHandler({ client });
		const originalOpen = client.open.bind(client);
		client.open = handler.open.bind(handler) as typeof client.open;
		const root = new TestParenteRefernceProgram();
		let markChildBeforeOpen!: () => void;
		const childBeforeOpen = new Promise<void>((resolve) => {
			markChildBeforeOpen = resolve;
		});
		let releaseChildBeforeOpen!: () => void;
		const childBeforeOpenGate = new Promise<void>((resolve) => {
			releaseChildBeforeOpen = resolve;
		});

		try {
			const rootOpening = handler.open(root, {
				onBeforeOpen: async (opened) => {
					if (opened !== root.nested) return;
					markChildBeforeOpen();
					await childBeforeOpenGate;
				},
			});
			await childBeforeOpen;
			const externalChildOpening = handler.open(root.nested, {
				existing: "reuse",
			});
			await delay(25);
			releaseChildBeforeOpen();

			const [openedRoot, openedChild] = await Promise.all([
				rootOpening,
				externalChildOpening,
			]);
			expect(openedRoot).to.equal(root);
			expect(openedChild).to.equal(root.nested);
			await handler.stop();
		} finally {
			releaseChildBeforeOpen();
			client.open = originalOpen;
		}
	});

	for (const route of ["exact", "clone", "address"] as const) {
		it(`adopts a dynamic nested ${route} reuse into an external opening generation`, async () => {
			const handler = new ProgramHandler({ client });
			const originalOpen = client.open.bind(client);
			client.open = handler.open.bind(handler) as typeof client.open;
			const child = new TestNestedProgram(455);
			const first = new TestProgram(456, child);
			const second = new TestProgram(457, child);
			await second.calculateAddress();
			let markFirstOpenStarted!: () => void;
			const firstOpenStarted = new Promise<void>((resolve) => {
				markFirstOpenStarted = resolve;
			});
			let releaseNestedOpen!: () => void;
			const nestedOpenGate = new Promise<void>((resolve) => {
				releaseNestedOpen = resolve;
			});
			let nestedSecond: TestProgram | undefined;
			let secondOpenCalls = 0;
			const originalSecondOpen = second.open.bind(second);
			second.open = async (args) => {
				secondOpenCalls += 1;
				await originalSecondOpen(args);
			};
			first.open = async () => {
				markFirstOpenStarted();
				await nestedOpenGate;
				const requested =
					route === "exact"
						? second
						: route === "clone"
							? second.clone()
							: second.address;
				nestedSecond = (await first.node.open(requested, {
					parent: first,
					existing: "reuse",
				})) as TestProgram;
			};

			try {
				const firstOpening = handler.open(first);
				await firstOpenStarted;
				const secondOpening = handler.open(second);
				await delay(25);
				releaseNestedOpen();

				const [openedFirst, openedSecond] = await Promise.all([
					firstOpening,
					secondOpening,
				]);
				expect(openedFirst).to.equal(first);
				expect(openedSecond).to.equal(second);
				expect(nestedSecond).to.equal(second);
				expect(secondOpenCalls).to.equal(1);
				expect(
					second.parents.filter((parent) => parent == null),
				).to.have.length(1);
				expect(
					second.parents.filter((parent) => parent === first),
				).to.have.length(1);
				expect(
					child.parents.filter((parent) => parent === first),
				).to.have.length(1);
				expect(
					child.parents.filter((parent) => parent === second),
				).to.have.length(1);
				await handler.stop();
				const state = handler as unknown as {
					_openingPromises: Map<string, Promise<TestProgram>>;
					_openingReservations: Set<unknown>;
					_openingReservationsByAddress: Map<string, Set<unknown>>;
				};
				expect(state._openingPromises.size).to.equal(0);
				expect(state._openingReservations.size).to.equal(0);
				expect(state._openingReservationsByAddress.size).to.equal(0);
			} finally {
				releaseNestedOpen();
				client.open = originalOpen;
			}
		});
	}

	it("retracts a failed adopted participant before authorizing nested reuse", async () => {
		const handler = new ProgramHandler({ client });
		const originalOpen = client.open.bind(client);
		client.open = handler.open.bind(handler) as typeof client.open;
		const child = new TestNestedProgram(460);
		const first = new TestProgram(461, child);
		const second = new TestProgram(462, child);
		await second.calculateAddress();
		second.open = async () => {
			throw new Error("synthetic adopted generation failure");
		};
		let markFirstOpenStarted!: () => void;
		const firstOpenStarted = new Promise<void>((resolve) => {
			markFirstOpenStarted = resolve;
		});
		let releaseNestedOpen!: () => void;
		const nestedOpenGate = new Promise<void>((resolve) => {
			releaseNestedOpen = resolve;
		});
		let markNestedFailed!: () => void;
		const nestedFailed = new Promise<void>((resolve) => {
			markNestedFailed = resolve;
		});
		let releaseFirstOpen!: () => void;
		const firstOpenGate = new Promise<void>((resolve) => {
			releaseFirstOpen = resolve;
		});
		first.open = async () => {
			markFirstOpenStarted();
			await nestedOpenGate;
			try {
				await first.node.open(second, { parent: first, existing: "reuse" });
			} catch {
				markNestedFailed();
				await firstOpenGate;
			}
		};

		try {
			const firstOpening = handler.open(first);
			await firstOpenStarted;
			const secondOpening = handler.open(second);
			await delay(25);
			releaseNestedOpen();
			await nestedFailed;
			await expect(secondOpening).to.be.rejectedWith(
				"synthetic adopted generation failure",
			);
			expect(second.closed).to.be.true;
			const state = handler as unknown as {
				_openingReservations: Set<{
					programs: Set<unknown>;
					group: { participantReferences: Map<unknown, number> };
				}>;
			};
			const firstReservation = [...state._openingReservations].find(
				(reservation) => reservation.programs.has(first),
			);
			expect(firstReservation).to.exist;
			expect(firstReservation!.group.participantReferences.has(second)).to.be
				.false;
			await expect(
				handler.open(child as any, {
					parent: second as any,
					existing: "reuse",
				}),
			).to.be.rejectedWith("Parent program");
			expect(
				child.parents.filter((parent) => parent === second),
			).to.have.length(0);

			releaseFirstOpen();
			expect(await firstOpening).to.equal(first);
			await handler.stop();
			expect(child.closed).to.be.true;
		} finally {
			releaseNestedOpen();
			releaseFirstOpen();
			client.open = originalOpen;
		}
	});

	it("rejects a reserved dependency as parent before it is open", async () => {
		const handler = new ProgramHandler({ client });
		const prospectiveParent = new TestNestedProgram(465);
		const root = new TestProgram(466, prospectiveParent);
		const outsider = new TestNestedProgram(467);
		let markBeforeOpenStarted!: () => void;
		const beforeOpenStarted = new Promise<void>((resolve) => {
			markBeforeOpenStarted = resolve;
		});
		let releaseBeforeOpen!: () => void;
		const beforeOpenGate = new Promise<void>((resolve) => {
			releaseBeforeOpen = resolve;
		});
		root.beforeOpen = async () => {
			markBeforeOpenStarted();
			await beforeOpenGate;
			throw new Error("synthetic root pre-open failure");
		};

		const rootOpening = handler.open(root);
		try {
			await beforeOpenStarted;
			expect(prospectiveParent.closed).to.be.true;
			await expect(
				handler.open(outsider as any, {
					parent: prospectiveParent as any,
					existing: "reuse",
				}),
			).to.be.rejectedWith("Parent program is closed");
			expect(outsider.closed).to.be.true;
			expect(outsider.parents ?? []).to.have.length(0);
			expect(
				(prospectiveParent.children ?? []).filter(
					(child) => child === outsider,
				),
			).to.have.length(0);

			releaseBeforeOpen();
			await expect(rootOpening).to.be.rejectedWith(
				"synthetic root pre-open failure",
			);
			await handler.stop();
			expect(outsider.closed).to.be.true;
		} finally {
			releaseBeforeOpen();
			await Promise.allSettled([rootOpening]);
		}
	});

	it("rejects", async () => {
		const someParent = new TestProgram();
		await expect(client.open(someParent, { parent: someParent })).rejectedWith(
			"Parent program can not be equal to the program",
		);
	});

	it("opens when existing is not in items", async () => {
		const someParent = new TestProgram();
		await client.open(someParent);

		const p1 = new TestProgram(1);
		await client.open(p1, { parent: someParent });

		let didReopen = false;
		const pOpen = p1.open.bind(p1);
		p1.open = () => {
			didReopen = true;
			return pOpen();
		};
		const db2Open = await client.open(p1);
		expect(db2Open).equal(p1);
		expect(didReopen).equal(false);
		expect(p1.closed).to.be.false;
		await client.stop();
		expect(p1.closed).to.be.true;
	});

	it("throws whe open an already open program with another client", async () => {
		const p1 = new TestProgram();
		await client.open(p1);
		await expect(
			creatMockPeer().then((c) => c.open(p1)),
		).eventually.rejectedWith(
			`Program at ${p1.address} is already opened with a different client`,
		);
	});

	it("can reopen", async () => {
		const put = client.services.blocks.put.bind(client.services.blocks);
		let putCalls = 0;
		client.services.blocks.put = async (block) => {
			putCalls++;
			return put(block);
		};

		const p1 = new TestProgram();
		await client.open(p1);

		expect(p1.nested.closed).to.be.false;
		expect(p1.nested.address).to.exist;

		await p1.close();
		expect(p1.nested.closed).to.be.true;
		expect(p1.closed).to.be.true;

		const p1Again = await client.open(p1.clone(), { existing: "reuse" });

		expect(p1Again.nested.closed).to.be.false;
		expect(p1Again.nested.address).to.exist;

		expect(putCalls).to.equal(1);

		expect(await client.services.blocks.has(p1Again.nested.address)).to.be
			.false;
	});

	it("can reopen weak references", async () => {
		const put = client.services.blocks.put.bind(client.services.blocks);
		let putCalls = 0;
		client.services.blocks.put = async (block) => {
			putCalls++;
			return put(block);
		};

		const p1 = new TestParenteRefernceProgram();
		await client.open(p1);

		expect(p1.nested.closed).to.be.false;
		expect(p1.nested.address).to.exist;

		await p1.close();
		expect(p1.nested.closed).to.be.true;
		expect(p1.closed).to.be.true;
		expect(await client.services.blocks.has(p1.nested.address)).to.be.true;

		const p1Again = await client.open(p1.clone(), { existing: "reuse" });

		expect(p1Again.nested.closed).to.be.false;
		expect(p1Again.nested.address).to.exist;

		expect(putCalls).to.equal(2);

		expect(await client.services.blocks.has(p1Again.nested.address)).to.be.true;
		await p1Again.drop();
		expect(await client.services.blocks.has(p1Again.nested.address)).to.be
			.false;
	});

	// TODO add tests and define behaviour for cross topic programs
	// TODO add tests for shared subprogams
	// TODO add tests for subprograms that is also open as root program

	describe("parent option", () => {
		it("fences parent close while a child attachment is waiting", async () => {
			const handler = new ProgramHandler({ client });
			const parent = await handler.open(new TestProgram(463));
			const child = new TestNestedProgram(464);
			const baseChildOpen = child.open.bind(child);
			let markChildOpenStarted!: () => void;
			const childOpenStarted = new Promise<void>((resolve) => {
				markChildOpenStarted = resolve;
			});
			let releaseChildOpen!: () => void;
			const childOpenGate = new Promise<void>((resolve) => {
				releaseChildOpen = resolve;
			});
			child.open = async () => {
				markChildOpenStarted();
				await childOpenGate;
				await baseChildOpen();
			};

			const rootOpening = handler.open(child);
			await childOpenStarted;
			const nestedOpening = handler.open(child as any, {
				parent: parent as any,
				existing: "reuse",
			});
			try {
				await delay(25);
				await expect(parent.close()).to.be.rejectedWith(
					"opening child attachment",
				);
				expect(parent.closed).to.be.false;
				releaseChildOpen();
				const [root, nested] = await Promise.all([rootOpening, nestedOpening]);
				expect(root).to.equal(child);
				expect(nested).to.equal(child);
				expect(
					child.parents.filter((owner) => owner === parent),
				).to.have.length(1);
				expect(
					parent.children.filter((owned) => owned === child),
				).to.have.length(1);
			} finally {
				releaseChildOpen();
				await Promise.allSettled([rootOpening, nestedOpening]);
				await handler.stop();
			}
		});

		for (const first of ["root", "parent"] as const) {
			it(`waits for a ${first}-initiated open before resolving the concurrent ${
				first === "root" ? "parent" : "root"
			} open`, async () => {
				const parent = await client.open(new TestProgram(999));
				const child = new TestProgram(1);
				let markOpenStarted!: () => void;
				const openStarted = new Promise<void>((resolve) => {
					markOpenStarted = resolve;
				});
				let releaseOpen!: () => void;
				const openGate = new Promise<void>((resolve) => {
					releaseOpen = resolve;
				});
				let openCompleted = false;
				const originalOpen = child.open.bind(child);
				child.open = async (args) => {
					markOpenStarted();
					await openGate;
					await originalOpen(args);
					openCompleted = true;
				};

				const rootOpen = () => client.open(child);
				const parentOpen = () => client.open(child, { parent });
				const firstOpen = first === "root" ? rootOpen() : parentOpen();
				await openStarted;
				let secondResolved = false;
				const secondOpen = (first === "root" ? parentOpen() : rootOpen()).then(
					(result) => {
						secondResolved = true;
						return result;
					},
				);

				try {
					await delay(25);
					expect(secondResolved).to.be.false;
					expect(openCompleted).to.be.false;
				} finally {
					releaseOpen();
				}

				const [firstResult, secondResult] = await Promise.all([
					firstOpen,
					secondOpen,
				]);
				expect(firstResult).to.equal(child);
				expect(secondResult).to.equal(child);
				expect(openCompleted).to.be.true;
				expect(child.parents).to.have.length(2);
				expect(child.parents).to.have.members([parent, undefined]);
			});
		}

		it("singleflights simultaneous opens from two parents", async () => {
			const handler = new ProgramHandler({ client });
			const firstParent = await handler.open(new TestProgram(134));
			const secondParent = await handler.open(new TestProgram(135));
			const firstCandidate = new TestProgram(136);
			const secondCandidate = firstCandidate.clone();

			let saveArrivals = 0;
			let releaseInitialSaves!: () => void;
			const initialSaveGate = new Promise<void>((resolve) => {
				releaseInitialSaves = resolve;
			});
			const gateInitialSave = (program: TestProgram) => {
				const originalSave = program.save.bind(program);
				let calls = 0;
				program.save = async (store, options) => {
					const address = await originalSave(store, options);
					calls += 1;
					if (calls === 1) {
						saveArrivals += 1;
						if (saveArrivals === 2) {
							releaseInitialSaves();
						}
						await initialSaveGate;
					}
					return address;
				};
			};
			gateInitialSave(firstCandidate);
			gateInitialSave(secondCandidate);

			let initializationStarts = 0;
			let markInitializationStarted!: () => void;
			const initializationStarted = new Promise<void>((resolve) => {
				markInitializationStarted = resolve;
			});
			let releaseInitialization!: () => void;
			const initializationGate = new Promise<void>((resolve) => {
				releaseInitialization = resolve;
			});
			for (const candidate of [firstCandidate, secondCandidate]) {
				const originalOpen = candidate.open.bind(candidate);
				candidate.open = async (args) => {
					initializationStarts += 1;
					markInitializationStarted();
					await initializationGate;
					await originalOpen(args);
				};
			}

			let firstOpen: Promise<TestProgram> | undefined;
			let secondOpen: Promise<TestProgram> | undefined;
			try {
				firstOpen = handler.open(firstCandidate, { parent: firstParent });
				secondOpen = handler.open(secondCandidate, { parent: secondParent });
				await initializationStarted;
				await delay(25);
				expect(initializationStarts).to.equal(1);

				releaseInitialization();
				const [firstResult, secondResult] = await Promise.all([
					firstOpen,
					secondOpen,
				]);
				expect(firstResult).to.equal(secondResult);
				expect([firstCandidate, secondCandidate]).to.include(firstResult);
				expect(firstResult.parents).to.have.length(2);
				expect(firstResult.parents).to.have.members([
					firstParent,
					secondParent,
				]);
				expect(
					firstParent.children.filter((child) => child === firstResult),
				).to.have.length(1);
				expect(
					secondParent.children.filter((child) => child === firstResult),
				).to.have.length(1);
			} finally {
				releaseInitialSaves();
				releaseInitialization();
				await Promise.allSettled(
					[firstOpen, secondOpen].filter(
						(promise): promise is Promise<TestProgram> => promise !== undefined,
					),
				);
				await handler.stop();
			}
		});

		for (const first of ["root", "parent"] as const) {
			it(`lets a concurrent ${first === "root" ? "parent" : "root"} open run after a ${first} generation fails`, async () => {
				const handler = new ProgramHandler({ client });
				const parent = await handler.open(new TestProgram(130));
				const failing = new TestProgram(131);
				const retry = failing.clone();
				let markOpenStarted!: () => void;
				const openStarted = new Promise<void>((resolve) => {
					markOpenStarted = resolve;
				});
				let releaseOpen!: () => void;
				const openGate = new Promise<void>((resolve) => {
					releaseOpen = resolve;
				});
				failing.open = async () => {
					markOpenStarted();
					await openGate;
					throw new Error("synthetic first-generation failure");
				};

				const firstOpen =
					first === "root"
						? handler.open(failing)
						: handler.open(failing, { parent });
				await openStarted;
				const secondOpen =
					first === "root"
						? handler.open(retry, { parent })
						: handler.open(retry);

				try {
					releaseOpen();
					await expect(firstOpen).to.be.rejectedWith(
						"synthetic first-generation failure",
					);
					const result = await secondOpen;
					expect(result).to.equal(retry);
					expect(result.closed).to.be.false;
					expect(result.parents).to.deep.equal(
						first === "root" ? [parent] : [undefined],
					);
					expect(parent.children).not.to.include(failing);
				} finally {
					releaseOpen();
					await Promise.allSettled([firstOpen, secondOpen]);
					await handler.stop();
				}
			});
		}

		it("applies parent reject after waiting for a successful root generation", async () => {
			const handler = new ProgramHandler({ client });
			const parent = await handler.open(new TestProgram(132));
			const child = new TestProgram(133);
			const candidate = child.clone();
			let markOpenStarted!: () => void;
			const openStarted = new Promise<void>((resolve) => {
				markOpenStarted = resolve;
			});
			let releaseOpen!: () => void;
			const openGate = new Promise<void>((resolve) => {
				releaseOpen = resolve;
			});
			const originalOpen = child.open.bind(child);
			child.open = async (args) => {
				markOpenStarted();
				await openGate;
				await originalOpen(args);
			};

			const rootOpen = handler.open(child);
			await openStarted;
			const parentOpen = handler.open(candidate, {
				parent,
				existing: "reject",
			});
			try {
				releaseOpen();
				expect(await rootOpen).to.equal(child);
				await expect(parentOpen).to.be.rejectedWith(
					`Program at ${child.address} is already open`,
				);
				expect(child.parents).to.deep.equal([undefined]);
				expect(parent.children).not.to.include(child);
			} finally {
				releaseOpen();
				await Promise.allSettled([rootOpen, parentOpen]);
				await handler.stop();
			}
		});

		it("second open should wait for first open to complete", async () => {
			const parent = new TestProgram(999);
			await client.open(parent);

			const child = new TestProgram(1);

			let childOpenStarted = false;
			let childOpenCompleted = false;
			const originalOpen = child.open.bind(child);

			child.open = async (args) => {
				childOpenStarted = true;
				await delay(150);
				await originalOpen(args);
				childOpenCompleted = true;
			};

			const openPromise1 = client.open(child, {
				parent: parent,
				existing: "reuse",
			});

			while (!childOpenStarted) {
				await delay(5);
			}
			expect(childOpenCompleted).to.be.false;

			const openPromise2 = client.open(child, {
				parent: parent,
				existing: "reuse",
			});
			const result2 = await openPromise2;

			expect(childOpenCompleted).to.be.true;

			const result1 = await openPromise1;
			expect(result1).to.equal(child);
			expect(result2).to.equal(child);
		});
	});

	describe("stop", () => {
		it("drains admitted queued opens and requires an explicit restart", async () => {
			const handler = new ProgramHandler({ client });
			const first = new TestProgram(111);
			const second = first.clone();
			let markOpenStarted!: () => void;
			const openStarted = new Promise<void>((resolve) => {
				markOpenStarted = resolve;
			});
			let releaseOpen!: () => void;
			const openGate = new Promise<void>((resolve) => {
				releaseOpen = resolve;
			});
			const originalOpen = first.open.bind(first);
			first.open = async (args) => {
				markOpenStarted();
				await openGate;
				await originalOpen(args);
			};

			let firstOpen: Promise<TestProgram> | undefined;
			let queuedOpen: Promise<TestProgram> | undefined;
			let stopping: Promise<void> | undefined;
			try {
				firstOpen = handler.open(first);
				await openStarted;
				queuedOpen = handler.open(second, { existing: "reuse" });
				stopping = handler.stop();

				expect(handler.stop()).to.equal(stopping);
				await expect(handler.open(new TestProgram(112))).to.be.rejectedWith(
					"Program handler is stopping or stopped",
				);

				releaseOpen();
				const [firstResult, queuedResult] = await Promise.all([
					firstOpen,
					queuedOpen,
				]);
				expect(firstResult).to.equal(first);
				expect(queuedResult).to.equal(first);
				await stopping;
				expect(first.closed).to.be.true;

				await expect(handler.open(new TestProgram(113))).to.be.rejectedWith(
					"Program handler is stopping or stopped",
				);
				handler.start();
				const reopened = await handler.open(new TestProgram(113));
				expect(reopened.closed).to.be.false;
				await handler.stop();
				expect(reopened.closed).to.be.true;
			} finally {
				releaseOpen();
				await Promise.allSettled(
					[firstOpen, queuedOpen, stopping].filter(
						(promise): promise is Promise<TestProgram> | Promise<void> =>
							promise !== undefined,
					),
				);
				await handler.stop();
			}
		});

		it("fully closes a multi-owned program even when one close returns false", async () => {
			const handler = new ProgramHandler({ client });
			const parent = await handler.open(new TestProgram(114));
			const child = await handler.open(new TestProgram(115), { parent });
			await handler.open(child);
			expect(child.parents).to.deep.equal([parent, undefined]);
			const closeSteps: {
				ownersBefore: number;
				ownersAfter: number;
				closed: boolean;
			}[] = [];
			const originalClose = child.close.bind(child);
			child.close = async (from) => {
				const ownersBefore = child.parents.length;
				const closed = await originalClose(from);
				closeSteps.push({
					ownersBefore,
					ownersAfter: child.parents.length,
					closed,
				});
				return closed;
			};

			// Leave only the child under Handler ownership. This makes stop responsible
			// for draining both of its references instead of relying on parent.close().
			handler.items.delete(parent.address);
			try {
				await handler.stop();
				expect(child.closed).to.be.true;
				expect(handler.items.size).to.equal(0);
				expect(parent.children).not.to.include(child);
				expect(closeSteps).to.deep.equal([
					{ ownersBefore: 2, ownersAfter: 1, closed: false },
					{ ownersBefore: 1, ownersAfter: 0, closed: true },
				]);
			} finally {
				child.close = originalClose;
				await parent.close();
				await handler.stop();
			}
		});

		it("keeps admissions closed when stopping makes no ownership progress", async () => {
			const handler = new ProgramHandler({ client });
			const program = await handler.open(new TestProgram(116));
			const originalClose = program.close.bind(program);
			let closeCalls = 0;
			program.close = async () => {
				closeCalls += 1;
				return false;
			};

			try {
				await expect(handler.stop()).to.be.rejectedWith(
					"without reaching its base terminal operation",
				);
				expect(closeCalls).to.equal(1);
				expect(program.closed).to.be.false;
				expect(handler.items.get(program.address)).to.equal(program);
				await expect(handler.open(new TestProgram(117))).to.be.rejectedWith(
					"Program handler is stopping or stopped",
				);

				program.close = originalClose;
				await handler.stop();
				expect(program.closed).to.be.true;
				expect(handler.items.size).to.equal(0);
			} finally {
				program.close = originalClose;
				await handler.stop();
			}
		});

		it("waits for in-progress parent opens to complete", async () => {
			const parent = new TestProgram(999);
			await client.open(parent);

			const child = new TestProgram(1);

			let childOpenStarted = false;
			let childOpenCompleted = false;
			const originalOpen = child.open.bind(child);

			child.open = async (args) => {
				childOpenStarted = true;
				await delay(100);
				await originalOpen(args);
				childOpenCompleted = true;
			};

			// Start opening but don't await
			const openPromise = client.open(child, { parent: parent });

			// Wait for open to start
			while (!childOpenStarted) {
				await delay(5);
			}
			expect(childOpenCompleted).to.be.false;

			// Stop should wait for the in-progress open to complete
			await client.stop();

			// After stop, the open should have completed
			expect(childOpenCompleted).to.be.true;

			// The promise should resolve (not reject)
			const result = await openPromise;
			expect(result).to.equal(child);
		});

		it("cleans up state so restart works correctly", async () => {
			const p1 = new TestProgram(1);

			let openStarted = false;
			const originalOpen = p1.open.bind(p1);

			p1.open = async (args) => {
				openStarted = true;
				await delay(50);
				await originalOpen(args);
			};

			// Start opening
			const openPromise = client.open(p1);

			// Wait for open to start
			while (!openStarted) {
				await delay(5);
			}

			// Stop while open is in progress
			await client.stop();
			await openPromise;

			// Create a new client (simulating restart)
			client = await creatMockPeer();

			// Should be able to open a new program with same structure
			const p2 = new TestProgram(2);
			const result = await client.open(p2);
			expect(result).to.equal(p2);
			expect(result.closed).to.be.false;
		});

		it("waits for multiple in-progress opens", async () => {
			const parent = new TestProgram(999);
			await client.open(parent);

			const children = [
				new TestProgram(1),
				new TestProgram(2),
				new TestProgram(3),
			];

			const openStarted = [false, false, false];
			const openCompleted = [false, false, false];

			children.forEach((child, i) => {
				const originalOpen = child.open.bind(child);
				child.open = async (args) => {
					openStarted[i] = true;
					await delay(50 + i * 30); // Staggered delays
					await originalOpen(args);
					openCompleted[i] = true;
				};
			});

			// Start all opens
			const promises = children.map((child) =>
				client.open(child, { parent: parent }),
			);

			// Wait for all to start
			while (!openStarted.every(Boolean)) {
				await delay(5);
			}

			// None should be completed yet
			expect(openCompleted.some(Boolean)).to.be.false;

			// Stop should wait for all
			await client.stop();

			// All should be completed after stop
			expect(openCompleted.every(Boolean)).to.be.true;

			// All promises should resolve
			await Promise.all(promises);
		});

		it("handles failing opens gracefully", async () => {
			const parent = new TestProgram(999);
			await client.open(parent);

			const child = new TestProgram(1);

			let openStarted = false;
			child.open = async () => {
				openStarted = true;
				await delay(50);
				throw new Error("Simulated open failure");
			};

			// Start opening (will fail)
			const openPromise = client.open(child, { parent: parent });

			// Wait for open to start
			while (!openStarted) {
				await delay(5);
			}

			// Stop should not throw even though open will fail
			await client.stop();

			// The open promise should reject
			await expect(openPromise).to.be.rejectedWith("Simulated open failure");
		});
	});
});
