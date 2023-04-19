import { LSession } from "../session";

it("connect", async () => {
	const session = await LSession.connected(3);
	await session.stop();
});
