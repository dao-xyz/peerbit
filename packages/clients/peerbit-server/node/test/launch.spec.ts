

describe("spawn", () => {
	it("no-test", () => { });
	/* it("launch and stop", async () => {
		const results = await launchNodes({
			email: "marcus@dao.xyz",
			count: 1,
			namePrefix: "test-counter",
			grantAccess: [await (await Ed25519Keypair.create()).publicKey.toPeerId()]
		});
		const domain = await waitForDomain(results[0].publicIp);
		const client = await createClient(await Ed25519Keypair.create(), {
			address: domain,
			origin: {
				type: "aws",
				instanceId: results[0].instanceId,
				region: "us-east-1",
			},
		});
		await client.terminate();
	}); */
});
