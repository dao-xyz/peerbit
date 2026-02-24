it("bootstrap", async function () {
	if (process.env.PEERBIT_RUN_REMOTE_BOOTSTRAP_TEST !== "1") {
		this.skip();
	}
	await import("./bootstrap.js");
});
