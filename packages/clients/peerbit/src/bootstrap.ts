export const resolveBootstrapAddresses = async (
	v: string = "4",
): Promise<string[]> => {
	// Bootstrap addresses for network
	return (
		await (
			await fetch(
				`https://raw.githubusercontent.com/dao-xyz/peerbit-bootstrap/master/bootstrap${v ? "-" + v : ""}.env`,
			)
		).text()
	)
		.split(/\r?\n/)
		.filter((x) => x.length > 0);
};
