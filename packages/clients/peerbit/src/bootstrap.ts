export const resolveBootstrapAddresses = async (): Promise<string[]> => {
	// Bootstrap addresses for network
	return (
		await (
			await fetch(
				"https://raw.githubusercontent.com/dao-xyz/peerbit-bootstrap/master/bootstrap.env"
			)
		).text()
	)
		.split(/\r?\n/)
		.filter((x) => x.length > 0);
};
