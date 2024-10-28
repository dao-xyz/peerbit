export const resolveBootstrapAddresses = async (
	v: string = "4",
): Promise<string[]> => {
	// Bootstrap addresses for network
	return (
		await (
			await fetch(
				`https://bootstrap.peerbit.org/bootstrap${v ? "-" + v : ""}.env`,
			)
		).text()
	)
		.split(/\r?\n/)
		.filter((x) => x.length > 0);
};
