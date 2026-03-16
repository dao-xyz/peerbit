import { type Multiaddr, multiaddr } from "@multiformats/multiaddr";

export const resolveBootstrapAddresses = async (
	v: string = "5",
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

export const getBootstrapPeerId = (
	address: string | Multiaddr,
): string | undefined => {
	try {
		const parsed = typeof address === "string" ? multiaddr(address) : address;
		return parsed.getComponents().find((component) => component.name === "p2p")
			?.value;
	} catch {
		return undefined;
	}
};
