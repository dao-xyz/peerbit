export const x = undefined;
/* import { ControllerType, createController, IPFSOptions } from "ipfsd-ctl";
import { installDocker, startContainer } from "./docker";

interface Module {
	type: ControllerType;
	test: boolean;
	disposable: boolean;
	args?: string[];
	ipfsHttpModule?: any;
	ipfsBin?: any;
	ipfsModule?: any;
	ipfsOptions?: IPFSOptions; // to be set later
}



export const ipfsDocker = async (): Promise<{
	api: IPFS;
	stop: () => Promise<void>;
}> => {
	const { exec } = await import("child_process");
	await new Promise((resolve, reject) => {
		exec(
			'echo "#!/bin/sh \nset -ex \nipfs bootstrap rm all \nipfs config Addresses.Swarm \'[\\"/ip4/0.0.0.0/tcp/4001\\", \\"/ip4/0.0.0.0/tcp/8081/ws\\", \\"/ip6/::/tcp/4001\\"]\' --json\nipfs config --json Pubsub.Enabled true \nipfs config Swarm.RelayService \'{\\"Enabled\\": true}\' --json" > ipfs-config.sh',
			(error, stdout, stderr) => {
				if (error || stderr) {
					reject("Failed to create config file" + stderr);
				}
				resolve(stdout);
			}
		);
	});

	await installDocker();
	await startContainer(
		"sudo docker start ipfs_host 2>/dev/null || docker run -d --name ipfs_host -v $(pwd)/ipfs-config.sh:/container-init.d/001-test.sh  -p 4001:4001 -p 4001:4001/udp -p 127.0.0.1:8081:8081 -p 127.0.0.1:5001:5001 ipfs/kubo:latest daemon"
	);
	const c = await import("ipfs-http-client");
	const http = await import("http");
	for (let i = 0; i < 3; i++) {
		try {
			const client = c.create({
				timeout: 10 * 1000,
				agent: new http.Agent({
					keepAlive: true,
					maxSockets: Infinity,
				}),
			});
			return {
				api: client,
				stop: async () => undefined,
			};
		} catch (error: any) {
			console.log(
				`Faield to create client, retrying ${i}: ${error?.message}`
			);
		}
	}
	throw new Error("Failed to create ipfs-http-client");
};
 */
