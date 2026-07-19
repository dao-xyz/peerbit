import fs from "fs";

export interface AWSOrigin {
	type: "aws";
	region: string;
	instanceId: string;
}

interface HetznerOrigin {
	type: "hetzner";
	location: string;
	serverId: number;
}

export type RemoteOrigin = AWSOrigin | HetznerOrigin;

export const getRetiredAWSManagementError = (origin: AWSOrigin): Error =>
	new Error(
		`Automatic AWS management has been retired. Terminate EC2 instance ${origin.instanceId} in region ${origin.region} from the AWS console, then remove the local remote entry.`,
	);

export const DEFAULT_REMOTE_GROUP = "default";

export interface RemoteObject {
	address: string;
	name: string;
	group: string;
	/** Stable server identity pinned when the remote is enrolled. */
	peerId?: string;
	origin?: RemoteOrigin;
}

export interface RemotesObject {
	remotes: RemoteObject[];
}

export class Remotes {
	private data: RemotesObject;

	constructor(readonly path: string) {
		if (fs.existsSync(path)) {
			this.data = JSON.parse(
				fs.readFileSync(path).toString("utf-8"),
			) as RemotesObject;
		} else {
			this.data = {
				remotes: [],
			};
		}
	}

	save() {
		fs.writeFileSync(this.path, JSON.stringify(this.data));
	}

	getByName(name: string) {
		return this.data.remotes.find((x) => x.name === name);
	}

	getByGroup(group: string) {
		return this.data.remotes.filter((x) => x.group === group);
	}

	getByAddress(address: string) {
		return this.data.remotes.find((x) => x.address === address);
	}

	add(remote: RemoteObject) {
		const existing = this.data.remotes.findIndex((x) => x.name === remote.name);
		if (existing >= 0) {
			const current = this.data.remotes[existing];
			if (current.peerId && remote.peerId && current.peerId !== remote.peerId) {
				throw new Error(
					`Remote '${remote.name}' is pinned to a different server identity. Remove it before replacing the pin.`,
				);
			}
			this.data.remotes[existing] = {
				...remote,
				peerId: current.peerId ?? remote.peerId,
			};
		} else {
			this.data.remotes.push(remote);
		}
		this.save();
	}

	remove(name: string) {
		const existing = this.data.remotes.findIndex((x) => x.name === name);
		if (existing >= 0) {
			this.data.remotes.splice(existing, 1);
			return true;
		}
		return false;
	}

	async all(): Promise<RemoteObject[]> {
		return this.data.remotes;
	}
}
