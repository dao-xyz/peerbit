import fs from "fs";

interface RemoteObject {
	address: string;
	name: string;
	password: string;
}

interface RemotesObject {
	remotes: RemoteObject[];
}

export class Remotes {
	private data: RemotesObject;

	constructor(readonly path: string) {
		if (fs.existsSync(path)) {
			this.data = JSON.parse(
				fs.readFileSync(path).toString("utf-8")
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
	getByAddress(address: string) {
		return this.data.remotes.find((x) => x.address === address);
	}
	add(name: string, address: string, password: string) {
		const obj: RemoteObject = {
			address,
			name,
			password,
		};
		const existing = this.data.remotes.findIndex((x) => x.name === name);
		if (existing >= 0) {
			this.data.remotes[existing] = obj;
		} else {
			this.data.remotes.push(obj);
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
