import fs from "fs";

export class Trust {
	trusted: string[];
	constructor(readonly path: string) {
		if (fs.existsSync(path)) {
			this.trusted = JSON.parse(
				fs.readFileSync(path).toString("utf-8"),
			) as string[];
		} else {
			this.trusted = [];
		}
	}

	save() {
		fs.writeFileSync(this.path, JSON.stringify(this.trusted));
	}

	isTrusted(hashcode: string) {
		return this.trusted.includes(hashcode);
	}

	add(key: string) {
		if (this.isTrusted(key)) {
			return;
		}
		this.trusted.push(key);
		this.save();
	}

	remove(hashcode: string) {
		const existing = this.trusted.findIndex((x) => (x = hashcode));
		if (existing >= 0) {
			this.trusted.splice(existing, 1);
			return true;
		}
		return false;
	}
}
