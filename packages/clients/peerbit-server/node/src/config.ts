import path from "path";
import os from "os";
import fs from "fs";
import { deserialize, serialize } from "@dao-xyz/borsh";
import { Duplex } from "stream"; // Native Node Module
import { Ed25519Keypair, Keypair, fromBase64 } from "@peerbit/crypto";

const bufferToStream = (myBuffer) => {
	const tmp = new Duplex();
	tmp.push(myBuffer);
	tmp.push(null);
	return tmp;
};

export const getHomeConfigDir = (): string => {
	const configDir = path.join(os.homedir(), ".peerbit");
	return configDir;
};

export const getServerConfigPath = (configDir: string): string => {
	return path.join(configDir, "server");
};

export const getRemotesPath = (configDir: string): string => {
	return path.join(configDir, "remotes.json");
};

export const getNodePath = (directory: string): string => {
	return path.join(directory, "node");
};

export const getTrustPath = (directory: string): string => {
	return path.join(directory, "trust.json");
};

const getKeysPath = (configDir: string): string => {
	return path.join(configDir, "keys");
};

export const getKeypair = async (
	configDir: string
): Promise<Ed25519Keypair> => {
	const keypath = getKeysPath(configDir);
	if (!fs.existsSync(keypath)) {
		const keypair = await Ed25519Keypair.create();
		fs.writeFileSync(keypath, serialize(keypair));
		return keypair;
	} else {
		const keypair = deserialize(fs.readFileSync(keypath), Ed25519Keypair);
		return keypair;
	}
};

export const checkExistPath = async (path: string) => {
	try {
		if (!fs.existsSync(path)) {
			fs.accessSync(path, fs.constants.W_OK); // will throw if fails
			return false;
		}
		return true;
	} catch (err: any) {
		if (err.message.indexOf("no such file")) {
			return false;
		}
		throw new Error("Can not access path");
	}
};

export const getPackageName = async (
	path: string | Uint8Array
): Promise<string> => {
	const tar = await import("tar-stream");
	const zlib = await import("zlib");

	if (typeof path === "string") {
		if (!fs.existsSync(path)) {
			throw new Error("File does not exist");
		}
	}
	return new Promise((resolve, reject) => {
		try {
			const extract = tar.extract();
			let data = "";

			extract.on("entry", function (header, stream, cb) {
				stream.on("data", function (chunk) {
					if (header.name == "package/package.json") data += chunk;
				});

				stream.on("end", function () {
					cb();
				});

				stream.resume();
			});

			extract.on("finish", function () {
				const name = JSON.parse(data)?.name;
				if (!name) {
					reject(new Error("Could not find name from package.json file"));
				} else {
					resolve(name);
				}
			});

			extract.on("error", (e) => {
				reject(e);
			});

			if (typeof path === "string") {
				fs.createReadStream(path).pipe(zlib.createGunzip()).pipe(extract);
			} else {
				bufferToStream(path).pipe(zlib.createGunzip()).pipe(extract);
			}
		} catch (error) {
			reject(error);
		}
	});
};

export class NotFoundError extends Error {}
