import path from "path";
import os from "os";
import fs from "fs";

export const getHomeConfigDir = async (): Promise<string> => {
	const configDir = path.join(os.homedir(), ".peerbit");
	return configDir;
};

export const getCredentialsPath = async (
	configDir: string
): Promise<string> => {
	return path.join(configDir, "credentials");
};

export const getKeysPath = async (configDir: string): Promise<string> => {
	return path.join(configDir, "keys");
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

export const loadPassword = async (): Promise<string> => {
	const configDir = await getHomeConfigDir();
	const credentialsPath = await getCredentialsPath(configDir);
	if (!(await checkExistPath(credentialsPath))) {
		throw new NotFoundError("Credentials file does not exist");
	}
	const password = JSON.parse(
		fs.readFileSync(credentialsPath, "utf-8")
	).password;
	if (!password || password.length === 0) {
		throw new NotFoundError("Password not found");
	}
	return password;
};

export const getPackageName = async (path: string): Promise<string> => {
	const tar = await import("tar-stream");
	const zlib = await import("zlib");

	if (!fs.existsSync(path)) {
		throw new Error("File does not exist");
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

			fs.createReadStream(path).pipe(zlib.createGunzip()).pipe(extract);
		} catch (error) {
			reject(error);
		}
	});
};

export class NotFoundError extends Error {}
