import { type ProgramClient } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import dotenv from "dotenv";
import type http from "http";
import path from "path";
import { getTrustPath } from "../src/config.js";
import { getDomainFromConfig } from "../src/domain.js";
import { startApiServer } from "../src/server.js";
import { Trust } from "../src/trust.js";

describe("getDomainFromConfig", () => {
	before(() => {
		dotenv.config();
	});

	it("%DOMAIN%", async () => {
		const config =
			" ssl_certificate         /etc/letsencrypt/live/%DOMAIN%/fullchain.pem; \nssl_certificate_key     /etc/letsencrypt/live/%DOMAIN%/privkey.pem; ";
		const domain = await getDomainFromConfig(config);
		expect(domain).equal(undefined);
	});

	it("specified domain", async () => {
		const expectedDomain = "a.b-c.d";
		const config = ` ssl_certificate         /etc/letsencrypt/live/${expectedDomain}/fullchain.pem; \nssl_certificate_key     /etc/letsencrypt/live/${expectedDomain}/privkey.pem; `;
		const domain = await getDomainFromConfig(config);
		expect(domain).equal(expectedDomain);
	});
});

describe("ssl", () => {
	before(() => {
		dotenv.config();
	});
	let session: TestSession, peer: ProgramClient, server: http.Server;

	before(async () => {
		const directory = "./tmp/peerbit/" + +new Date();
		session = await TestSession.connected(1, {
			directory: path.join(directory, "node"),
		});
		peer = session.peers[0];
		server = await startApiServer(peer, {
			trust: new Trust(getTrustPath(directory)),
			port: 12345,
		});
	});

	after(async () => {
		await peer.stop();
		await session.stop();
		await server.close();
	});

	it("_", () => {});
	/* These test are flaky, or have side effects, and should not be running in ci yet
	it("certbot", async () => {
		const { exec } = await import("child_process");
		const containerName = "nginx-certbot-" + +new Date();
		const domain = await createTestDomain();
		await startCertbot(
			domain,
			"marcus@dao.xyz",
			path.join(__filename, "../tmp/config"),
			false,
			containerName
		);
		expect(domain.length > 0).to.be.true;
		const exist =
			(await new Promise((resolve, reject) => {
				exec(
					"docker ps --format '{{.Names}}' | egrep '^" +
						containerName +
						"$'",
					(error, stdout, stderr) => {
						resolve(stdout.trimEnd());
						if (error || stderr) {
							reject("Failed to check docker container exist");
						}
					}
				);
			})) === containerName;
		expect(exist).to.be.true;
		await new Promise((resolve, reject) => {
			exec(
				"docker container stop " + containerName,
				(error, stdout, stderr) => {
					resolve(stdout.trimEnd());
					if (error || stderr) {
						reject("Failed to check docker container exist");
					}
				}
			);
		});
	});
 
	it("can create aws record", async () => {
		const ak = process.env.TEST_AWS_ACCESS_KEY_ID;
		const sk = process.env.TEST_AWS_SECREY_ACCESS_KEY;
		const subdomain = uuid();
		await createRecord({
			domain: subdomain + ".peerchecker.com",
			hostedZoneId: "Z0762538EEV3HRTQOXY3",
			credentials:
				ak && sk
					? {
						  accessKeyId: ak,
						  secretAccessKey: sk,
					  }
					: undefined,
		});
	}); */
});
