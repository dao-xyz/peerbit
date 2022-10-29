import { Session } from '@dao-xyz/peerbit-test-utils';
import { setupDomain } from '../domain';
import { fileURLToPath } from "url";
import path from 'path';
import { exec } from 'child_process';
import { delay } from '@dao-xyz/peerbit-time';
const __filename = fileURLToPath(import.meta.url);

describe('ssl', () => {

    let session: Session;

    beforeAll(async () => {
        session = await Session.connected(1);
    })

    afterAll(async () => {
        await session.stop();
    })

    it('can setup domain', async () => {

        const containerName = 'nginx-certbot-' + +new Date;
        const { domain } = await setupDomain(session.peers[0].ipfs, 'marcus@dao.xyz', path.join(__filename, '../tmp/config'), false, containerName);
        expect(domain.length > 0).toBeTrue()
        const exist = (await new Promise((resolve, reject) => {
            exec("docker ps --format '{{.Names}}' | egrep '^" + containerName + "$'", (error, stdout, stderr) => {
                resolve(stdout.trimEnd());
                if (error || stderr) {
                    reject('Failed to check docker container exist');
                }
            });
        })) === containerName;
        expect(exist).toBeTrue();
        await new Promise((resolve, reject) => {
            exec("docker container stop " + containerName, (error, stdout, stderr) => {
                resolve(stdout.trimEnd());
                if (error || stderr) {
                    reject('Failed to check docker container exist');
                }
            });
        })
    })

});
