import { IPFS } from "ipfs-core-types";
import isNode from 'is-node';
import path from "path";
import { waitFor, waitForAsync } from '@dao-xyz/peerbit-time';

const validateEmail = (email) => {
    return String(email)
        .toLowerCase()
        .match(
            /^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/
        );
};

const createConfig = async (ipfs: IPFS, outputPath: string): Promise<{ domain: string }> => {
    if (!isNode) {
        throw new Error("Config can only be created with node");
    }
    const { fileURLToPath } = await import("url");

    const domain = await createDomain();
    const __filename = fileURLToPath(import.meta.url);
    const fs = await import('fs');
    let file = fs.readFileSync(path.join(__filename, '../nginx-template.conf'), 'utf-8');
    const ipfsId = (await ipfs.id()).id.toString()
    file = file.replaceAll("%IPFS_ID%", ipfsId)
    file = file.replaceAll("%DOMAIN%", domain)

    fs.mkdir(outputPath, { recursive: true }, (err) => {
        if (err) throw err;
    });

    await waitFor(() => fs.existsSync(outputPath))

    fs.writeFileSync(path.join(outputPath, 'default.conf'), file);
    return { domain }

}
const createDomain = async () => {
    const { exec } = await import('child_process');
    const { default: axios } = await import('axios')
    const ipv4: string = await new Promise((resolve, reject) => {
        exec("dig @resolver4.opendns.com myip.opendns.com +short", (error, stdout, stderr) => {
            if (error || stderr) {
                reject('DNS lookup failed');
            }
            resolve(stdout.trimEnd());
        });
    });
    const domain: string = (await axios.post("https://bfbbnhwpfj2ptcmurz6lit4xlu0vjajw.lambda-url.us-east-1.on.aws", ipv4, { headers: { 'Content-Type': 'application/json' } })).data.domain;
    return domain;
};


/**
 * 
 * @param ipfs 
 * @param email 
 * @param nginxConfigPath 
 * @param dockerProcessName 
 * @returns domain
 */
export const setupDomain = async (ipfs: IPFS, email: string, nginxConfigPath?: string, waitForUp: boolean = false, dockerProcessName: string = "nginx-certbot"): Promise<{ domain: string }> => {

    if (!validateEmail(email)) {
        throw new Error("Email for SSL renenewal is invalid")
    };

    const { exec } = await import('child_process');
    const pwd: string = await new Promise((resolve, reject) => {
        exec("pwd", (error, stdout, stderr) => {
            if (error || stderr) {
                reject("Failed to get current directory");
            }
            resolve(stdout.trimEnd());
        });
    });
    nginxConfigPath = path.join((nginxConfigPath || pwd), "nginx");
    const { domain } = await createConfig(ipfs, nginxConfigPath)


    // check if docker is installed
    const dockerExist = async () => {
        try {
            const out = await new Promise((resolve, reject) => {
                exec("docker --version", (error, stdout, stderr) => {
                    if (error || stderr) {
                        reject();
                    }
                    resolve(stdout);

                })
            });
            return true;
        } catch (error) {
            return false;
        }
    }

    if (!await dockerExist()) {
        await new Promise((resolve, reject) => {
            exec("sudo snap install docker", (error, stdout, stderr) => {
                if (error || stderr) {
                    reject();
                }
                resolve(stdout);

            });
        })

        try {
            await waitForAsync(() => dockerExist(), { timeout: 30 * 1000, delayInterval: 1000 })
        } catch (error) {
            throw new Error("Failed to install docker")
        }
    }

    // run
    const isTest = process.env.JEST_WORKER_ID !== undefined


    const certbotDockerCommand = `docker pull jonasal/nginx-certbot:latest && docker run -d --net=host \
    --env CERTBOT_EMAIL=${email} ${isTest ? "--env STAGING=1" : ""}\
    -v $(pwd)/nginx_secrets:/etc/letsencrypt \
    -v ${nginxConfigPath}:/etc/nginx/user_conf.d:ro \
    --name ${dockerProcessName} jonasal/nginx-certbot:latest`

    console.log('Starting Certbot');
    const result = await new Promise((resolve, reject) => {
        exec(certbotDockerCommand, (error, stdout, stderr) => {
            if (error || stderr) {
                reject('Failed to start docker container "jonasal/nginx-certbot:latest". ' + stderr);
            }
            resolve(stdout);

        });
    });

    console.log('Certbot started succesfully!');

    console.log("You domain is: ")
    console.log(domain);

    if (waitForUp) {
        const { default: axios } = await import("axios");

        console.log('Waiting for domain to be ready ...')
        await waitForAsync(async () => {
            try {
                const status = (await axios.get('https://' + domain)).status;
                return status >= 200 && status < 400
            } catch (error) {
                return false;
            }
        }, { timeout: 5 * 60 * 10000, delayInterval: 5000 })
        console.log('Domain is ready')
    }
    return { domain };
};
