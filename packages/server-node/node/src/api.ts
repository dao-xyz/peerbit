import http from "http";
import { PublicSignKey } from "@dao-xyz/peerbit-crypto";
import { Peerbit, inNetwork } from "@dao-xyz/peerbit";
import { serialize, deserialize } from "@dao-xyz/borsh";
import { Program, Address } from "@dao-xyz/peerbit-program";
import { IdentityRelation } from "@dao-xyz/peerbit-trusted-network";
import { multiaddr } from "@multiformats/multiaddr";
import { waitFor } from "@dao-xyz/peerbit-time";
import { v4 as uuid } from "uuid";
export const LOCAL_PORT = 8082;
export const SSL_PORT = 9002;
export const getPort = (protocol: string) => {
    if (protocol === "https:") {
        return SSL_PORT;
    }

    if (protocol === "http:") {
        return LOCAL_PORT;
    }

    throw new Error("Unsupported protocol: " + protocol);
};

const IPFS_ID_PATH = "/ipfs/id";
const IPFS_ADDRESSES_PATH = "/ipfs/addresses";
const TOPIC_PATH = "/topic";
const TOPICS_PATH = "/topics";
const PROGRAM_PATH = "/program";
const LIBRARY_PATH = "/library";
const NETWORK_PEER_PATH = "/network/peer";
const NETWORK_PEERS_PATH = "/network/peers";

const getConfigDir = async (ipfsId: string): Promise<string> => {
    const path = await import("path");
    const os = await import("os");
    const configDir = path.join(os.homedir(), ".peerbit", ipfsId);
    return configDir;
};

const getCredentialsPath = async (configDir: string): Promise<string> => {
    const path = await import("path");
    return path.join(configDir, "credentials");
};

class NotFoundError extends Error {}

export const createPassword = async (ipfsId: string): Promise<string> => {
    const fs = await import("fs");
    const configDir = await getConfigDir(ipfsId);
    const credentialsPath = await getCredentialsPath(configDir);
    if (fs.existsSync(credentialsPath)) {
        throw new Error(
            "Config path for credentials: " +
                credentialsPath +
                ", already exist"
        );
    }
    fs.mkdir(configDir, { recursive: true }, (err) => {
        if (err) throw err;
    });

    await waitFor(() => fs.existsSync(configDir));
    const password = uuid();
    fs.writeFileSync(
        credentialsPath,
        JSON.stringify({ username: "admin", password })
    );
    return password;
};

export const loadPassword = async (ipfsId: string): Promise<string> => {
    const fs = await import("fs");
    const configDir = await getConfigDir(ipfsId);
    const credentialsPath = await getCredentialsPath(configDir);
    if (!fs.existsSync(credentialsPath)) {
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

export const loadOrCreatePassword = async (ipfsId: string): Promise<string> => {
    try {
        return await loadPassword(ipfsId);
    } catch (error) {
        if (error instanceof NotFoundError) {
            return createPassword(ipfsId);
        }
        throw error;
    }
};

export const startServer = async (
    client: Peerbit,
    port: number = LOCAL_PORT
): Promise<http.Server> => {
    type ACL = (req: http.IncomingMessage) => boolean;
    const password = await loadOrCreatePassword(
        (await client.ipfs.id()).id.toString()
    );
    const adminACL = (req: http.IncomingMessage): boolean => {
        const auth = req.headers["authorization"];
        if (!auth?.startsWith("Basic ")) {
            return false;
        }
        const credentials = auth?.substring("Basic ".length);
        const username = credentials.split(":")[0];
        if (username !== "admin") {
            return false;
        }
        if (password !== credentials.substring(username.length + 1)) {
            return false;
        }
        return true;
    };

    const getProgramFromPath = (
        req: http.IncomingMessage,
        pathIndex: number
    ): Program | undefined => {
        if (!req.url) {
            throw new Error("Missing url");
        }

        const url = new URL(req.url, "http://localhost:" + port);
        const path = url.pathname
            .substring(Math.min(1, url.pathname.length), url.pathname.length)
            .split("/");
        if (path.length <= pathIndex) {
            throw new Error("Invalid path");
        }
        const address = decodeURIComponent(path[pathIndex]);
        for (const [topic, programs] of client.programs.entries()) {
            {
                const p = programs.get(address);
                if (p) {
                    return p.program;
                }
            }
        }
        return;
    };
    const getBody = (
        req: http.IncomingMessage,
        callback: (body: string) => void
    ) => {
        let body = "";
        req.on("data", function (d) {
            body += d;
        });
        req.on("end", function () {
            callback(body);
        });
    };

    const e404 = "404";
    const endpoints = (client: Peerbit, acl?: ACL): http.RequestListener => {
        return async (req, res) => {
            res.setHeader("Access-Control-Allow-Origin", "*");
            res.setHeader("Access-Control-Request-Method", "*");
            res.setHeader("Access-Control-Allow-Headers", "*");
            res.setHeader("Access-Control-Allow-Methods", "*");

            const r404 = () => {
                res.writeHead(404);
                res.end(e404);
            };
            if (req.url) {
                if (
                    !req.url.startsWith(IPFS_ID_PATH) &&
                    !(await adminACL(req))
                ) {
                    res.writeHead(401);
                    res.end("Not authorized");
                    return;
                } else if (req.url.startsWith(TOPICS_PATH)) {
                    switch (req.method) {
                        case "GET":
                            res.setHeader("Content-Type", "application/json");
                            res.writeHead(200);
                            res.write(
                                JSON.stringify([...client.programs.keys()])
                            );
                            res.end();
                            break;
                        default:
                            r404();
                            break;
                    }
                } else if (req.url.startsWith(TOPIC_PATH)) {
                    switch (req.method) {
                        case "PUT":
                            getBody(req, (body) => {
                                const topic = body;
                                if (
                                    typeof topic !== "string" ||
                                    topic.trim().length !== topic.length
                                ) {
                                    res.writeHead(400);
                                    res.end(
                                        "Invalid topic: " +
                                            JSON.stringify(topic)
                                    );
                                } else if (client.programs.has(topic)) {
                                    res.writeHead(400);
                                    res.end("Already subscribed to this topic");
                                } else {
                                    client
                                        .subscribeToReplicationTopic(topic)
                                        .then(() => {
                                            res.writeHead(200);
                                            res.end();
                                        });
                                }
                            });
                            break;
                        default:
                            r404();
                            break;
                    }
                } else if (req.url.startsWith(PROGRAM_PATH)) {
                    const url = new URL(req.url, "http://localhost:" + port);
                    switch (req.method) {
                        case "GET":
                            try {
                                const program = getProgramFromPath(req, 1);
                                if (program) {
                                    res.writeHead(200);
                                    res.write(
                                        Buffer.from(
                                            serialize(program)
                                        ).toString("base64")
                                    );
                                    res.end();
                                } else {
                                    res.writeHead(404);
                                    res.end();
                                }
                            } catch (error: any) {
                                res.writeHead(404);
                                res.end(error.message);
                            }
                            break;

                        case "PUT":
                            getBody(req, (body) => {
                                const topic = url.searchParams.get("topic");
                                if (topic && topic.length === 0) {
                                    res.writeHead(400);
                                    res.end("Invalid topic: " + topic);
                                } else {
                                    try {
                                        const parsed = deserialize(
                                            new Uint8Array(
                                                Buffer.from(body, "base64")
                                            ),
                                            Program
                                        );
                                        client
                                            .open(parsed, {
                                                replicationTopic:
                                                    topic || undefined,
                                            })
                                            .then((program) => {
                                                res.writeHead(200);
                                                res.end(
                                                    program.address.toString()
                                                );
                                            })
                                            .catch((error) => {
                                                res.writeHead(400);
                                                res.end(
                                                    "Failed to open program: " +
                                                        error.toString()
                                                );
                                            });
                                    } catch (error) {
                                        res.writeHead(400);
                                        res.end(
                                            "Invalid base64 program binary"
                                        );
                                    }
                                }
                            });
                            break;

                        default:
                            r404();
                            break;
                    }
                } else if (req.url.startsWith(LIBRARY_PATH)) {
                    const url = new URL(req.url, "http://localhost:" + port);
                    switch (req.method) {
                        case "PUT":
                            getBody(req, (body) => {
                                const name = body;
                                if (name && name.length === 0) {
                                    res.writeHead(400);
                                    res.end("Invalid library: " + name);
                                } else {
                                    import(name)
                                        .then(() => {
                                            res.writeHead(200);
                                            res.end();
                                        })
                                        .catch((e) => {
                                            res.writeHead(400);
                                            res.end(e.message.toString?.());
                                        });
                                }
                            });
                            break;

                        default:
                            r404();
                            break;
                    }
                } else if (req.url.startsWith(NETWORK_PEERS_PATH)) {
                    switch (req.method) {
                        case "GET":
                            try {
                                const program = getProgramFromPath(req, 2);
                                if (program) {
                                    if (inNetwork(program)) {
                                        res.setHeader(
                                            "Content-Type",
                                            "application/json"
                                        );
                                        res.writeHead(200);
                                        res.write(
                                            JSON.stringify(
                                                [
                                                    ...program.network.trustGraph._index._index.values(),
                                                ].map((x) =>
                                                    Buffer.from(
                                                        serialize(x.value)
                                                    ).toString("base64")
                                                )
                                            )
                                        );
                                        res.end();
                                    } else {
                                        res.writeHead(400);
                                        res.end("Program is not in a VPC");
                                    }
                                } else {
                                    res.writeHead(404);
                                    res.end();
                                }
                            } catch (error: any) {
                                res.writeHead(404);
                                res.end(error.message);
                            }
                            break;

                        default:
                            r404();
                            break;
                    }
                } else if (req.url.startsWith(NETWORK_PEER_PATH)) {
                    const url = new URL(req.url, "http://localhost:" + port);
                    //const path = url.pathname.substring(NETWORK_PEER_PATH.length, url.pathname.length).split("/");
                    switch (req.method) {
                        case "PUT":
                            getBody(req, (body) => {
                                try {
                                    const program = getProgramFromPath(req, 2);
                                    if (program) {
                                        if (inNetwork(program)) {
                                            try {
                                                const reciever = deserialize(
                                                    new Uint8Array(
                                                        Buffer.from(
                                                            body,
                                                            "base64"
                                                        )
                                                    ),
                                                    PublicSignKey
                                                );
                                                program.network
                                                    .add(reciever)
                                                    .then((r) => {
                                                        res.writeHead(200);
                                                        res.end(
                                                            Buffer.from(
                                                                serialize(r)
                                                            ).toString("base64")
                                                        );
                                                    })
                                                    .catch((error?: any) => {
                                                        res.writeHead(400);
                                                        res.end(
                                                            "Failed to add relation: " +
                                                                typeof error.message ===
                                                                "string"
                                                                ? error.message
                                                                : JSON.stringify(
                                                                      error.message
                                                                  )
                                                        );
                                                    });
                                            } catch (error) {
                                                res.writeHead(400);
                                                res.end(
                                                    "Invalid base64 program binary"
                                                );
                                            }
                                        }
                                    } else {
                                        res.writeHead(404);
                                        res.end();
                                    }
                                } catch (error: any) {
                                    res.writeHead(404);
                                    res.end(error.message);
                                }
                            });
                            break;

                        default:
                            r404();
                            break;
                    }
                } else if (req.url.startsWith(IPFS_ID_PATH)) {
                    res.writeHead(200);
                    res.end((await client.ipfs.id()).id.toString());
                } else if (req.url.startsWith(IPFS_ADDRESSES_PATH)) {
                    res.setHeader("Content-Type", "application/json");
                    res.writeHead(200);
                    const addresses = (await client.ipfs.id()).addresses.map(
                        (x) => x.toString()
                    );
                    res.end(JSON.stringify(addresses));
                } else {
                    r404();
                }
            } else {
                r404();
            }
        };
    };

    const server = http.createServer(endpoints(client));
    server.listen(port);
    console.log("API server accessible at port", port);
    return server;
};

export const client = async (
    endpoint: string = "http://localhost:" + LOCAL_PORT
) => {
    const { default: axios } = await import("axios");

    const validateStatus = (status: number) => {
        return (status >= 200 && status < 300) || status == 404;
    };

    const throwIfNot200 = (resp: { status: number; data: any }) => {
        if (resp.status !== 200) {
            throw new Error(resp.data);
        }
        return resp;
    };
    const getBodyByStatus = <
        D extends { toString(): string },
        T extends { status: number; data: D }
    >(
        resp: T
    ): D | undefined => {
        if (resp.status === 404) {
            return;
        }
        if (resp.status == 200) {
            return resp.data;
        }
        throw new Error(
            typeof resp.data === "string" ? resp.data : resp.data.toString()
        );
    };
    const getId = async () =>
        throwIfNot200(
            await axios.get(endpoint + IPFS_ID_PATH, { validateStatus })
        ).data;
    const getHeaders = async () => {
        const headers = {
            authorization: "Basic admin:" + (await loadPassword(await getId())),
        };
        return headers;
    };
    return {
        ipfs: {
            id: {
                get: getId,
            },
            addresses: {
                get: async () => {
                    return (
                        throwIfNot200(
                            await axios.get(endpoint + IPFS_ADDRESSES_PATH, {
                                validateStatus,
                                headers: await getHeaders(),
                            })
                        ).data as string[]
                    ).map((x) => multiaddr(x));
                },
            },
        },
        topic: {
            put: async (topic: string): Promise<void> => {
                throwIfNot200(
                    await axios.put(endpoint + TOPIC_PATH, topic, {
                        validateStatus,
                        headers: await getHeaders(),
                    })
                );
            },
        },
        topics: {
            get: async (): Promise<string[]> => {
                const result = throwIfNot200(
                    await axios.get(endpoint + TOPICS_PATH, {
                        validateStatus,
                        headers: await getHeaders(),
                    })
                );
                return result.data as string[];
            },
        },
        program: {
            get: async (
                address: Address | string
            ): Promise<Program | undefined> => {
                const result = getBodyByStatus<string, any>(
                    await axios.get(
                        endpoint +
                            PROGRAM_PATH +
                            "/" +
                            encodeURIComponent(address.toString()),
                        { validateStatus, headers: await getHeaders() }
                    )
                );
                return !result
                    ? undefined
                    : deserialize(
                          new Uint8Array(Buffer.from(result, "base64")),
                          Program
                      );
            },

            /**
             * @param program Program, or base64 string representation
             * @param topic, replicationTopic
             * @returns
             */
            put: async (
                program: Program | string,
                topic?: string
            ): Promise<Address> => {
                const base64 =
                    program instanceof Program
                        ? Buffer.from(serialize(program)).toString("base64")
                        : program;
                const resp = throwIfNot200(
                    await axios.put(
                        endpoint +
                            PROGRAM_PATH +
                            (topic ? "?topic=" + topic : ""),
                        base64,
                        { validateStatus, headers: await getHeaders() }
                    )
                );
                return Address.parse(resp.data);
            },
        },
        library: {
            put: async (name: string): Promise<void> => {
                throwIfNot200(
                    await axios.put(endpoint + LIBRARY_PATH, name, {
                        validateStatus,
                        headers: await getHeaders(),
                    })
                );
                return;
            },
        },

        network: {
            peers: {
                get: async (
                    address: Address | string
                ): Promise<IdentityRelation[] | undefined> => {
                    const result = getBodyByStatus(
                        await axios.get(
                            endpoint +
                                NETWORK_PEERS_PATH +
                                "/" +
                                encodeURIComponent(address.toString()),
                            { validateStatus, headers: await getHeaders() }
                        )
                    );
                    return !result
                        ? undefined
                        : (result as string[]).map((r) =>
                              deserialize(
                                  Buffer.from(r, "base64"),
                                  IdentityRelation
                              )
                          );
                },
            },
            peer: {
                put: async (
                    address: Address | string,
                    publicKey: PublicSignKey
                ): Promise<IdentityRelation> => {
                    const base64 = Buffer.from(serialize(publicKey)).toString(
                        "base64"
                    );
                    const resp = throwIfNot200(
                        await axios.put(
                            endpoint +
                                NETWORK_PEER_PATH +
                                "/" +
                                encodeURIComponent(address.toString()),
                            base64,
                            { validateStatus, headers: await getHeaders() }
                        )
                    );
                    return deserialize(
                        Buffer.from(resp.data, "base64"),
                        IdentityRelation
                    );
                },
            },
        },
    };
};
