import http from "http";
import { PublicSignKey, fromBase64, toBase64 } from "@dao-xyz/peerbit-crypto";
import { Peerbit } from "@dao-xyz/peerbit";
import { serialize, deserialize } from "@dao-xyz/borsh";
import { Program, Address } from "@dao-xyz/peerbit-program";
import { IdentityRelation } from "@dao-xyz/peerbit-trusted-network";
import { multiaddr } from "@multiformats/multiaddr";
import { delay, waitFor } from "@dao-xyz/peerbit-time";
import { v4 as uuid } from "uuid";
import { Libp2p } from "libp2p";
import { DEFAULT_BLOCK_TRANSPORT_TOPIC } from "@dao-xyz/peerbit-block";
import { getNetwork } from "@dao-xyz/peerbit";
import { getConfigDir, getCredentialsPath, NotFoundError } from "./config.js";
import { setMaxListeners } from "events";
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

const IPFS_ID_PATH = "/peer/id";
const ADDRESSES_PATH = "/peer/addresses";
const TOPIC_PATH = "/topic";
const TOPICS_PATH = "/topics";
const PROGRAM_PATH = "/program";
const LIBRARY_PATH = "/library";
const NETWORK_PEER_PATH = "/network/peer";
const NETWORK_PEERS_PATH = "/network/peers";

export const checkExistPath = async (path: string) => {
    const fs = await import("fs");

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
export const createPassword = async (): Promise<string> => {
    const fs = await import("fs");
    const configDir = await getConfigDir();
    const credentialsPath = await getCredentialsPath(configDir);
    if (await checkExistPath(credentialsPath)) {
        throw new Error(
            "Config path for credentials: " +
                credentialsPath +
                ", already exist"
        );
    }
    console.log(`Creating config folder ${configDir}`);

    fs.mkdirSync(configDir, { recursive: true });
    await waitFor(() => fs.existsSync(configDir));

    console.log(`Created config folder ${configDir}`);

    const password = uuid();
    fs.writeFileSync(
        credentialsPath,
        JSON.stringify({ username: "admin", password })
    );
    console.log(`Created credentials at ${credentialsPath}`);
    return password;
};

export const loadPassword = async (): Promise<string> => {
    const fs = await import("fs");
    const configDir = await getConfigDir();
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

export const loadOrCreatePassword = async (): Promise<string> => {
    try {
        return await loadPassword();
    } catch (error) {
        if (error instanceof NotFoundError) {
            return createPassword();
        }
        throw error;
    }
};

export const startServer = async (
    client: Peerbit | Libp2p,
    port: number = LOCAL_PORT
): Promise<http.Server> => {
    const notPeerBitError =
        "Client is just a Libp2p node, not a full Peerbit client. The command is not supported for this node type";
    const libp2p = client instanceof Peerbit ? client.libp2p : client;

    // TODO for convinience we do this, but should we? Who might not like this
    // This is needed atm for all Peerbit apps, but not for other thinngs potentially
    let err: any = undefined;
    for (let i = 0; i < 3; i++) {
        try {
            await libp2p.pubsub.subscribe(DEFAULT_BLOCK_TRANSPORT_TOPIC);
            err = undefined;
            break;
        } catch (error) {
            err = error;
            await delay(5000);
        }
    }

    if (err) {
        throw err;
    }

    const password = await loadOrCreatePassword();

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
        if (client instanceof Peerbit) {
            const url = new URL(req.url, "http://localhost:" + port);
            const path = url.pathname
                .substring(
                    Math.min(1, url.pathname.length),
                    url.pathname.length
                )
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
        } else {
            throw new Error(notPeerBitError);
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
    const endpoints = (client: Peerbit | Libp2p): http.RequestListener => {
        return async (req, res) => {
            res.setHeader("Access-Control-Allow-Origin", "*");
            res.setHeader("Access-Control-Request-Method", "*");
            res.setHeader("Access-Control-Allow-Headers", "*");
            res.setHeader("Access-Control-Allow-Methods", "*");

            const r404 = () => {
                res.writeHead(404);
                res.end(e404);
            };

            try {
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
                                import("url").then((parse) => {
                                    const replicateParam = parse.parse(
                                        req.url!,
                                        true
                                    ).query["replicate"];
                                    if (Array.isArray(replicateParam)) {
                                        res.writeHead(400);
                                        res.end(
                                            "Expecting one replicate param"
                                        );
                                        return;
                                    }
                                    const replicate = replicateParam
                                        ? JSON.parse(replicateParam)
                                        : replicateParam;

                                    res.setHeader(
                                        "Content-Type",
                                        "application/json"
                                    );
                                    res.writeHead(200);
                                    if (replicate) {
                                        if (client instanceof Peerbit) {
                                            res.write(
                                                JSON.stringify([
                                                    ...client.programs.keys(),
                                                ])
                                            );
                                            res.end();
                                        } else {
                                            res.writeHead(400);
                                            res.write(notPeerBitError);
                                            res.end();
                                        }
                                    } else {
                                        res.write(
                                            JSON.stringify(
                                                libp2p.pubsub.getTopics()
                                            )
                                        );
                                        res.end();
                                    }
                                });

                                break;
                            default:
                                r404();
                                break;
                        }
                    } else if (req.url.startsWith(TOPIC_PATH)) {
                        switch (req.method) {
                            case "PUT":
                                getBody(req, (body) => {
                                    import("url").then((parse) => {
                                        const replicateParam = parse.parse(
                                            req.url!,
                                            true
                                        ).query["replicate"];
                                        if (Array.isArray(replicateParam)) {
                                            res.writeHead(400);
                                            res.end(
                                                "Expecting one replicate param"
                                            );
                                            return;
                                        }
                                        const replicate = replicateParam
                                            ? JSON.parse(replicateParam)
                                            : replicateParam;
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
                                        } else if (!replicate) {
                                            libp2p.pubsub.subscribe(topic);
                                            res.writeHead(200);
                                            res.end();
                                        } else {
                                            if (client instanceof Peerbit) {
                                                if (
                                                    client.programs.has(topic)
                                                ) {
                                                    res.writeHead(400);
                                                    res.end(
                                                        "Already subscribed to this topic"
                                                    );
                                                } else {
                                                    client
                                                        .subscribeToTopic(
                                                            topic,
                                                            true
                                                        )
                                                        .then(() => {
                                                            res.writeHead(200);
                                                            res.end();
                                                        });
                                                }
                                            } else {
                                                res.writeHead(400);
                                                res.write(notPeerBitError);
                                                res.end();
                                            }
                                        }
                                    });
                                });
                                break;
                            default:
                                r404();
                                break;
                        }
                    } else if (req.url.startsWith(PROGRAM_PATH)) {
                        const url = new URL(
                            req.url,
                            "http://localhost:" + port
                        );
                        if (client instanceof Peerbit === false) {
                            res.writeHead(400);
                            res.write(notPeerBitError);
                            res.end();
                        } else {
                            switch (req.method) {
                                case "GET":
                                    try {
                                        const program = getProgramFromPath(
                                            req,
                                            1
                                        );
                                        if (program) {
                                            res.writeHead(200);
                                            res.write(
                                                toBase64(serialize(program))
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
                                        const topic =
                                            url.searchParams.get("topic");
                                        if (topic && topic.length === 0) {
                                            res.writeHead(400);
                                            res.end("Invalid topic: " + topic);
                                        } else {
                                            try {
                                                const parsed = deserialize(
                                                    fromBase64(body),
                                                    Program
                                                );
                                                (client as Peerbit)
                                                    .open(parsed, {
                                                        topic:
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
                        }
                    } else if (req.url.startsWith(LIBRARY_PATH)) {
                        const url = new URL(
                            req.url,
                            "http://localhost:" + port
                        );
                        switch (req.method) {
                            case "PUT":
                                getBody(req, (body) => {
                                    const name = body;
                                    if (name && name.length === 0) {
                                        res.writeHead(400);
                                        res.end("Invalid library: " + name);
                                    } else {
                                        import(/* webpackIgnore: true */ name)
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
                                        const network = getNetwork(program);
                                        if (network) {
                                            res.setHeader(
                                                "Content-Type",
                                                "application/json"
                                            );
                                            res.writeHead(200);
                                            res.write(
                                                JSON.stringify(
                                                    [
                                                        ...network.trustGraph._index._index.values(),
                                                    ].map((x) =>
                                                        toBase64(
                                                            serialize(x.value)
                                                        )
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
                        const url = new URL(
                            req.url,
                            "http://localhost:" + port
                        );
                        //const path = url.pathname.substring(NETWORK_PEER_PATH.length, url.pathname.length).split("/");
                        switch (req.method) {
                            case "PUT":
                                getBody(req, (body) => {
                                    try {
                                        const program = getProgramFromPath(
                                            req,
                                            2
                                        );
                                        if (program) {
                                            const network = getNetwork(program);
                                            if (network) {
                                                try {
                                                    const reciever =
                                                        deserialize(
                                                            fromBase64(body),
                                                            PublicSignKey
                                                        );
                                                    network
                                                        .add(reciever)
                                                        .then((r) => {
                                                            res.writeHead(200);
                                                            res.end(
                                                                toBase64(
                                                                    serialize(r)
                                                                )
                                                            );
                                                        })
                                                        .catch(
                                                            (error?: any) => {
                                                                res.writeHead(
                                                                    400
                                                                );
                                                                res.end(
                                                                    "Failed to add relation: " +
                                                                        typeof error.message ===
                                                                        "string"
                                                                        ? error.message
                                                                        : JSON.stringify(
                                                                              error.message
                                                                          )
                                                                );
                                                            }
                                                        );
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
                        res.end(libp2p.peerId.toString());
                    } else if (req.url.startsWith(ADDRESSES_PATH)) {
                        res.setHeader("Content-Type", "application/json");
                        res.writeHead(200);
                        const addresses = libp2p
                            .getMultiaddrs()
                            .map((x) => x.toString());
                        res.end(JSON.stringify(addresses));
                    } else {
                        r404();
                    }
                } else {
                    r404();
                }
            } catch (error: any) {
                res.writeHead(500);
                console.error(error?.message);
                res.end("Unexpected error");
            }
        };
    };

    setMaxListeners(Infinity); // TODO make this better (lower and large enough)

    const server = http.createServer(endpoints(client));
    server.listen(port);
    server.on("error", (e) => {
        console.error("Server error: " + e?.message);
        import("fs").then((fs) => {
            fs.writeFile("error.log", JSON.stringify(e.message), function () {
                /* void */ 0;
            });
        });
    });
    server.on("close", () => {
        console.log("Exit server");
    });

    console.log("API available at port", port);
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
            authorization: "Basic admin:" + (await loadPassword()),
        };
        return headers;
    };
    return {
        peer: {
            id: {
                get: getId,
            },
            addresses: {
                get: async () => {
                    return (
                        throwIfNot200(
                            await axios.get(endpoint + ADDRESSES_PATH, {
                                validateStatus,
                                headers: await getHeaders(),
                            })
                        ).data as string[]
                    ).map((x) => multiaddr(x));
                },
            },
        },
        topic: {
            put: async (topic: string, replicate: boolean): Promise<void> => {
                throwIfNot200(
                    await axios.put(
                        endpoint + TOPIC_PATH + "?replicate=" + replicate,
                        topic,
                        {
                            validateStatus,
                            headers: await getHeaders(),
                        }
                    )
                );
            },
        },
        topics: {
            get: async (replicate: boolean): Promise<string[]> => {
                const result = throwIfNot200(
                    await axios.get(
                        endpoint + TOPICS_PATH + "?replicate=" + replicate,
                        {
                            validateStatus,
                            headers: await getHeaders(),
                        }
                    )
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
                    : deserialize(fromBase64(result), Program);
            },

            /**
             * @param program Program, or base64 string representation
             * @param topic, topic
             * @returns
             */
            put: async (
                program: Program | string,
                topic?: string
            ): Promise<Address> => {
                const base64 =
                    program instanceof Program
                        ? toBase64(serialize(program))
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
                              deserialize(fromBase64(r), IdentityRelation)
                          );
                },
            },
            peer: {
                put: async (
                    address: Address | string,
                    publicKey: PublicSignKey
                ): Promise<IdentityRelation> => {
                    const base64 = toBase64(serialize(publicKey));
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
                    return deserialize(fromBase64(resp.data), IdentityRelation);
                },
            },
        },
    };
};
