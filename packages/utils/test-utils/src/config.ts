
import path from 'path'
import * as ipfs from 'ipfs';
import * as ipfsModule from 'ipfs'
import * as ipfsHttpModule from 'ipfs-http-client'
import * as ipfsBin from 'go-ipfs'

export const factoryConfig = {
    defaults: {
        type: 'proc',
        test: true,
        disposable: true,
        ipfsModule: ipfsModule,
        init: false,
        start: false,
        ipfsOptions: {
            config: {
                Addresses: {
                    API: '/ip4/127.0.0.1/tcp/0',
                    Swarm: ['/ip4/0.0.0.0/tcp/0'],
                    Gateway: '/ip4/0.0.0.0/tcp/0'
                },
                Bootstrap: []
            }
        }
    },
    overrides: {
        go: {
            test: false,
            ipfsHttpModule: ipfsHttpModule,
            ipfsBin: ipfsBin
        }
    }
}

export const browserConfig = {
    timeout: 30000,
    identityKeyFixtures: (dir) => path.resolve(dir, 'fixtures/keys/identity-keys'),
    signingKeyFixtures: (dir) => path.resolve(dir, 'fixtures/keys/signing-keys'),
    identityKeysPath: './orbitdb/identity/identitykeys',
    signingKeysPath: './orbitdb/identity/signingkeys',
    defaultIpfsConfig: {
        preload: {
            enabled: false
        },
        EXPERIMENTAL: {
            pubsub: true
        },
        config: {
            Addresses: {
                API: '/ip4/127.0.0.1/tcp/0',
                Swarm: [],
                Gateway: '/ip4/0.0.0.0/tcp/0'
            },
            Bootstrap: [],
            Discovery: {
                MDNS: {
                    Enabled: true,
                    Interval: 0
                },
                webRTCStar: {
                    Enabled: false
                }
            }
        }
    },
    daemon1: {
        relay: { enabled: true, hop: { enabled: true, active: true } },
        EXPERIMENTAL: {
            pubsub: true
        },
        config: {
            Addresses: {
                API: '/ip4/127.0.0.1/tcp/0',
                Swarm: ['/dns4/wrtc-star1.par.dwebops.pub/tcp/443/wss/p2p-webrtc-star'],
                Gateway: '/ip4/0.0.0.0/tcp/0'
            },
            Bootstrap: [],
            Discovery: {
                MDNS: {
                    Enabled: true,
                    Interval: 10
                },
                webRTCStar: {
                    Enabled: true
                }
            }
        }
    },
    daemon2: {
        relay: { enabled: true, hop: { enabled: true, active: true } },
        EXPERIMENTAL: {
            pubsub: true
        },
        config: {
            Addresses: {
                API: '/ip4/127.0.0.1/tcp/0',
                Swarm: ['/dns4/wrtc-star1.par.dwebops.pub/tcp/443/wss/p2p-webrtc-star'],
                Gateway: '/ip4/0.0.0.0/tcp/0'
            },
            Bootstrap: [],
            Discovery: {
                MDNS: {
                    Enabled: true,
                    Interval: 10
                },
                webRTCStar: {
                    Enabled: true
                }
            }
        }
    }
}


export const nodeConfig = {
    timeout: 30000,
    identityKeyFixtures: (dir) => path.resolve(dir, 'fixtures/keys/identity-keys'),
    signingKeyFixtures: (dir) => path.resolve(dir, 'fixtures/keys/signing-keys'),
    identityKeysPath: './orbitdb/identity/identitykeys',
    signingKeysPath: './orbitdb/identity/signingkeys',
    defaultIpfsConfig: {
        preload: {
            enabled: false
        },
        EXPERIMENTAL: {
            pubsub: true
        },
        config: {
            Addresses: {
                API: '/ip4/127.0.0.1/tcp/0',
                Swarm: ['/ip4/0.0.0.0/tcp/0'],
                Gateway: '/ip4/0.0.0.0/tcp/0'
            },
            Bootstrap: [],
            Discovery: {
                MDNS: {
                    Enabled: true,
                    Interval: 0
                },
                webRTCStar: {
                    Enabled: false
                }
            }
        }
    },
    daemon1: {
        EXPERIMENTAL: {
            pubsub: true
        },
        config: {
            Addresses: {
                API: '/ip4/127.0.0.1/tcp/0',
                Swarm: ['/ip4/0.0.0.0/tcp/0'],
                Gateway: '/ip4/0.0.0.0/tcp/0'
            },
            Bootstrap: [],
            Discovery: {
                MDNS: {
                    Enabled: true,
                    Interval: 0
                },
                webRTCStar: {
                    Enabled: false
                }
            }
        }
    },
    daemon2: {
        EXPERIMENTAL: {
            pubsub: true
        },
        config: {
            Addresses: {
                API: '/ip4/127.0.0.1/tcp/0',
                Swarm: ['/ip4/0.0.0.0/tcp/0'],
                Gateway: '/ip4/0.0.0.0/tcp/0'
            },
            Bootstrap: [],
            Discovery: {
                MDNS: {
                    Enabled: true,
                    Interval: 0
                },
                webRTCStar: {
                    Enabled: false
                }
            }
        }
    }
}
