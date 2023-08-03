import { createLibp2p } from "libp2p"
import { Keychain } from ".."

describe('index', () => {
    let libp2p: any

    beforeEach(async () => {

        libp2p = createLibp2p<{ keychain: Keychain }>({
            services: {
                keychain: (components) => new Keychain(components)
            }
        })
    })
})