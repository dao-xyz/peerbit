import { Keypair } from "@solana/web3.js"
import nacl from "tweetnacl";

export interface SignedPayload {
    payload: Uint8Array,
    signature: Uint8Array
}

export abstract class Trust {
    sign(payload: Uint8Array): SignedPayload {
        throw new Error("Not implemented")
    }

    // grant trust to another
    grantTrust(key: string): SignedPayload {
        throw new Error("Not implemented")
    }

    // revoke trust to another
    revokeTrust(key: string): SignedPayload {
        throw new Error("Not implemented")
    }
}

export class SolanaTrust extends Trust {
    keypair: Keypair;

    constructor(keypair: Keypair) {
        super();
        this.keypair = keypair;
    }

    sign(payload: Uint8Array): SignedPayload {
        return {
            payload,
            signature: nacl.sign(payload, this.keypair.secretKey)
        }
    }

    grantTrust(key: string): SignedPayload {
        throw new Error("Not implemented")
    }

    revokeTrust(key: string): SignedPayload {
        throw new Error("Not implemented")
    }

}


export const isAuthorized = (transaction: any) => {
    // simulate transaction? 

}