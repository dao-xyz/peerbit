import IdentityProvider from "orbit-db-identity-provider/src/identity-provider-interface";
import { Keypair, PublicKey } from '@solana/web3.js';
import { Identity, IdentityProviderType } from "orbit-db-identity-provider";
import nacl from "tweetnacl";
import bs58 from 'bs58';



import AccessControllers from 'orbit-db-access-controllers';
import KeyValueStore from 'orbit-db-kvstore';
export const CONTRACT_ACCESS_CONTROLLER = 'contract-access-controller';
import OrbitDB from 'orbit-db';
import AccessController from "orbit-db-access-controllers/src/access-controller-interface";
/* import { Trust } from '@dao-xyz/orbit-trust'; */
class ContractAccessController extends AccessController {
    /*   trustProvider?: Trust; */
    _db: KeyValueStore<any>;
    constructor(options: { /* trustProvider?: Trust */ }) {
        super();
        /*    this.trustProvider = options.trustProvider; */

    }

    static get type() { return CONTRACT_ACCESS_CONTROLLER } // Return the type for this controller

    async canAppend(entry: LogEntry<any>, identityProvider: any) {
        // logic to determine if entry can be added, for example:
        /*  if (entry.payload === "hello world" && entry.identity.id === identity.id && identityProvider.verifyIdentity(entry.identity))
           return true */

        // Check identity
        /*   if (!identityProvider.verifyIdentity(entry.identity)) {
              return false;
          } */

        // Verify message is signed by gatekeeper!

        return true
    }

    async grant(capability: string, key: string) {
        // Merge current keys with the new key
        throw new Error("Not supported, this is DAOs job")
    }

    async revoke(capability, key) {
        throw new Error("Not supported, this is DAOs job")
    }


    async save() {
        // return parameters needed for loading
        return { parameter: 'some-parameter-needed-for-loading' }
    }
    async load(address: string) {
        await super.load(address);
        /*      if (address) {
                 try {
                     if (address.indexOf('/ipfs') === 0) { address = address.split('/')[2] }
                     const access = await io.read(this._ipfs, address)
                     this.contractAddress = access.contractAddress
                     this.abi = JSON.parse(access.abi)
                 } catch (e) {
                     console.log('ContractAccessController.load ERROR:', e)
                 }
             }
             this.contract = new this.web3.eth.Contract(this.abi, this.contractAddress) */
    }

    static async create(_orbitdb: OrbitDB, options: {/*  trustProvider: Trust */ }) {
        return new ContractAccessController({/*  trustProvider: options.trustProvider */ })
    }
}

AccessControllers.addAccessController({ AccessController: ContractAccessController })


export class SolanaIdentityProvider extends IdentityProvider {

    wallet: PublicKey;
    keypair: Keypair | undefined;
    signer: ((data: Uint8Array) => any) | undefined
    constructor(options: { wallet: PublicKey, keypair?: Keypair, signer?: (data: Uint8Array) => any }) {
        super()
        this.wallet = options.wallet;
        this.keypair = options.keypair;
        this.signer = options.signer;
    }

    // Returns the type of the identity provider
    static get type(): IdentityProviderType {
        return 'solana'
    }

    // Returns the signer's id
    async getId(): Promise<string> {
        return this.wallet.toBase58();
    }

    // Returns a signature of pubkeysignature
    async signIdentity(data: string) {
        if (!this.signer && !this.keypair) {
            throw new Error("This identity to not support signing")
        }

        let array = Uint8Array.from(Buffer.from(data));
        return this.signer ? this.signer(array) : nacl.sign(array, this.keypair.secretKey)
    }

    static async verifyIdentity(identity: Identity) {

        // Verify that identity was signed by the id
        const signedKey = nacl.sign.open(Uint8Array.from(Buffer.from(identity.signatures.publicKey)), bs58.decode(identity.id));
        let verified = nacl.verify(signedKey, Uint8Array.from(Buffer.from(identity.publicKey + identity.signatures.id)));
        return verified
    }
}
