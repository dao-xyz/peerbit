import { Wallet, verifyMessage } from '@ethersproject/wallet'
import { Identity } from './identity';
import { IdentityProvider } from './identity-provider-interface';
import { joinUint8Arrays } from '@dao-xyz/borsh-utils';
import { Secp256k1PublicKey, verifySignatureSecp256k1 } from '@dao-xyz/peerbit-crypto';

export type EthIdentityProviderOptions = { wallet?: Wallet };
export class EthIdentityProvider extends IdentityProvider {
  wallet: Wallet;
  constructor(options?: EthIdentityProviderOptions) {
    super()
    this.wallet = options?.wallet
  }

  // Returns the type of the identity provider
  static get type() { return 'ethereum' }

  // Returns the signer's id
  async getId(options = {}) {
    if (!this.wallet) {
      this.wallet = await this._createWallet(options)
    }
    return new Uint8Array(Buffer.from(await this.wallet.getAddress()))
  }

  // Returns a signature of pubkeysignature
  async sign(data: Uint8Array, options = {}) {
    const wallet = this.wallet
    if (!wallet) { throw new Error('wallet is required') }

    return new Uint8Array(Buffer.from((await wallet.signMessage(data))))
  }

  /*   static async verify(signature: Uint8Array, data: string | Uint8Array, publicKey: Uint8Array): Promise<boolean> {
      const signerAddress = verifyMessage(data, Buffer.from(signature).toString())
      return (signerAddress === Buffer.from(publicKey).toString())
    } */

  static async verifyIdentity(identity: Identity) {
    // Verify that identity was signed by the id
    if (identity.id instanceof Secp256k1PublicKey) {
      return verifySignatureSecp256k1(identity.signatures.publicKey, identity.id, Buffer.concat([identity.publicKey.getBuffer(), identity.signatures.id])) // EthIdentityProvider.verify(identity.signatures.publicKey, Buffer.concat([identity.publicKey.getBuffer(), identity.signatures.id]), identity.id)
    }
    return false;
  }

  async _createWallet(options?: { encryptedJsonOpts?: { progressCallback: any, json: any, password: any }, mnemonicOpts?: { mnemonic: any, path: any, wordlist: any } }) {
    if (options?.mnemonicOpts) {
      if (!options.mnemonicOpts.mnemonic) {
        throw new Error('mnemonic is required')
      }
      return Wallet.fromMnemonic(options.mnemonicOpts.mnemonic, options.mnemonicOpts.path, options.mnemonicOpts.wordlist)
    }
    if (options?.encryptedJsonOpts) {
      if (!options.encryptedJsonOpts.json) {
        throw new Error('encrypted json is required')
      }
      if (!options.encryptedJsonOpts.password) {
        throw new Error('password for encrypted json is required')
      }
      return Wallet.fromEncryptedJson(options.encryptedJsonOpts.json, options.encryptedJsonOpts.password, options.encryptedJsonOpts.progressCallback)
    }
    return Wallet.createRandom()
  }
}

