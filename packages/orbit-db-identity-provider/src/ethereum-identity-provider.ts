import { Wallet, verifyMessage } from '@ethersproject/wallet'
import { Identity, IdentitySerializable } from './identity';
import { IdentityProvider } from './identity-provider-interface';
import { joinUint8Arrays } from './utils';
const type = 'ethereum'

export type EthIdentityProviderOptions = { wallet?: Wallet };
export class EthIdentityProvider extends IdentityProvider {
  wallet: Wallet;
  constructor(options?: EthIdentityProviderOptions) {
    super()
    this.wallet = options?.wallet
  }

  // Returns the type of the identity provider
  static get type() { return type }

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

  static async verify(signature: Uint8Array, data: string | Uint8Array, publicKey: Uint8Array): Promise<boolean> {
    const signerAddress = verifyMessage(data, Buffer.from(signature).toString())
    return (signerAddress === Buffer.from(publicKey).toString())
  }

  static async verifyIdentity(identity: Identity | IdentitySerializable) {
    // Verify that identity was signed by the id
    return EthIdentityProvider.verify(identity.signatures.publicKey, joinUint8Arrays([identity.publicKey, identity.signatures.id]), identity.id)
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

