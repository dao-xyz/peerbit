import { Wallet, verifyMessage } from '@ethersproject/wallet'
import { IdentityAsJson } from './identity';
import { IdentityProvider } from './identity-provider-interface';
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
    return this.wallet.getAddress()
  }

  // Returns a signature of pubkeysignature
  async sign(data, options = {}) {
    const wallet = this.wallet
    if (!wallet) { throw new Error('wallet is required') }

    return wallet.signMessage(data)
  }

  static async verify(signature: string, data: string | Uint8Array, publicKey: string): Promise<boolean> {
    const signerAddress = verifyMessage(data, signature)
    return (signerAddress === publicKey)
  }

  static async verifyIdentity(identity: IdentityAsJson) {
    // Verify that identity was signed by the id
    return EthIdentityProvider.verify(identity.signatures.publicKey, identity.publicKey + identity.signatures.id, identity.id)
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

