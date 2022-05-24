
import { AnyPeer } from '../../index';
import * as ipfs from 'ipfs';
import fs from 'fs';
import Identities from 'orbit-db-identity-provider';
import { SolanaIdentityProvider } from '../solana-identity-provider';
import { Keypair } from '@solana/web3.js';

describe('node', () => {
    test('keypair signer', async () => {
        let root = './ipfs';
        fs.rmSync(root, { recursive: true, force: true });
        Identities.addIdentityProvider(SolanaIdentityProvider)
        let keypair = Keypair.generate();
        const identity = await Identities.createIdentity({ type: 'solana', wallet: keypair.publicKey, keypair: keypair })
        const blobby = new AnyPeer();
        /*   await blobby.create({
              local: false,
              repo: root,
              identity
          })
   */
        /*     await blobby.addNewPost({
                content: 'hello'
            });
            await blobby.disconnect(); */

    })
});

