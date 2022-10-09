import { createStore, Keystore } from "../keystore.js"
import path from 'path';
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { dirname } from 'path';
import { fileURLToPath } from 'url';
const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);
export const fixturePath = path.join(__dirname, 'fixtures', 'signing-keys')

describe('setup fixture', () => {

    /*  it('replace fixture 1->10 keys', async () => {
         const store = await createStore(fixturePath) // storagePath
 
         const keystore = new Keystore(store)
         for (let i = 0; i < 10; i++) {
             await keystore.createKey(await Ed25519Keypair.create(), { id: new Uint8Array([i]), overwrite: true })
         }
         await store.close();
     }) */
})