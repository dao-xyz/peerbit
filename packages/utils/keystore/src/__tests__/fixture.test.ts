import { createStore, Keystore } from "../keystore.js"
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { delay } from "@dao-xyz/peerbit-time";
import { fixturePath } from './fixture.js';


describe('setup fixture', () => {

    it('_', () => { })
    /*  it('replace fixture 1->10 keys', async () => {
 
         const store = await createStore(fixturePath) // storagePath
 
         const keystore = new Keystore(store)
         for (let i = 0; i < 10; i++) {
             await keystore.createKey(await Ed25519Keypair.create(), { id: new Uint8Array([i]), overwrite: true })
         }
         await delay(3000);
         await store.close();
 
     }) */
})