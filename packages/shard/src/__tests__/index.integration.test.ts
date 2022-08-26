
import * as ipfs from 'ipfs';
import fs from 'fs';

describe('node', () => {
  /*   it('local', async () => {
      const blobby = new Blobby();
      await blobby.create(true);
      let cid = await blobby.addNewPiece("QmNR2n4zywCV61MeMLB6JwPueAPqheqpfiA4fLPMxouEmQ")
      let content = await blobby.node.dag.get(ipfs.CID.parse(cid));
      expect(content).toBeDefined();
      await blobby.disconnect();
    });
   */
  it('global', async () => {
    /* let nodeRoots = [1, 2].map(x => './ipfs-' + x);
    nodeRoots.forEach((n) => {
      try {
        fs.rmSync(n, { recursive: true, force: true });
      } catch (error) {
        // its ok
      }
    });
    const blobbies = await Promise.all(nodeRoots.map(async (root) => {
      const blobby = new OrbitDBPeer();
      await blobby.create({
        local: false,
        repo: root,
        behaviours: undefined,
        replicationCapacity: 0,
        rootAddress: 'root',
        identity: undefined,
        trustProvider: undefined
      });
      return blobby;
    }))
    await delay(5000);
    let a = await blobbies[0].node.bootstrap.list();
    let b = await blobbies[1].node.bootstrap.list();
    const id1 = await blobbies[0].node.id();
    const id2 = await blobbies[1].node.id()
    await blobbies[0].node.swarm.connect(id2.addresses[0]);
    console.log('SENDING MESSAGE TO', (id1));
    await blobbies[1].sendMessage(id1.id, "hello");
    await delay(5000);

    for (const b of blobbies) {
      await b.disconnect();
    } */
    /*
    await blobby.create(false, './ipfs-1');
 
    
 
    blobby.node.bootstrap.reset()
    blobby2.node.bootstrap.reset()
    let abc = blobby2.getAllProfileFields();
 
    let a = await blobby.node.bootstrap.list();
    let b = await blobby2.node.bootstrap.list();
 
    const id = await blobby2.node.id()
    // console.log(id.addresses[0])
    //  await blobby.connectToPeer(id.publicKey);
    let addrs = await blobby.node.swarm.addrs();
    let laddrs = await blobby.node.swarm.localAddrs();
    let peers = await blobby.node.swarm.peers();
    let peers2 = await blobby2.node.swarm.peers();
    const x = 1;
    await delay(5000);
    await blobby.node.swarm.connect(id.addresses[0]);

    console.log('SENDING MESSAGE TO', (await blobby.node.id()).id)
    await blobby2.sendMessage((await blobby.node.id()).id, "hello");
    await delay(5000);
 
   
    await blobby.disconnect();
    await blobby2.disconnect(); */


    // const peers = await blobby.getIpfsPeers();
    /*   let cid = await blobby.addNewPiece("QmNR2n4zywCV61MeMLB6JwPueAPqheqpfiA4fLPMxouEmQ")
      let content = await blobby.node.dag.get(ipfs.CID.parse(cid));
      expect(content).toBeDefined(); */
  });
});

// Test grantee can grante another grantee

// 
const delay = ms => new Promise(res => setTimeout(res, ms));

