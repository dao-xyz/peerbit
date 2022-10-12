import { variant, field, vec, serialize } from '@dao-xyz/borsh';
import { Entry, Identity } from '@dao-xyz/ipfs-log'
import { ProtocolMessage } from './message.js';
import { StoreLike } from '@dao-xyz/orbit-db-store';
import { DecryptedThing, PublicSignKey } from "@dao-xyz/peerbit-crypto";
import { MaybeSigned } from '@dao-xyz/peerbit-crypto';
import { ResourceRequirement } from './exchange-replication.js';
// @ts-ignore
import Logger from 'logplease'
import { EntryWithRefs } from '@dao-xyz/orbit-db-store';
const logger = Logger.create('exchange-heads', { color: Logger.Colors.Yellow })
Logger.setLogLevel('ERROR')




@variant([0, 0])
export class ExchangeHeadsMessage<T> extends ProtocolMessage {

  @field({ type: 'string' })
  replicationTopic: string;

  @field({ type: 'string' })
  address: string;

  @field({ type: vec(EntryWithRefs) })
  heads: EntryWithRefs<T>[];

  @field({ type: vec(ResourceRequirement) })
  resourceRequirements: ResourceRequirement[];

  constructor(props?: {
    replicationTopic: string,
    address: string,
    heads: EntryWithRefs<T>[],
    resourceRequirements?: ResourceRequirement[]
  }) {
    super();
    if (props) {
      this.resourceRequirements = props.resourceRequirements || [];
      this.replicationTopic = props.replicationTopic;
      this.address = props.address;
      this.heads = props.heads;
    }
  }
}

@variant([0, 1])
export class RequestHeadsMessage extends ProtocolMessage {

  @field({ type: 'string' })
  replicationTopic: string;

  @field({ type: 'string' })
  address: string;

  constructor(props?: {
    replicationTopic: string,
    address: string
  }) {
    super();
    if (props) {
      this.replicationTopic = props.replicationTopic;
      this.address = props.address;
    }
  }
}

/* export const exchangeHeads = async (send: (peer, message: Uint8Array) => Promise<void>, store: StoreLike<any>, isSupported: (hash: string) => boolean, sign: (bytes: Uint8Array) => Promise<{ signature: Uint8Array, publicKey: PublicKey }>) => {
  const heads = await store.getHeads();
  logger.debug(`Send latest heads of '${store.replicationTopic}'`)
  if (heads && heads.length > 0) {

    const headsToSend = heads.filter(head => !isSupported(head.hash));

    // Calculate leaders and send directly ? Batch by channel instead
    const message = new ExchangeHeadsMessage({ replicationTopic: store.replicationTopic, address: store.address.toString(), heads: headsToSend });
    const signedMessage = await new MaybeSigned({ data: serialize(message) }).sign(sign)
    const decryptedMessage = new DecryptedThing({
      data: serialize(signedMessage)
    }) // TODO encryption?
    const serializedMessage = serialize(decryptedMessage);
    await send(serializedMessage)
  }
}
 */

export const exchangeHeads = async (send: (message: Uint8Array) => Promise<void>, store: StoreLike<any>, identity: Identity, heads: Entry<any>[], replicationTopic: string) => {
  const gids = new Set(heads.map(h => h.gid));
  if (gids.size > 1) {
    throw new Error("Expected to share heads only from 1 gid")
  }

  const headsSet = new Set(heads);
  const headsWithRefs = heads.map(head => {
    const refs = store.oplog.getPow2Refs(store.oplog.length, [head]).filter(r => !headsSet.has(r)); // pick a proportional amount of refs so we can efficiently load the log. TODO should be equidistant for good performance? 
    return new EntryWithRefs({
      entry: head,
      references: refs
    })
  });
  logger.debug(`Send latest heads of '${store.address.toString()}'`)
  if (heads && heads.length > 0) {

    /*  const mapFromPeerToHead: Map<string, Entry<any>[]> = new Map();
     heads.forEach((head) => {
       const ls = leaders(head.gid);
       store.oplog.setPeersByGid(head.gid, new Set(ls))
       store.oplog.getPeersByGid(head.gid).forEach((peer) => {
         let arr = mapFromPeerToHead.get(peer);
         if (!arr) {
           arr = [];
           mapFromPeerToHead.set(peer, arr)
         };
         arr.push(head)
       }) */
    /*  if (head.next.length === 0) {
       if (!head.peers) {
         head.peers = new Set(leaders(head.gid));
       }
     }
     if (head.peers.size === 0) {
       throw new Error("Unexpected");
     }
 
     head.peers.forEach((peer) => {
       if (peer === id) {
         return;
       }
       let hs: Entry<any>[] = mapFromPeerToHead.get(peer);
       if (!hs) {
         hs = [];
         mapFromPeerToHead.set(peer, hs);
       }
       hs.push(head);
     }) */

    /*  });
  */
    /* const promises = [];
    for (const [peer, headsToPeer] of mapFromPeerToHead) {
     
      promises.push()
    }
    await Promise.all(promises); */

    const message = new ExchangeHeadsMessage({ replicationTopic, address: store.address.toString(), heads: headsWithRefs });
    const signer = async (data: Uint8Array) => {
      return {
        signature: await identity.sign(data),
        publicKey: identity.publicKey
      }
    };
    const signedMessage = await new MaybeSigned({ data: serialize(message) }).sign(signer)
    const decryptedMessage = new DecryptedThing({
      data: serialize(signedMessage)
    }) // TODO encryption?
    const serializedMessage = serialize(decryptedMessage);
    await send(serializedMessage)
  }
}

