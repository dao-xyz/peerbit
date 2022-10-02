import { variant, field, vec, serialize } from '@dao-xyz/borsh';
import { Entry } from '@dao-xyz/ipfs-log-entry'
import { Message } from './message.js';
import { StoreLike } from '@dao-xyz/orbit-db-store';
import Logger from 'logplease'
import { DecryptedThing } from "@dao-xyz/peerbit-crypto";
import { MaybeSigned, PublicKey } from '@dao-xyz/peerbit-crypto';
import { ResourceRequirement } from './exchange-replication.js';
const logger = Logger.create('exchange-heads', { color: Logger.Colors.Yellow })
Logger.setLogLevel('ERROR')

@variant([0, 0])
export class ExchangeHeadsMessage<T> extends Message {

  @field({ type: 'string' })
  replicationTopic: string;

  @field({ type: 'string' })
  address: string;

  @field({ type: vec(Entry) })
  heads: Entry<T>[];

  @field({ type: vec(ResourceRequirement) })
  resourceRequirements: ResourceRequirement[];

  constructor(props?: {
    replicationTopic: string,
    address: string,
    heads: Entry<T>[],
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
export class RequestHeadsMessage extends Message {

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

export const exchangeHeads = async (send: (message: Uint8Array) => Promise<void>, store: StoreLike<any>, sign: (bytes: Uint8Array) => Promise<{ signature: Uint8Array, publicKey: PublicKey }>, headsToShare?: Entry<any>[]) => {
  const heads = headsToShare || await store.getHeads();
  logger.debug(`Send latest heads of '${store.replicationTopic}'`)
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

    const message = new ExchangeHeadsMessage({ replicationTopic: store.replicationTopic, address: store.address.toString(), heads });
    const signedMessage = await new MaybeSigned({ data: serialize(message) }).sign(sign)
    const decryptedMessage = new DecryptedThing({
      data: serialize(signedMessage)
    }) // TODO encryption?
    const serializedMessage = serialize(decryptedMessage);
    await send(serializedMessage)
  }
}

