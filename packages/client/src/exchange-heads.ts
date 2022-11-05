import { variant, option, field, vec, serialize } from '@dao-xyz/borsh';
import { Entry, Identity } from '@dao-xyz/ipfs-log'
import { ProtocolMessage } from './message.js';
import { DecryptedThing } from "@dao-xyz/peerbit-crypto";
import { MaybeSigned } from '@dao-xyz/peerbit-crypto';

import { Program } from '@dao-xyz/peerbit-program';
import { fixedUint8Array } from '@dao-xyz/peerbit-borsh-utils';
import { logger as parentLogger } from './logger.js'
import { Store } from '@dao-xyz/peerbit-store';
const logger = parentLogger.child({ module: 'exchange-heads' });



export class MinReplicas {
  get value(): number {
    throw new Error("Not implemented")
  }
}

@variant(0)
export class AbsolutMinReplicas extends MinReplicas {

  _value: number;
  constructor(value: number) {
    super()
    this._value = value
  }
  get value() {
    return this._value;
  }
}




/**
 * This thing allows use to faster sync since we can provide 
 * references that can be read concurrently to 
 * the entry when doing Log.fromEntry or Log.fromEntryHash
 */
@variant(0)
export class EntryWithRefs<T> {

  @field({ type: Entry })
  entry: Entry<T>

  @field({ type: vec(Entry) })
  references: Entry<T>[] // are some parents to the entry

  constructor(properties?: { entry: Entry<T>, references: Entry<T>[] }) {
    if (properties) {
      this.entry = properties.entry;
      this.references = properties.references;
    }
  }
}

@variant([0, 0])
export class ExchangeHeadsMessage<T> extends ProtocolMessage {

  @field({ type: 'string' })
  replicationTopic: string;

  @field({ type: 'string' })
  programAddress: string;

  @field({ type: 'u32' })
  storeIndex: number;

  @field({ type: option('u32') })
  programIndex?: number;


  @field({ type: vec(EntryWithRefs) })
  heads: EntryWithRefs<T>[];


  @field({ type: option(MinReplicas) })
  minReplicas?: MinReplicas

  @field({ type: fixedUint8Array(4) })
  reserved: Uint8Array = new Uint8Array(4);


  constructor(props?: {
    replicationTopic: string,
    programIndex?: number,
    programAddress: string,
    storeIndex: number;

    heads: EntryWithRefs<T>[],
    minReplicas?: MinReplicas,
  }) {
    super();
    if (props) {
      /* this.resourceRequirements = props.resourceRequirements || []; */
      this.replicationTopic = props.replicationTopic;
      this.storeIndex = props.storeIndex;
      this.programIndex = props.programIndex;
      this.programAddress = props.programAddress;
      this.heads = props.heads;
      this.minReplicas = props.minReplicas;
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


export const exchangeHeads = async (send: (message: Uint8Array) => Promise<void>, store: Store<any>, program: Program, heads: Entry<any>[], replicationTopic: string, includeReferences: boolean, identity?: Identity) => {
  const gids = new Set(heads.map(h => h.gid));
  if (gids.size > 1) {
    throw new Error("Expected to share heads only from 1 gid")
  }

  const headsSet = new Set(heads);
  const headsWithRefs = heads.map(head => {
    const refs = !includeReferences ? [] : store.oplog.getPow2Refs(store.oplog.length, [head]).filter(r => !headsSet.has(r)); // pick a proportional amount of refs so we can efficiently load the log. TODO should be equidistant for good performance? 
    return new EntryWithRefs({
      entry: head,
      references: refs
    })
  });
  logger.debug(`Send latest heads of '${store._storeIndex}'`)
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

    const message = new ExchangeHeadsMessage({ replicationTopic, storeIndex: store._storeIndex, programIndex: program._programIndex, programAddress: ((program.address || program.parentProgram.address)!).toString(), heads: headsWithRefs });
    const maybeSigned = new MaybeSigned({ data: serialize(message) });
    let signedMessage: MaybeSigned<any> = maybeSigned;
    if (identity) {
      const signer = async (data: Uint8Array) => {
        return {
          signature: await identity.sign(data),
          publicKey: identity.publicKey
        }
      };
      signedMessage = await signedMessage.sign(signer)
    }

    const decryptedMessage = new DecryptedThing({
      data: serialize(signedMessage)
    }) // TODO encryption?
    const serializedMessage = serialize(decryptedMessage);
    await send(serializedMessage)
  }
}

