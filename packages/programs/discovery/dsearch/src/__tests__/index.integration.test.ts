import { DocumentQueryRequest, FieldStringMatchQuery, StoreAddressMatchQuery, ResultWithSource } from "../"
// @ts-ignore
import { v4 as uuid } from 'uuid';
import type { Message } from '@libp2p/interface-pubsub'
import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { delay, waitFor } from "@dao-xyz/peerbit-time";
import { Session, waitForPeers } from '@dao-xyz/peerbit-test-utils';
import { CustomBinaryPayload } from '@dao-xyz/peerbit-bpayload';
import { decryptVerifyInto, Ed25519Keypair, Ed25519PublicKey, X25519Keypair, X25519PublicKey, X25519SecretKey } from "@dao-xyz/peerbit-crypto";
import { QueryRequestV0, QueryResponseV0, query, respond } from '@dao-xyz/peerbit-dquery';
import { Ed25519Identity } from "@dao-xyz/ipfs-log";
import { Results } from "../result";

@variant("number")//@variant([1, 1])
class NumberResult extends CustomBinaryPayload {
  @field({ type: 'u32' })
  number: number
  constructor(opts?: { number: number }) {
    super();
    if (opts) {
      this.number = opts.number;
    }
  }
}


const createIdentity = async () => {
  const ed = await Ed25519Keypair.create();
  return {
    publicKey: ed.publicKey,
    privateKey: ed.privateKey,
    sign: (data) => ed.sign(data)
  } as Ed25519Identity
}

describe('query', () => {
  let session: Session;
  beforeAll(async () => {
    session = await Session.connected(3);
  })
  afterAll(async () => {
    await session.stop();
  })

  it('any', async () => {
    const topic = uuid();
    await session.peers[0].ipfs.pubsub.subscribe(topic, async (msg: Message) => {
      let { result: request } = await decryptVerifyInto(msg.data, QueryRequestV0, () => Promise.resolve(undefined)); // deserialize, so we now this works, even though we will not analyse the query
      await respond(session.peers[0].ipfs, topic, request, new QueryResponseV0({
        response: serialize(new Results({
          results: [new ResultWithSource({
            source: new NumberResult({ number: 123 })
          })]
        }))
      }))
    })

    await waitForPeers(session.peers[1].ipfs, [session.peers[0].id], topic);
    let results: number[] = [];
    await query(session.peers[1].ipfs, topic, new QueryRequestV0({
      query: serialize(new DocumentQueryRequest({
        queries: [new FieldStringMatchQuery({
          key: 'a',
          value: 'b'
        }), new StoreAddressMatchQuery({
          address: 'a'
        })
        ]
      }))
    }), (resp) => {
      const r = deserialize(resp.response, Results);
      expect(r.results[0]).toBeInstanceOf(ResultWithSource);
      expect((r.results[0] as ResultWithSource).source).toBeInstanceOf(NumberResult);
      results.push((((r.results[0] as ResultWithSource).source) as NumberResult).number);
    }, { waitForAmount: 1 })

    await waitFor(() => results.length === 1);

  })


  it('timeout', async () => {
    let maxAggregationTime = 2000;

    const topic = uuid();
    await session.peers[0].ipfs.pubsub.subscribe(topic, async (msg: Message) => {
      let { result: request } = await decryptVerifyInto(msg.data, QueryRequestV0, () => Promise.resolve(undefined));
      await respond(session.peers[0].ipfs, topic, request, new QueryResponseV0({
        response: serialize(new Results({
          results: [new ResultWithSource({
            source: new NumberResult({ number: 123 })
          })]
        }))
      }));

      setTimeout(() => {
        respond(session.peers[0].ipfs, topic, request, new QueryResponseV0({
          response: serialize(new Results({
            results: [new ResultWithSource({
              source: new NumberResult({ number: 234 })
            })]
          }))
        }));
      }, maxAggregationTime + 500) // more than aggregation time
    })
    await waitForPeers(session.peers[1].ipfs, [session.peers[0].id], topic);

    let results: number[] = [];
    await query(session.peers[1].ipfs, topic, new QueryRequestV0({
      query: serialize(new DocumentQueryRequest({
        queries: []
      }))
    }), (resp) => {
      const r = deserialize(resp.response, Results);

      expect(r.results[0]).toBeInstanceOf(ResultWithSource);
      expect((r.results[0] as ResultWithSource).source).toBeInstanceOf(NumberResult);
      results.push((((r.results[0] as ResultWithSource).source) as NumberResult).number);
    }, {
      maxAggregationTime
    })

    await waitFor(() => results.length == 1);
    await delay(1000); // wait some time to check whether new messages appear even if abort option is set as timeout
    await waitFor(async () => (await session.peers[1].ipfs.pubsub.ls()).length == 0)
    expect(results).toHaveLength(1);
  })

  it('waitForAmount', async () => {
    let waitForAmount = 2;
    let maxAggregationTime = 2000;

    const topic = uuid();
    for (let i = 1; i < 3; i++) {
      await session.peers[i].ipfs.pubsub.subscribe(topic, async (msg: Message) => {
        let { result: request } = await decryptVerifyInto(msg.data, QueryRequestV0, () => Promise.resolve(undefined));
        await respond(session.peers[i].ipfs, topic, request, new QueryResponseV0({
          response: serialize(new Results({
            results: [new ResultWithSource({
              source: new NumberResult({ number: 123 })
            })]
          }))
        }));
      })
    }

    await waitForPeers(session.peers[0].ipfs, [session.peers[1].id, session.peers[2].id], topic);

    let results: number[] = [];
    await query(session.peers[0].ipfs, topic, new QueryRequestV0({
      query: serialize(new DocumentQueryRequest({
        queries: []
      }))
    }), (resp) => {
      const r = deserialize(resp.response, Results);

      expect(r.results[0]).toBeInstanceOf(ResultWithSource);
      expect((r.results[0] as ResultWithSource).source).toBeInstanceOf(NumberResult);
      results.push((((r.results[0] as ResultWithSource).source) as NumberResult).number);
    }, {
      maxAggregationTime,
      waitForAmount
    })

    await waitFor(() => results.length == waitForAmount);

  })


  it('signed', async () => {
    let waitForAmount = 1;

    let maxAggregationTime = 3000;

    const sender = await createIdentity();
    const responder = await createIdentity();
    const topic = uuid();
    await session.peers[1].ipfs.pubsub.subscribe(topic, async (msg: Message) => {
      let { result: request, from } = await decryptVerifyInto(msg.data, QueryRequestV0, () => Promise.resolve(undefined));

      // Check that it was signed by the sender
      expect(from).toBeInstanceOf(Ed25519PublicKey);
      expect((from as Ed25519PublicKey).equals(sender.publicKey)).toBeTrue();


      await respond(session.peers[1].ipfs, topic, request, new QueryResponseV0({
        response: serialize(new Results({
          results: [new ResultWithSource({
            source: new NumberResult({ number: 123 })
          })]
        }))
      }), { signer: responder });
    })

    await waitForPeers(session.peers[0].ipfs, [session.peers[1].id], topic);

    let results: number[] = [];
    await query(session.peers[0].ipfs, topic, new QueryRequestV0({
      query: serialize(new DocumentQueryRequest({
        queries: []
      })),
    }), (resp, from) => {

      const r = deserialize(resp.response, Results);
      expect(r.results[0]).toBeInstanceOf(ResultWithSource);
      expect((r.results[0] as ResultWithSource).source).toBeInstanceOf(NumberResult);

      // Check that it was signed by the responder
      expect(from).toBeInstanceOf(Ed25519PublicKey);
      expect((from as Ed25519PublicKey).equals(responder.publicKey)).toBeTrue();

      results.push((((r.results[0] as ResultWithSource).source) as NumberResult).number);


    }, {
      maxAggregationTime,
      waitForAmount,
      signer: sender
    })

    await waitFor(() => results.length == waitForAmount);

  })


  it('encrypted', async () => {

    // query encrypted and respond encrypted
    let waitForAmount = 1;
    let maxAggregationTime = 3000;

    const responder = await createIdentity();
    const requester = await createIdentity();
    const topic = uuid();
    await session.peers[1].ipfs.pubsub.subscribe(topic, async (msg: Message) => {
      let { result: request } = await decryptVerifyInto(msg.data, QueryRequestV0, async (keys) => { return { index: 0, keypair: await X25519Keypair.from(new Ed25519Keypair({ ...responder })) } });
      await respond(session.peers[1].ipfs, topic, request, new QueryResponseV0({
        response: serialize(new Results({
          results: [new ResultWithSource({
            source: new NumberResult({ number: 123 })
          })]
        }))
      }));
    })
    await waitForPeers(session.peers[0].ipfs, [session.peers[1].id], topic);

    let results: number[] = [];
    await query(session.peers[0].ipfs, topic, new QueryRequestV0({
      query: serialize(new DocumentQueryRequest({
        queries: []
      })),
      responseRecievers: [await X25519PublicKey.from(requester.publicKey)]
    }), (resp) => {
      const r = deserialize(resp.response, Results);
      expect(r.results[0]).toBeInstanceOf(ResultWithSource);
      expect((r.results[0] as ResultWithSource).source).toBeInstanceOf(NumberResult);
      results.push((((r.results[0] as ResultWithSource).source) as NumberResult).number);
    }, {
      maxAggregationTime,
      waitForAmount,
      signer: requester,
      keyResolver: async () => {
        return {
          index: 0,
          keypair: await X25519Keypair.from(new Ed25519Keypair({ ...requester }))
        }
      },
      encryption: {
        key: () => new Ed25519Keypair({ ...requester }),
        responders: [await X25519PublicKey.from(responder.publicKey)]
      }
    })

    await waitFor(() => results.length == waitForAmount);

  })

}) 