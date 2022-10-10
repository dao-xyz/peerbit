import { QueryRequestV0, QueryResponseV0, DocumentQueryRequest, FieldStringMatchQuery, StoreAddressMatchQuery, ResultWithSource } from "@dao-xyz/query-protocol"
// @ts-ignore
import { v4 as uuid } from 'uuid';
import type { Message } from '@libp2p/interface-pubsub'
import { field, variant } from "@dao-xyz/borsh";
import { delay, waitFor } from "@dao-xyz/time";
import { Session, waitForPeers } from '@dao-xyz/orbit-db-test-utils';
import { CustomBinaryPayload } from '@dao-xyz/bpayload';
import { decryptVerifyInto, Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { query, respond } from '../io.js';
import { Identity } from "@dao-xyz/ipfs-log";

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
    sign: (data) => ed.sign(data)
  } as Identity
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
      let request = await decryptVerifyInto(msg.data, QueryRequestV0); // deserialize, so we now this works, even though we will not analyse the query
      await respond(session.peers[0].ipfs, topic, request, new QueryResponseV0({
        results: [new ResultWithSource({
          source: new NumberResult({ number: 123 })
        })]
      }))
    })

    await waitForPeers(session.peers[1].ipfs, [session.peers[0].id], topic);
    let results = [];
    await query(session.peers[1].ipfs, topic, new QueryRequestV0({
      type: new DocumentQueryRequest({
        queries: [new FieldStringMatchQuery({
          key: 'a',
          value: 'b'
        }), new StoreAddressMatchQuery({
          address: 'a'
        })
        ]
      })
    }), (resp) => {
      expect(resp.results[0]).toBeInstanceOf(ResultWithSource);
      expect((resp.results[0] as ResultWithSource).source).toBeInstanceOf(NumberResult);
      results.push((((resp.results[0] as ResultWithSource).source) as NumberResult).number);
    }, { waitForAmount: 1 })

    await waitFor(() => results.length === 1);

  })


  it('timeout', async () => {
    let maxAggregationTime = 2000;

    const topic = uuid();
    await session.peers[0].ipfs.pubsub.subscribe(topic, async (msg: Message) => {
      let request = await decryptVerifyInto(msg.data, QueryRequestV0);
      await respond(session.peers[0].ipfs, topic, request, new QueryResponseV0({
        results: [new ResultWithSource({
          source: new NumberResult({ number: 123 })
        })]
      }));

      setTimeout(() => {
        respond(session.peers[0].ipfs, topic, request, new QueryResponseV0({
          results: [new ResultWithSource({
            source: new NumberResult({ number: 234 })
          })]
        }));
      }, maxAggregationTime + 500) // more than aggregation time
    })
    await waitForPeers(session.peers[1].ipfs, [session.peers[0].id], topic);

    let results: number[] = [];
    await query(session.peers[1].ipfs, topic, new QueryRequestV0({
      type: new DocumentQueryRequest({
        queries: []
      })
    }), (resp) => {
      expect(resp.results[0]).toBeInstanceOf(ResultWithSource);
      expect((resp.results[0] as ResultWithSource).source).toBeInstanceOf(NumberResult);
      results.push((((resp.results[0] as ResultWithSource).source) as NumberResult).number);
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
        let request = await decryptVerifyInto(msg.data, QueryRequestV0);
        await respond(session.peers[i].ipfs, topic, request, new QueryResponseV0({
          results: [new ResultWithSource({
            source: new NumberResult({ number: 123 })
          })]
        }));
      })
    }

    await waitForPeers(session.peers[0].ipfs, [session.peers[1].id, session.peers[2].id], topic);

    let results: number[] = [];
    await query(session.peers[0].ipfs, topic, new QueryRequestV0({
      type: new DocumentQueryRequest({
        queries: []
      })
    }), (resp) => {
      expect(resp.results[0]).toBeInstanceOf(ResultWithSource);
      expect((resp.results[0] as ResultWithSource).source).toBeInstanceOf(NumberResult);
      results.push((((resp.results[0] as ResultWithSource).source) as NumberResult).number);
    }, {
      maxAggregationTime,
      waitForAmount
    })

    await waitFor(() => results.length == waitForAmount);

  })


  it('signed', async () => {
    let waitForAmount = 1;
    let maxAggregationTime = 3000;

    const responder = await createIdentity();
    const topic = uuid();
    await session.peers[1].ipfs.pubsub.subscribe(topic, async (msg: Message) => {
      let request = await decryptVerifyInto(msg.data, QueryRequestV0);
      await respond(session.peers[1].ipfs, topic, request, new QueryResponseV0({
        results: [new ResultWithSource({
          source: new NumberResult({ number: 123 })
        })]
      }), { signer: responder });
    })

    await waitForPeers(session.peers[0].ipfs, [session.peers[1].id], topic);

    let results: number[] = [];
    await query(session.peers[0].ipfs, topic, new QueryRequestV0({
      type: new DocumentQueryRequest({
        queries: []
      }),
    }), (resp) => {
      expect(resp.results[0]).toBeInstanceOf(ResultWithSource);
      expect((resp.results[0] as ResultWithSource).source).toBeInstanceOf(NumberResult);
      results.push((((resp.results[0] as ResultWithSource).source) as NumberResult).number);
    }, {
      maxAggregationTime,
      waitForAmount,
      identiy: await createIdentity()
    })

    await waitFor(() => results.length == waitForAmount);

  })


  /* it('encrypted', async () => {
    let waitForAmount = 1;
    let maxAggregationTime = 3000;

    const responder = await createIdentity();
    const requester = await createIdentity();
    const topic = uuid();
    await session.peers[1].ipfs.pubsub.subscribe(topic, async (msg: Message) => {
      let request = await decryptVerifyInto(msg.data, QueryRequestV0);
      await respond(session.peers[1].ipfs, topic, request, new QueryResponseV0({
        results: [new ResultWithSource({
          source: new NumberResult({ number: 123 })
        })]
      }));
    })
    console.log('c');
    await waitForPeers(session.peers[0].ipfs, [session.peers[1].id], topic);
    console.log('d');

    let results: number[] = [];
    await query(session.peers[0].ipfs, topic, new QueryRequestV0({
      type: new DocumentQueryRequest({
        queries: []
      }),
    }), (resp) => {
      expect(resp.results[0]).toBeInstanceOf(ResultWithSource);
      expect((resp.results[0] as ResultWithSource).source).toBeInstanceOf(NumberResult);
      results.push((((resp.results[0] as ResultWithSource).source) as NumberResult).number);
    }, {
      maxAggregationTime,
      waitForAmount,
      identiy: requester,
      encryption: ()
    })

    await waitFor(() => results.length == waitForAmount);

  }) */

}) 