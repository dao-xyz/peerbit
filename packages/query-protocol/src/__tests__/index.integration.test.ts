import { query, QueryRequestV0, QueryResponseV0, respond } from "../query"
import { v4 as uuid } from 'uuid';
import { Message } from "ipfs-core-types/src/pubsub";
import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { DocumentQueryRequest, FieldStringMatchQuery } from "../document-query";
import { ResultWithSource } from "../result";
import { delay, waitFor } from "@dao-xyz/time";
import { disconnectPeers, getConnectedPeers } from '@dao-xyz/peer-test-utils';
import { StoreAddressMatchQuery } from '../context';
import { BinaryPayload } from '@dao-xyz/bpayload';
import { decryptVerifyInto } from "@dao-xyz/encryption-utils";

@variant("number")//@variant([1, 1])
class NumberResult extends BinaryPayload {
  @field({ type: 'u32' })
  number: number
  constructor(opts?: { number: number }) {
    super();
    if (opts) {
      this.number = opts.number;
    }
  }
}

describe('query', () => {

  it('any', async () => {

    const [a, b] = await getConnectedPeers(2);

    const topic = uuid();
    await a.node.pubsub.subscribe(topic, async (msg: Message) => {
      let request = await decryptVerifyInto(msg.data, QueryRequestV0); // deserialize, so we now this works, even though we will not analyse the query
      await respond(a.node.pubsub, topic, request, new QueryResponseV0({
        results: [new ResultWithSource({
          source: new NumberResult({ number: 123 })
        })]
      }))
    })

    await delay(1000); // arb delay as the subscription has to "start"
    let results = [];
    await query(b.node.pubsub, topic, new QueryRequestV0({
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

    await waitFor(() => results.length == 1);
    await disconnectPeers([a, b]);

  })


  it('timeout', async () => {
    const [a, b] = await getConnectedPeers(2);
    let maxAggregationTime = 2000;

    const topic = uuid();
    await a.node.pubsub.subscribe(topic, async (msg: Message) => {
      let request = await decryptVerifyInto(msg.data, QueryRequestV0);
      await respond(a.node.pubsub, topic, request, new QueryResponseV0({
        results: [new ResultWithSource({
          source: new NumberResult({ number: 123 })
        })]
      }));

      setTimeout(() => {
        respond(a.node.pubsub, topic, request, new QueryResponseV0({
          results: [new ResultWithSource({
            source: new NumberResult({ number: 234 })
          })]
        }));
      }, maxAggregationTime + 500) // more than aggregation time
    })
    await delay(1000); // arb delay as the subscription has to "start"

    let results = [];
    await query(b.node.pubsub, topic, new QueryRequestV0({
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
    await waitFor(async () => (await b.node.pubsub.ls()).length == 0)
    expect(results).toHaveLength(1);
    await disconnectPeers([a, b]);
  })

}) 