import * as IPFS from 'ipfs';
import { query, QueryRequestV0, QueryResponseV0 } from "../query"
import { v4 as uuid } from 'uuid';
import { Message } from "ipfs-core-types/src/pubsub";
import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { DocumentQueryRequest, FieldStringMatchQuery } from "../document-query";
import { ResultWithSource } from "../result";
import { delay, waitFor } from "@dao-xyz/time";
import { disconnectPeers, getConnectedPeers } from '@dao-xyz/peer-test-utils';
import { StoreAddressMatchQuery } from '../context';
import { BinaryPayload } from '@dao-xyz/bpayload';

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

  test('any', async () => {

    const [a, b] = await getConnectedPeers(2);

    const topic = uuid();
    await a.node.pubsub.subscribe(topic, (msg: Message) => {
      let request = deserialize(Buffer.from(msg.data), QueryRequestV0); // deserialize, so we now this works, even though we will not analyse the query
      a.node.pubsub.publish(request.getResponseTopic(topic), serialize(new QueryResponseV0({
        results: [new ResultWithSource({
          source: new NumberResult({ number: 123 })
        })]
      })));
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
    }, 1)

    await waitFor(() => results.length == 1);
    await disconnectPeers([a, b]);

  })


  test('timeout', async () => {
    const [a, b] = await getConnectedPeers(2);
    let maxAggrergationTime = 2000;

    const topic = uuid();
    await a.node.pubsub.subscribe(topic, (msg: Message) => {
      let request = deserialize(Buffer.from(msg.data), QueryRequestV0);
      a.node.pubsub.publish(request.getResponseTopic(topic), serialize(new QueryResponseV0({
        results: [new ResultWithSource({
          source: new NumberResult({ number: 123 })
        })]
      })));

      setTimeout(() => {
        a.node.pubsub.publish(request.getResponseTopic(topic), serialize(new QueryResponseV0({
          results: [new ResultWithSource({
            source: new NumberResult({ number: 234 })
          })]
        })));
      }, maxAggrergationTime + 500) // more than aggregation time
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
    }, undefined, maxAggrergationTime)

    await waitFor(() => results.length == 1);
    await delay(1000); // wait some time to check whether new messages appear even if abort option is set as timeout
    await waitFor(async () => (await b.node.pubsub.ls()).length == 0)
    expect(results).toHaveLength(1);
    await disconnectPeers([a, b]);
  })

}) 