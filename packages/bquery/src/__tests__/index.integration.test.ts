import * as IPFS from 'ipfs';
import { query, QueryRequestV0, QueryResponseV0 } from "../query"
import { v4 as uuid } from 'uuid';
import { IPFS as IPFSInstance } from 'ipfs-core-types';
import { Message } from "ipfs-core-types/src/pubsub";
import { deserialize, field, serialize, serializeField, variant } from "@dao-xyz/borsh";
import { DocumentQueryRequest } from "../document-query";
import { Result, ResultSource, ResultWithSource } from "../result";
export const createIPFSNode = (repo: string = './ipfs'): Promise<IPFSInstance> => {
  // Create IPFS instance
  const ipfsOptions = {
    relay: { enabled: true, hop: { enabled: true, active: true } },
    repo: repo,
    EXPERIMENTAL: { pubsub: true },
    config: {
      Addresses: {
        Swarm: [
          `/ip4/0.0.0.0/tcp/0`,
          `/ip4/127.0.0.1/tcp/0/ws`
        ]
      }

    },
  }
  return IPFS.create(ipfsOptions)

}
export const disconnectPeers = async (peers: IPFSInstance[]): Promise<void> => {
  await Promise.all(peers.map(peer => peer.stop()));

}


const delay = (ms: number) => new Promise(res => setTimeout(res, ms));

const waitFor = async (fn: () => boolean | Promise<boolean>, timeout: number = 60 * 1000) => {

  let startTime = +new Date;
  while (+new Date - startTime < timeout) {
    if (await fn()) {
      return;
    }
    await delay(50);
  }
  throw new Error("Timed out")

};


@variant(4)
class NumberResult extends ResultSource {
  @field({ type: 'u32' })
  number: number
  constructor(opts?: { number: number }) {
    super();
    if (opts) {
      this.number = opts.number;
    }
  }
}

const createIPFSNodePair = async (): Promise<{ a: IPFSInstance, b: IPFSInstance }> => {
  let a = await createIPFSNode('./ipfs/' + uuid() + '/');
  let b = await createIPFSNode('./ipfs/' + uuid() + '/');
  const addresses = (await a.id()).addresses;
  for (const addr of addresses) {
    await b.swarm.connect(addr);
  }
  return {
    a, b
  }
}

describe('query', () => {

  test('any', async () => {

    const {
      a, b
    } = await createIPFSNodePair();

    const topic = uuid();
    await a.pubsub.subscribe(topic, (msg: Message) => {
      let request = deserialize(Buffer.from(msg.data), QueryRequestV0);
      a.pubsub.publish(request.getResponseTopic(topic), serialize(new QueryResponseV0({
        results: [new ResultWithSource({
          source: new NumberResult({ number: 123 })
        })]
      })));
    })
    await delay(1000); // arb delay as the subscription has to "start"

    let results = [];
    await query(b.pubsub, topic, new QueryRequestV0({
      type: new DocumentQueryRequest({
        queries: []
      })
    }), (resp) => {
      expect(resp.results[0]).toBeInstanceOf(ResultWithSource);
      expect((resp.results[0] as ResultWithSource).source).toBeInstanceOf(NumberResult);
      results.push((((resp.results[0] as ResultWithSource).source) as NumberResult).number);
    })

    await waitFor(() => results.length == 1);
    await disconnectPeers([a, b]);

  })


  test('timeout', async () => {
    const {
      a, b
    } = await createIPFSNodePair();
    let maxAggrergationTime = 2000;

    const topic = uuid();
    await a.pubsub.subscribe(topic, (msg: Message) => {
      let request = deserialize(Buffer.from(msg.data), QueryRequestV0);
      a.pubsub.publish(request.getResponseTopic(topic), serialize(new QueryResponseV0({
        results: [new ResultWithSource({
          source: new NumberResult({ number: 123 })
        })]
      })));

      setTimeout(() => {
        a.pubsub.publish(request.getResponseTopic(topic), serialize(new QueryResponseV0({
          results: [new ResultWithSource({
            source: new NumberResult({ number: 234 })
          })]
        })));
      }, maxAggrergationTime + 500) // more than aggregation time
    })
    await delay(1000); // arb delay as the subscription has to "start"

    let results = [];
    await query(b.pubsub, topic, new QueryRequestV0({
      type: new DocumentQueryRequest({
        queries: []
      })
    }), (resp) => {
      expect(resp.results[0]).toBeInstanceOf(ResultWithSource);
      expect((resp.results[0] as ResultWithSource).source).toBeInstanceOf(NumberResult);
      results.push((((resp.results[0] as ResultWithSource).source) as NumberResult).number);
    }, maxAggrergationTime)

    await waitFor(() => results.length == 1);
    await delay(1000); // wait some time to check whether new messages appear even if abort option is set as timeout
    await waitFor(async () => (await b.pubsub.ls()).length == 0)
    expect(results).toHaveLength(1);
    await disconnectPeers([a, b]);
  })

}) 