# Mapping the Future of Replication: Custom Data Domains in Peerbit

Imagine a system where data replication isn’t fixed but is dynamically mapped to a continuous space. With Peerbit’s custom data domains, you can define replication ranges that flexibly reflect properties of your data—whether that’s a hash, a timestamp, or even the buffering timeline of a live video stream.


## A Generalized Address Space
Traditional replication methods rely on fixed partitions or discrete addressing. In contrast, **range-based replication** projects content onto a 1-dimensional line bounded by **[0, 1]**. This continuous space can represent any property you choose:

- **Hash Mapping:**  
  Convert a document’s hash into a number within [0, 1], effectively turning your system into a DHT-like network.

- **Time Mapping:**  
  Map timestamps so that 1 represents the present and 0 the earliest time. This is ideal for chronological data such as logs or video segments.

- **Identity Mapping:**  
  Use data such as author identity to group documents, enabling replication strategies that prioritize content from specific creators.

For example, subscribing to a live feed might involve replicating only the latest segment (e.g., [0.999, 1]). As the stream evolves, the replication range shifts dynamically. Or when you are buffering a video, then you will replicate the segments that are being buffered and continously expand the replication range as the video progresses. See the video below 

<video src="/topics/custom-domain/buffer.mp4" controls muted style="width: 100%" ></video>

The timeline will turn more blue and become taller when more replicators/viewers are watching the video.

Watch a clip for yourself here 

https://stream.dao.xyz/#/s/zb2rhZdJCHqzReHfLFokXaUTL8aqw7e3H5fHrfimifyCwokEz

[Source code for the video on demand](https://github.com/dao-xyz/peerbit-examples/tree/master/packages/live-streaming)

## Domain Mapping Strategies: Hash vs Time vs Identity
When designing a custom data domain, the choice of mapping strategy is crucial. Here’s a breakdown of the benefits and trade-offs of three common approaches:

### Hash-Based Domains
- **Benefits:**
  - **Determinism:**  
    A hash consistently maps the same input to the same output, ensuring predictable data placement.
  - **Uniform Distribution:**  
    Cryptographic hashes distribute data evenly across the [0, 1] space.
- **Cons:**
  - **Lack of Context:**  
    Hashes do not convey temporal or author-related context, making it harder to group data created closely in time or by the same author.
  - **Replication Granularity:**  
    Data can be scattered, which may complicate localized replication.

### Time-Based Domains

- **Benefits:**
  - **Temporal Grouping:**  
    Data is naturally ordered by time, making it ideal for logs, streams, or any time-sensitive content.
  - **Real-Time Efficiency:**  
    Applications like live feeds can focus replication on the most current data.
- **Cons:**
  - **Constant Evolution:**  
    As time advances, the mapping shifts continuously, which may require frequent adjustments.
  - **Clustering Risks:**  
    High-activity periods may result in many items falling into a small segment of time, leading to imbalanced replication.

### Identity-Based Domains
- **Benefits:**
  - **Author-Centric Grouping:**  
    Data can be grouped by creator, allowing targeted replication strategies.
  - **Enhanced Filtering:**  
    Enables replication or moderation based on the source of the content.
- **Cons:**
  - **Scalability Issues:**  
    A prolific author might dominate a segment of the space, potentially overloading that segment.
  - **Temporal Ambiguity:**  
    Identity mapping does not inherently order data chronologically.

---

## Practical Example: Creating a Time-Based Domain

Below is an example of how you might create a custom replication domain for a time property in a document. In this example, the domain maps a `timestamp` property into the [0, 1] range, allowing you to replicate data based on when it was created.

```typescript
import { Program } from "@peerbit/program";
import { Documents, createDocumentDomain } from "@peerbit/document-store";
import { field } from "@dao-xyz/borsh";
import { v4 as uuid } from "uuid";

class Document {
  @field({ type: "string" })
  id: string;

  @field({ type: "u32" })
  timestamp: number; // Represents the time property

  constructor({ id, timestamp }: { id?: string; timestamp: number }) {
    this.id = id || uuid();
    this.timestamp = timestamp;
  }
}

@variant("time-based-store")
class TimeBasedStore extends Program {
  @field({ type: Documents })
  docs: Documents<Document, Document>;

  constructor(docs?: Documents<Document, Document>) {
    super();
    this.docs = docs || new Documents<Document, Document>();
  }

  async open(options?: any): Promise<void> {
    await this.docs.open({
      // The domain is created based on the 'timestamp' property.
      domain: createDocumentDomain({
        // resolution, u32 or u64
        resolution: "u32",

        // whether a search request should project all results to one replication range
        canProjectToOneSegment: (request) => true,

        // from document to coordinate space [0, max U32]
        fromValue(value) {
          return value?.time ?? 0
        },
      }),
      type: Document,
      ...options,
    });
  }
}

```

In this code:

- **Document Class:**  
  The document is defined with a `timestamp` property that represents its creation time.

- **TimeBasedStore Class:**  
  The store uses a custom domain where the `timestamp` property is mapped into the replication space. The `open` method initializes the document store with this domain, ensuring that documents are replicated based on their time property.


## Bringing It All Together
Peerbit’s custom data domains represent a paradigm shift in replication:

- **Flexibility:**  
  Map any property—hash, time, or identity—into a continuous space, adapting replication to your application’s needs.
  
- **Efficiency:**  
  Dynamic adjustment of replication ranges minimizes rebalancing overhead and improves data availability.
  
- **Real-World Utility:**  
  Whether you’re building a decentralized document store or a live video stream, the ability to tailor replication based on content properties leads to more efficient and targeted data distribution.

Watch the upcoming video demonstration where the buffering timeline of a live video translates directly into the replication domain. As the video buffers, new segments are dynamically replicated, ensuring a seamless, real-time experience.

https://stream.dao.xyz/#/s/zb2rhZdJCHqzReHfLFokXaUTL8aqw7e3H5fHrfimifyCwokEz
