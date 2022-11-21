# Sharding

## Some background
Sharding in Peerbit is based on the content being committed. Each commit will be added to a log that represents some meaningful state, like an image or a document. 

Every change in Peerbit are committed with explicit links to the content it depends on. This means that by following the dependencies to the root, we can get the full state

<p align="center">
    <img width="400" src="./p0b.png"  alt="p1">
</p>


Every graph gets an graph id (GID)
<p align="center">
    <img width="400" src="./p7.png"  alt="p1">
</p>


When graphs merge (two independent states becomes dependent) the new graph will be named the same as the graph with the longest chain

<p align="center">
    <img width="400" src="./p8.png"  alt="p1">
</p>

<p align="center">
    <img width="400" src="./p9.png"  alt="p1">
</p>


Now this is important to background in order to understand how replicators/content leaders are chosen based on new changes. 

## The distribution algorithm
Imagine the commit above is made, so that the merged graph gets the label "DOG", how can we choose replicators in a fully connected network in a simple random way? (By being a replicator you have the task of storing the log and potentially also make it searchable for peers)

<p align="center">
    <img width="400" src="./p1.png"  alt="p1">
</p>


### 1. 
The first thing we need to do is to hash the labels of the peers (IPFS IDs) and the DOG label with a hash function (more details on this function later)

<p align="center">
    <img width="400" src="./p2.png"  alt="p2">
</p>


### 2. 
Secondly put all the hashes into a list and sort it. 

<p align="center">
    <img width="400" src="./p3.png"  alt="p3">
</p>


### 3. 
Now we lookup the labels from the hashes again

<p align="center">
    <img width="400" src="./p4.png"  alt="p4">
</p>


### 4. 
Now if we want 2 replicas of our content we can choose that the replicators are the 2 next elements in the list

<p align="center">
    <img width="400" src="./p5.png"  alt="p5">
</p>


The hash function is seeded with the checksum of the content itself, so it changes for every new commit. This means that the results would differ if the content changes. E.g. 

<p align="center">
    <img width="400" src="./p6.png"  alt="p5">
</p>



## When graphs merges and peers joins and leaves

When peers leave and join we need to redo leader selection for the heads of our content, this is because there might be replicators that no longer are online, or there might be new peers that should be replicators instead of someone else since the outcome of the algorithm is dependent on what peers participate in the replication process. 


Similarly graphs merge (like when the CAT and DOG became a DOG), this is functionally equivalent to that replicators of CAT stop to replicate and that DOG replicators start to replicate a larger log.


## Implementation

The implementation can be found [here](https://github.com/dao-xyz/peerbit/blob/2041b18bd955d7ca029c5a1e35a0892b06f89230/packages/client/src/peer.ts#L1236) (findLeaders method)
