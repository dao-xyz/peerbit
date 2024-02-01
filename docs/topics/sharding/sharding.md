Here's the corrected version with spelling and grammar errors fixed:

# Resource-Aware Sharding

In distributed systems, sharding is a crucial mechanism to distribute and manage data across multiple nodes. Adaptive sharding is a dynamic approach that adjusts the distribution of data based on various factors to optimize performance and resource utilization. This article explores the implementation of adaptive sharding with Peerbit.

## The Goal
Starting from the end goal and working backward, below are reasonable requirements that any distributed database needs to be able to adapt to.

(A) Controllable replication degree. Allow participants to agree on what data should be replicated at least N times, and this should always be respected (if possible).

(B) Allow peers of different capacity to participate effectively. A powerful computer should be able to do more work than a less powerful one.

(C) Find content efficiently. By using the knowledge from (A) to reduce the number of peers you have to ask for data.

Additionally, great Developer Experience (DX) is also important, and can sometimes affect what solutions you end up with, so this was the initial idea for a good DX when opening a database where we control min replication degree and provide resource constraints.

```ts
const peer = await Peerbit.create();
const db = peer.open("address", {
    args: {
        replicas: {
            min: 3
        },
        limits: {
            storage: 1e6, // limit storage to 1mb
            cpu: { max: 0.5 } // allow 50% CPU utilization
        }
    }
});
```

## The Problem with Existing Solutions
(A) and (B) can be seen as two forces acting on each other. On one hand, you have a force that makes nodes replicate as much as possible to satisfy the min replication degree. On the other hand, computers will eventually want to do as little as possible to save resources and, in extension, hardware and electricity costs.

For (A), we already have a lot of solutions that work well but generally do not consider (B) and (C). For example, in common DHT systems, we can use the identities of the participants to distribute content and picking neighbors to satisfy the minimum replication degree constraint.

<p align="center">
<img width="800" src="./topics/sharding/p1.png" alt="p1">
</p>

However, once you mix in constraints from (B), this quickly becomes more challenging because no longer can you rely on that your neighbors are ready to help you out with replication. If you then need to "skip" neighbors for this reason, it might become challenging to actually keep up-to-date info where all the replicas are hanging around.

Additionally, imagine the data that is stored are images, and you want to find all images that represent dogs. How many peers do you need to ask at least if min replicas are 1, 2, 3... to make sure you will certainly find all dogs? (Asking every peer will always work, but will not be feasible when the network is large)

## The Solution
The idea of the solution is based on that instead of peers representing points on a line that loops around (or a circle). Peers are responsible for *ranges*. And instead of making the point/range start at their "id" we allow peers to place themselves anywhere they please. Below is a walkthrough of how this solves (A), (B), and (C). Afterwards, challenges and future work are discussed.

### (A) Satisfy Replication Degree

For simplicity, we consider that every peer can only have one range. And that range has a "width" that represents how much they need to replicate at least. If the width is 0.5, it means they need to store 50% of all data.

<p align="center">
<img width="800" src="./topics/sharding/p2.svg" alt="p2">
</p>

A piece of data that needs to be stored is to be stored at a location that depends on its hash. But instead of using the hash, we are transforming it into a number bounded by [0,1].

If the vertical line intersects with a range, that peer will be responsible to replicate this data. If there is a gap, then the closest node will be chosen in the following way.

<p align="center">
<img width="800" src="./topics/sharding/p3.svg" alt="p3">
</p>

A nice consequence of this is that peers can participate with different degrees of trust in how much work others will perform.

IMAGE

By replicating with a factor (width) of 1, every data point will intersect the range, hence always be responsible for every data point.

Another nice consequence of this is that you only want to "pin" a specific data point, you only need to make your width be as small as the floating points allow, to only cover that particular data point. *A line is a special case of a curve* and *pinning is a special case of range replication* (a range with width that approaches 0).

IMAGE

Min replicas (A) constraint is satisfied by inserting `min replicas` times starting from the starting point. Where each jump is of length `1 / min replicas`.

IMAGE

If you think of content space as a circle, this would represent a rotation of `360° / min replicas`. So if `min replicas = 2` and the start point is the north pole, the second point would be the south pole.

But we are going to stick with the line for now on because it will be easier to visualize (consider that everything just wraps around at 1 instead).

### (B) Resource Awareness

With this in (A) place, now it is time to consider constraint (B). The innovative step here is that we adjust our width to satisfy any resource constraint. Is the memory or CPU usage too high? Just reduce the width of your responsibility until satisfied. Do you have capacity? Then perhaps it would be helpful for others if you increase your width of responsibility.

But this problem is actually more nuanced than just memory and CPU, for a healthy replication we also need to consider a few other goals.

IMAGE

We cannot feasibly predict the optimal width for every participant in one go because we cannot continuously share all node info (CPU, memory, and other) usage to every other node at all times. Additionally, while data is inserted, storage-limited nodes will take up less width over time, so this is a continuous process. Therefore, the solution is to work iteratively where everyone adjusts their widths in small steps, and eventually the system converges to an optimal point.

The iterative solver for this could take many forms. Assume this problem is of a convex nature, in that there is always a known direction we can update our width to get closer to what we want. This means we can efficiently use gradient descent to reach the goal.

In practice, a special form of it that comes in the form of something that is called a [PID controller](https://en.wikipedia.org/wiki/Proportional%E2%80%93integral%E2%80%93derivative_controller) will be used. A PID controller has similar properties as an adaptive gradient optimizer like the Adam optimizer that uses first and second-order moments to more quickly iterate towards a solution. PID controllers are very common in heat-regulating systems that allow you to maintain a constant temperature in a house even if the outside temperature is changing, by regulating the power the radiators

 use. Very simplified, the PID controller equivalence here for the memory constraint is that the house temperature is the memory usage, the outside temperature is the total amount of data that exists, and the radiator power is the range width.

A simplified mathematical representation of the problem looks like this.

IMAGE

Simplified, we can say we are using [Lagrange relaxation](https://en.wikipedia.org/wiki/Lagrangian_relaxation) to combine the constraints into one big objective function.

When everything works well, the width will converge to a number for every peer over time.

### (C) Efficient Aggregation

This is the final piece in the puzzle. How can we, with the solution outlined in (A), efficiently aggregate all unique data points to find all dog photos?

Simplified, by the way we have done the distribution by jumping with `1 / min replicas` for every replication. We know that if we "walk" along the axis with `1 / min replicas` distance we have actually had the opportunity to see all the data (!). (Though there will be edge cases for handling gaps, round the boundaries of the start and end point of our walk).

What is nice about this walk is that we can make it "local first" by starting to walk on our "range". For every step we take, we only need 1 node, so if multiple are overlapping we just consider the one with the longest width (so we have to consider few nodes as possible).

IMAGE

## Future Work and Improvements
### Scaling with Many Peers
When the number of replicators (or more specifically ranges/segments) is large, we will eventually run into a scaling problem, where everyone needs to know about everyone else (which will not be feasible). This is not a problem in general for existing DHT solutions which utilize the peer IDs and content address to make it possible size route table logarithmically with participation count.

For this system, we have lost the opportunity to use peer IDs this way because allowed peers to choose their "points" to be anywhere. But there is a nice solution around this because we have unlocked (C) and use recursion. The idea is that we create one or more databases that track replication ranges of peers:

"The loop":

Consider we create a database that contains all the responsibility ranges of another database as documents. Since there is realistically less data in this database, we should not need as many replicators. This means that the amount of metadata that describes the responsibilities of this database will be LESS.

Do the "The loop" to define databases that describe the replication of the next database. Eventually, the "head" database will be very small, and when it is small enough we can consider it as our "root" db. Now to interact with the database where content is actually stored, we can efficiently aggregate replicator ranges using (C) iteratively down the database chain until we reach the data. The solution will behave similarly as how partial routing tables work with common DHT systems, but the difference here is that we can control the replication of the partial routing tables as well.

### Numerical Optimizers

Previously described, the resource optimization problem was solved with a PID controller, under the assumption the problem has nice "convex" properties. This assumption might hold for many cases, but there might be scenarios where more robust (and more resource-heavy) solvers will be preferable. E.g. when non-numerical properties and non-linear features are used, an [RNN](https://en.wikipedia.org/wiki/Recurrent_neural_network) could work better.

Additionally, the parameters for the PID regulator perhaps need to be adaptive depending on network dynamics. For volatile networks, you might not want to too quickly adjust your width to others. The current implementation supports the concepts of maturity times with hard cutoffs, but this solution might need to be more "continuous".