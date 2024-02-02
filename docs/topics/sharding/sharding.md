# Resource-Aware Sharding

In distributed systems, sharding is an essential mechanism for distributing and managing data across multiple nodes. Adaptive sharding is a dynamic approach that adjusts data distribution based on various factors to optimize performance and resource utilization. This article delves into the implementation of adaptive sharding in Peerbit.

## The Goal
Starting from the end goal and working backward, the following are reasonable requirements that any distributed database should be able to adapt to:

(A) Controllable replication degree: Allow participants to agree on the data to be replicated at least N times, which should always be respected (if possible).

(B) Enable peers of different capacities to participate effectively: A more powerful computer should be able to do more work than a less powerful one.

(C) Efficient content retrieval: Use the knowledge from (A) to minimize the number of peers you have to ask for data.

Additionally, great Developer Experience (DX) is crucial and can sometimes influence the solutions you end up with. This was the initial idea for good DX when opening a database, where we control the minimum replication degree and provide resource constraints:

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
(A) and (B) can be seen as two forces acting on each other. On one hand, there's a force that makes nodes replicate as much as possible to satisfy the minimum replication degree. On the other hand, computers will eventually want to do as little as possible to save resources and, by extension, hardware and electricity costs.

For (A), we already have many solutions that work well but generally do not consider (B) and (C). For instance, in common DHT systems, we can use the identities of the participants to distribute content and pick neighbors to satisfy the minimum replication degree constraint.

However, once you mix in constraints from (B), this quickly becomes more challenging because you can no longer rely on your neighbors being ready to help you with replication. If you then need to "skip" neighbors for this reason, it might become challenging to keep up-to-date information on where all the replicas are located.

Additionally, imagine the data being stored are images, and you want to find all images that represent dogs. How many peers do you need to ask at least if the minimum number of replicas is 1, 2, 3... to ensure you will certainly find all dogs? (Asking every peer will always work, but will not be feasible when the network is large)

## The Solution
The solution's concept is based on peers representing *ranges* instead of points on a line that loops around (or a circle). And instead of making the point/range start at their "id," we allow peers to place themselves anywhere they please. Below is a walkthrough of how this solves (A), (B), and (C).

### (A) Satisfying Replication Degree

For simplicity, we consider that every peer can only have one range. And that range has a "width" that represents how much they need to replicate at least. If the width is 0.5, it means they need to store 50% of all data.

A piece of data that needs to be stored will be stored at a location that depends on its hash. But instead of using the hash, we transform it into a number bounded by [0,1].

<p align="center">
<img width="800" src="./topics/sharding/p2.png" alt="p2">
</p>

If the vertical line intersects with a range, that peer will be responsible for replicating this data. A nice consequence of this is that peers can participate with different degrees of trust in how much work others will perform.
<p align="center">
<img width="800" src="./topics/sharding/p4.png" alt="p4">
</p> 
By replicating with a factor (width) of 1, every data point will intersect the range, hence the node will always be responsible for every data point. This means that if anyone in a network creates data, it will always be sent to this peer. This is also useful property if you want to create an app where every peer always should hold the complete state locally at all times.

Another nice consequence of this is that if you only want to "pin" a specific data point, you only need to make your width as small as the floating points allow, to only cover that particular data point. *A line is a special case of a curve* and *pinning is a special case of range replication* (a range with width that approaches 0).

<p align="center">
<img width="800" src="./topics/sharding/p5.png" alt="p5">
</p> 

If there is a gap, then the closest node will be chosen in the following way:
<p align="center">
<img width="800" src="./topics/sharding/p6.png" alt="p3">
</p>

This means that even if the longer range is further away by measuring from the closest edge, it still needs to replicate the data due to that the transformed distance gets shorter because of the wider range. This property is important, because we wan't to make sure that someone who replicates with width 0 does not get delegated any replication work.



The "min replicas" (A) constraint is satisfied by inserting `min replicas` times starting from the starting point, where each jump is of length `1 / min replicas`.
<p align="center">
<img width="800" src="./topics/sharding/p7.png" alt="p7">
</p>

<p align="center">
<img width="800" src="./topics/sharding/p8.png" alt="p8">
</p>

If you think of the content space as a circle, this would represent a rotation of `360° / min replicas`. So if `min replicas = 2` and the start point is the north pole, the second point would be the south pole.

But we will stick with the line representation because it will be easier to visualize (consider that everything just wraps around at 1 instead).

### (B) Resource Awareness

<p align="center">
<img width="400" src="./topics/sharding/p10.png" alt="p10">
</p>

With this in (A) place, now it is time to consider constraint (B). The innovative step here is that we adjust our width to satisfy any resource constraint. Is the memory or CPU usage too high? Just reduce the width of your responsibility until satisfied. Do you have capacity? Then perhaps it would be helpful for others if you increase your width of responsibility.

<p align="center">
<img width="800" src="./topics/sharding/p9.png" alt="p9">
</p>


But this problem is actually more nuanced than just memory and CPU, for a healthy replication we also need to consider a few other goals.

<p align="center">
<img width="800" src="./topics/sharding/p13.png" alt="p13">
</p>


We cannot feasibly predict the optimal width for every participant in one go because we cannot continuously share all node info (CPU, memory, and other) usage to every other node at all times. Additionally, while data is inserted, storage-limited nodes will take up less width over time, so this is a continuous process. Therefore, the solution is to work iteratively where everyone adjusts their widths in small steps, and eventually the system converges to an optimal point.

For clarity these iterations on what happens when you update your width over time:
<p align="center">
<img width="800" src="./topics/sharding/p11.png" alt="p11">
</p>


The iterative solver for this could take many forms. Assume this problem is of a convex nature, in that there is always a known direction we can update our width to get closer to what we want, then this means we can efficiently use gradient descent to reach the goal.

In practice, a special form of it that comes in the form of something that is called a [PID controller](https://en.wikipedia.org/wiki/Proportional%E2%80%93integral%E2%80%93derivative_controller) will be used. A PID controller has similar properties as an adaptive gradient optimizer like the Adam optimizer that uses first and second-order moments to more quickly iterate towards a solution. PID controllers are very common in heat-regulating systems that allow you to maintain a constant temperature in a house even if the outside temperature is changing, by regulating the power the radiators use. Very simplified, the PID controller equivalence here for the memory constraint is that the house temperature is the memory usage, the outside temperature is the total amount of data that exists, and the radiator power is the range width.

A simplified mathematical representation of the iterator looks like this:

<p align="center">
<img width="800" src="./topics/sharding/p14.png" alt="p14">
</p>



Simplified, we can say we are using [Lagrange relaxation](https://en.wikipedia.org/wiki/Lagrangian_relaxation) to combine the constraints into one big objective function.

When everything works well, the width will converge to a number for every peer over time.

<p align="center">
<img width="800" src="./topics/sharding/p15.png" alt="p15">
</p>

If it is helpful for understanding: conceptually this is equivalent of that we are trying to regulate the heat in three houses at the same time, where the controller in one house depends on the other houses (if someone else is to do less replication work, I might have to do more work instead)

 
Source code for the PID controller can be found [here](https://github.com/dao-xyz/peerbit/blob/master/packages/programs/data/shared-log/src/pid.ts).

### (C) Efficient Aggregation

This is the final piece in the puzzle. How can we, with the solution outlined in (A), efficiently aggregate all unique data points to find all dog photos?

Simplified, by the way we have done the distribution by jumping with `1 / min replicas` for every replication. We know that if we "walk" along the axis with `1 / min replicas` distance we have actually had the opportunity to see all the data (!). (Though there will be edge cases for handling gaps, round the boundaries of the start and end point of our walk).

What is nice about this walk is that we can make it "local first" by starting to walk on our "range". For every step we take, we only need 1 node (unless you want to have redundancy in the search), so if multiple are overlapping we just consider the one with the longest width (so we have to consider few nodes as possible).

Consider the figure below for how aggregation is performed

Start "local first":
<p align="center">
<img width="500" src="./topics/sharding/p16.png" alt="p16">
</p>

Calculate how long you have too "walk":
<p align="center">
<img width="500" src="./topics/sharding/p17.png" alt="p17">
</p>

Aggregate every range, but don't consider more than one range per "step":
<p align="center">
<img width="500" src="./topics/sharding/p18.png" alt="p18">
</p>
 
Source code for the aggregation can be found [here](https://github.com/dao-xyz/peerbit/blob/95420cd37cb8d2ced4733495b6901b2b5e445e01/packages/programs/data/shared-log/src/ranges.ts#L155) 


## Demo
The [file-sharing](https://files.dao.xyz) app showcases how this technology behaves in practice. 

First we can see that peers get some segments in the content space. We choose starting points indenpendetly based on the public key. 

IMAGE


When CPU limitation is enabled we can see that if we minimize the tab of a client, it will stop replicating data. This because, a minimized tab is generally heavily throttled, which means processing capacity becomes limited. Once we re-open the tab again, we can see that everything returns to the previous optimal state. 

IMAGE

When memory limitation is enabled, we can see that the ranges only update once data is added. This is expected because this limitation is not constraining if no data is present. 

IMAGE

Try it ourself and read the source code [here](https://github.com/dao-xyz/peerbit-examples/tree/master/packages/file-share)


## Future Work and Improvements
### Scaling with Many Peers
When the number of replicators (or more specifically ranges/segments) is large, we will eventually run into a scaling problem, where everyone needs to know about everyone else (which will not be feasible). This is not a problem in general for existing DHT solutions which utilize the peer IDs and content address to make it possible size route table logarithmically with participation count.

For this system, we have lost the opportunity to use peer IDs this way because allowed peers to choose their "points" to be anywhere. But there is a nice solution around this because we have unlocked (C) and use recursion. The idea is that we create one or more databases that track replication ranges of peers:

"The loop":

Consider we create a database that contains all the responsibility ranges of another database as documents. Since there is realistically less data in this database, we should not need as many replicators. This means that the amount of metadata that describes the responsibilities of this database will be LESS.

Do the "The loop" to define databases that describe the replication of the next database. Eventually, the "head" database will be very small, and when it is small enough we can consider it as our "root" db. Now to interact with the database where content is actually stored, we can efficiently aggregate replicator ranges using (C) iteratively down the database chain until we reach the data. The solution will behave similarly as how partial routing tables work with common DHT systems, but the difference here is that we can control the replication of the partial routing tables as well.

### Numerical Optimizers

Previously described, the resource optimization problem was solved with a PID controller, under the assumption the problem has nice "convex" properties. This assumption might hold for many cases, but there might be scenarios where more robust (and more resource-heavy) solvers will be preferable. E.g. when non-numerical properties and non-linear features are used, an [RNN](https://en.wikipedia.org/wiki/Recurrent_neural_network) could work better.

Additionally, the parameters for the PID regulator perhaps need to be adaptive depending on network dynamics. For volatile networks, you might not want to too quickly adjust your width to others. Finding optimal `Kp`, `Ki` and `Kd`that minimizes convergence time, and unecessary data transfers would most likeli need a more robust estimator that can handle the most likeli non-linear properties. Therefore neural network models will naturallly be a good candidate here. 

