# Helium - A Distributed Key Value Store 

Helium is a Distributed Key Value store. This is a Course project which I made while taking [Distributed Systems] (http://courses.engr.illinois.edu/cs425/fa2013/) course. The purpose of this project is to learn Distributed Systems in a Hands On way. 

Following are the salient features of the Helium 
 - A [Gossip based membership protocol] (http://en.wikipedia.org/wiki/Gossip_protocol) to keep track of Alive Hosts
 - Replication - Cassandra style passive replication on 3 machines to protect against failure tolerance
 - Partitioning and Load Balancing - [Consistent Hashing] (http://en.wikipedia.org/wiki/Consistent_hashing)
 - Concurrency - Support for ONE, QUORUM and ALL consistency levels
 - A Distributed Grep tool to grep the logs of different Servers from a single machine for convenient debugging

## Design
 - Each machine acting a Key Value Store will have HeliumServer process running. ConsistentHashing with SHA512 as hashing function is used to distribute keys among HeliumServers.
 - Each key is assigned a HeliumServer as coordinator. HeliumServer that is clockwise successor of the key position on the ring acts as its Coordinator.
 - All the requests(INSERT/UPDATE/DELETE) for a particular key are processed by coordinator for that key.
 - With the help of Distributed Grep, logs of HeliumServers residing at different machines can be easily grepped to debug different issues/operations from a single host.

## Replication
  - Coordinator replicates the keys for which it is responsible on its two clockwise successors. These two clock wise successors act as replicas for that key.
  - Passive replication is used. Coordinator is responsible for replicating all the updates on the Key to the replicas.
  - Since there are 3 copies of each key, System can tolerate upto 2 simultaneous failures. Figure below shows how keys are replicated among Coordinator and replicas.

Following figure shows how keys are distributed among coordinator and replicas - 

![alt tag](https://lh4.googleusercontent.com/-2qZEWzRXTYI/UrsqESQ_UpI/AAAAAAAABi8/X9AYzEGiJ3c/s606/MP4.png)

 - For key K1, C1 is the coordinator. Its two clockwise successors, C2 and C3 are replicas for the K1. Any Requests (INSERT/UPDATE/ DELETE) for K1 are received by C1. 
 - C1 executes the request locally and on C2 and C2 for ALL level. 
 - C1 executes the request only locally for ONE level.
 - C1 executes the request locally and on C2 for QUORUM level. 
 - C1 runs a background thread to execute QUORUM/ONE requests on its replicas asynchronously

## Handling Node Failures and Joins
  - Helium Servers use a gossip based membership protocol. This way they are aware of what Key Value Servers are part of the system.
  - Helium Sever serves two roles – coordinator and Replica. These two roles need to be taken over by some other Helium Server if this Helium Server fails. Similarly if a new node joins, it takes on the roles of coordinator and replica.
  - Re-replication ensures that there are always 3 copies of a key even after node failures. 

  Following figure shows a scenario when a machine C1 fails and the actions taken by other machines
![alt tag](https://lh4.googleusercontent.com/-mgiFawdM2hA/UrsqCSTXxiI/AAAAAAAABik/FQyr5EZ6OH8/s616/MP4-1.png)

  - C1 fails
  - C4 detects that C1 was in its replica group. C4 selects C3 as a new replica and copies K4 to C3
  - C3 detects that C1 was in its replica group. C3 selects C2 as a new replica and copies K3 to C2
  - C2 detects that its predecessor C1 has died. For C1’s keys K1, it becomes coordinator and replicates K1 to new replica C4

  Following figure shows a scenario when a new machine C5 joins and the actions taken by other machines
  ![alt tag](https://lh4.googleusercontent.com/-dfQgnviDNbY/UrsqDHv0hnI/AAAAAAAABi4/vW5AN_ftMVQ/s610/MP4-2.png)

  - C5 joins
  - C1 detects that C5 is its successor. C1 selects C5 as a new replica. It copies K1 to C5 and deletes K1 from C3
  - C4 detects that C5 is its second successor. C4 selects C5 as a new replica. It copies K4 to C5 and deletes K4 from C2
  - C2 detects that its C5 is the new predecessor. For C2’s keys K1, C5 becomes new coordinator. C2 sends K2 to C5 and deletes K2 from C4

## Concurrency
  - If the consistency level is ONE, coordinator processes that request locally
  - If the consistency level is ALL, coordinator processes that request locally as well as on both its replicas
  - If the consistency level is QUORUM, coordinator processes that request locally and its first replica
  - coordinator stamps each request with time-stamp equal to when that request was received at coordinator
  - coordinator runs a background thread which applies the updates corresponding to ONE and QUORUM requests on the replicas asynchronously. This guarantees eventual consistency and ensures that Last Writer Wins (LWW) property holds

## Membership Protocol
 Each host runs a Failure Detector process. This implements a gossip based membership protocol which is used to keep track of live hosts in the system. Gossip based protocol is very scalable for large number of machines. For n nodes, each node needs to transmit only log(n) gossip messages. So this is a very bandwidth efficient and fault tolerant scheme.
 
## Distributed Grep
 Debugging distributed applications can be very tricky and therefore I developed a distributed Grep tool that can be used to grep the logs of multiple Key Value servers from a single machine - 

  - Each host will have a FAILURE MONITOR process running. This process implements a Gossip style Membership algorithm and keeps track of live nodes that are part of Distributed System. Gossip Protocol scales very nicely as the number of machines in the system increases and is robust against process failures. For N hosts, each host receives gossip in log(N) time. 
  - GREP SERVER process running on each machine is responsible for handling Grep Requests for log files residing that host.
  - GREP CLIENT is a grep like command line utility which user uses to search for patterns. It sends the pattern to each host that is part of system and displays the results from all hosts to the user.
  - Messages are compressed using Google Protocol Buffers library before sending over the network. For frequently occurring patterns in large files, this will result in faster message delivery and less network bandwidth usage.
  - Grep Server is a multi-threaded process and can process multiple grep requests concurrently.
  - Grep Client is a multi-threaded process and can query multiple Grep Servers simultaneously. 


Following diagram illustrates the steps involved in processing a Grep command. The numbers on arrows indicate the order of events.

![alt tag](https://lh3.googleusercontent.com/-8KbAUPgxRpM/UrsqCUV-3sI/AAAAAAAABis/PKUdIPbpZDM/s811/MP1-new.png)