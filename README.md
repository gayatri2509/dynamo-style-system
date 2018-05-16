# dynamo-style-system
#### Date completed: May 2017

* Technologies used: Android (Java)

Implemented a simplified version of Amazon's dynamo which includes functionalities like partitioning, replication and failure handling. This system creates client and server threads, opens sockets and responds to the incoming requests.

It utilizes socket programming using Asynctask in multithreaded environment to send and receive requests among multiple nodes in a distributed system. The main goal is to provide both availability and linearizability at the same time. The content provider is designed such that it supports concurrent read/write operations while handling failures. 

Different operations supported by the system include insert, query and delete. It works as follows:-
* If "*" is given as the selection parameter to query(), then it returns all the <key, value> pairs stored in the system.

* If "@" is given as the selection parameter to query() on an AVD, it returns all <key, value> pairs stored in the local partition of the node, i.e., all <key, value> pairs stored locally in the AVD on which query() is run.

* If "*" is given as the selection parameter to delete(), then it deletes all <key, value> pairs stored in the system.

* If "@" is given as the selection parameter to delete() on an AVD, it deletes all <key, value> pairs stored in the local partition of the node, i.e., all <key, value> pairs stored locally in the AVD on which delete() is run.

* If no parameter is used then query() returns the value corresponding to the given key and delete() deletes the value corresponding to the given key.

Following are the functionalities supported by the system:-
1) Partitioning:  
Each node is capable of handling a particular set of keys. Every node knows the address of every other node in the system and knows exactly which partition belongs to which node; any node can forward a request to the correct node without using a ring-based routing.

2) Replication:  
For linearizability, quorum-based replication, used by Dynamo, was implemented. The replication degree N is 3. This means that given a key, the key’s co-ordinator as well as the 2 successor nodes in the Dynamo ring should store the key. For write operations, all objects are versioned in order to distinguish stale copies from the most recent copy. For read operations, if the readers in the reader quorum have different versions of the same object, the co-ordinator picks the most recent version and return it.

3) Failure Handling:  
Just as the original Dynamo, each request is used to detect a node failure. For this purpose, timeout for a socket read is used with timeout value of 1000 ms; and if a node does not respond within the timeout, it is considered as failed. If its detected that the original co-ordinator has failed then the request is forwarded to its successor. On recovery, the node copies all the objects that it missed during the failure.




