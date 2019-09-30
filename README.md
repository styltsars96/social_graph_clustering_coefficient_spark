# Social Graph Clustering Coefficient in Spark
Spark program that computes the clustering coefficient metric, at a local and a global level, given a social graph.
It does NOT use other libraries like graphX, as it is made to be a faster implementation.

## Introduction

Find the clustering coefficient metric, at a local (node/entity/person) and a
global level, in a social graph. Apache Spark is used for this, and the implementation
of the chosen algorithm is written in Java with only the Spark core libraries.

The input graph is a directed, and each edge represents a social
relationship (e.g. following or some equivalent form of exchange) between two people/accounts.
Each account is represented by an integer, and the input dataset should be composed of
pairs of integers. Each integer is in fact a node in the graph.
Throughout the problem and the solution implementation,
each node is represented by a unique integer as id.

In order to formulate the solution, some literature review has been done, to build an
algorithm that is well-suited for Map-Reduce. The written solution is based upon the
“CCFinder” Algorithm presented in this [paper](https://link.springer.com/article/10.1007/s11227-017-2040-8).

## Problem Modelling

In order to efficiently perform analysis on networks with billions of vertices, the
problem must be broken down and reduced to a form that is both easily processable by
a platform based on Map-Reduce and dataflow models, and scalable without much
communication and data overhead.

It has been found that computing the clustering coefficient can be reduced to triangle
counting, since the local clustering coefficient is the ratio of the number of triangles
incident to a vertex to the number of all possible triangles that it can participate!


In order to ensure acceptable performance in big graphs, this approach caches an
efficient and reusable data structure in the distributed memory using the Spark’s RDD
data abstraction. This data structure is called the “Filtered Ordered Neighbor List”,
**FONL** for short, and is designed specifically to hold the minimum amount of
information required for the triangle counting operations. Instead of keeping, as key-
value pairs, the complete neighbor list (value) for each node (key) cached, the FONL is
a Pair RDD that has the node as key, but the value is a list, in this implementation
an array/vector, which has the order of the node (number of neighbors) as the first
element, and the rest of the elements are the neighbors with equal or higher order
than the node / key. If there are no neighbors with a higher order, the value has only
the order of the node / key. If there are neighbors with a higher order than the node /
key, they are sorted before they are cached. Using the FONL the algorithm can
efficiently calculate the exact number of triangles in a given graph, without redundant
computations, because the algorithm will not parse the same node more times than
necessary.

In order to be able to find the clustering coefficient, the triangle counting algorithm
requires that each vertex has information about its degree tied to it for later use.

##### Here is an example of how a FONL is created by the list of neighbor lists of a small

example graph:
![alt text](https://github.com/styltsars96/social_graph_clustering_coefficient_spark/raw/master/images/img_1.jpg)

Following is an example of the FONL and how it can be visualized.
![alt text](https://github.com/styltsars96/social_graph_clustering_coefficient_spark/raw/master/images/img_2.jpg)

The FONL essentially keeps node **pairs** to be parsed for triangle counting. In order to
get one step closer to finding triangles, we need “candidate triangles”, i.e. **pairs of
edges** / **triples of nodes** , in a “ **Candidates List** ”. Once again, the vertices kept in
the list are of higher degree than the second node in the FONL pair. In a way, each
candidate is a path of two edges, which needs one more check in order to see if the
triple of nodes is indeed a triangle. From the above example the candidates list is as
follows:
![alt text](https://github.com/styltsars96/social_graph_clustering_coefficient_spark/raw/master/images/img_3.jpg)

This way, by **combining the FONL with the candidate list** we can find the triangles:
![alt text](https://github.com/styltsars96/social_graph_clustering_coefficient_spark/raw/master/images/img_4.jpg)

As it is evident, the algorithm steps can be based on these data structures to **count
the triangles by ”cogroup”ing the candidates with the FONL and doing a
sorted intersection** for each row by finding common neighbors. This way, the
resulting RDD has vertex as key and triangle count as value.

After having found the triangles and the degrees of the edges, the computation
of the Local Clustering Coefficient is just a left outer join of the FONL with the
Vertex-Triangle Count pair and computing the following for each row (where T is the
triangle count, v is the vertex, d is the degree, and the denominator is "d choose 2"):
![alt text](https://github.com/styltsars96/social_graph_clustering_coefficient_spark/raw/master/images/img_5.jpg)

Afterwards, based on previous results, the Global Clustering coefficient is computed, which is
essentially the triangle count over overall number of nodes choose 2.

### Algorithm steps overview

 The algorithm first constructs the neighbor list for each vertex. This is a
necessary step, in order to create the FONL.
* Then, the FONL is created from the induced neighbor lists, by counting the
neighbors of the vertex to find its degree, filtering out the vertices with lower
order, and sorting the remaining ones. The structure is kept for later use.
* Using the FONL, the candidate list is created by filtering out vertices that have
only one higher order neighbor, and the edges with no common neighbors.
* Join the FONL and the candidates list, in order to find the triangles and count
them for each vertex. This creates vertex-triangle count pairs.
* Use the above results to compute LCC and GCC.