# *Tricalysia* (Tinkerpop, Neo4J, Jena...)

<<<<<<< HEAD
A set of (testing) interfaces for reading and writing triples in a store.
=======

A set of (testing) interfaces for reading and writing triples from a store.
>>>>>>> 97d3367... Update README.md

The project initialization includes the use of [Apache Tinkerpop](tinkerpop.apache.org/) for reading and writing the triples from a generic store. A basic RDF extraction from URLs using [Apache Jena](https://jena.apache.org) is implemented.

For testing purpose, [Neo4J-gremlin](tinkerpop.apache.org/docs/current/reference/#neo4j-gremlin) connector is included to store the triples (the graph traversal feature should be more effective for reading than writing). The [Neo4J-gremling-bolt](https://github.com/SteelBridgeLabs/neo4j-gremlin-bolt/) form *SteelBridgeLabs* is also used to access the store over the network using the Neo4J BOLT protocol.
