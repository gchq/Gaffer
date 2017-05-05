${HEADER}

${CODE_LINK}

This example demonstrates how the LongsSketch sketch from the Data Sketches library can be used to maintain estimates of the frequencies of longs stored on on vertices and edges. For example suppose every time an edge is observed there is a long value associated with it which specifies the size of the interaction. Storing all the different longs on the edge could be expensive in storage. Instead we can use a LongsSketch which will give us approximate counts of the number of times a particular long was observed.

##### Data schema
This is our new data schema. The edge has a property called 'longsSketch'. This will store the LongsSketch object.
${DATA_SCHEMA_JSON}

##### Data types
We have added a new data type - 'longs.sketch'. This is a com.yahoo.sketches.frequencies.LongsSketch object.
${DATA_TYPES_JSON}

##### Store types
Here we have added in the serialiser and aggregator for the LongsSketch object. Gaffer will automatically aggregate these sketches, using the provided aggregator, so they will keep up to date as new edges are added to the graph.
${STORE_TYPES_JSON}

Only one edge is in the graph. This was added 1000 times, and each time it had the 'longs.sketch' property containing a randomly generated long between 0 and 9 (inclusive). The sketch does not retain all the distinct occurrences of these long values, but allows one to estimate the number of occurrences of the different values. Here is the Edge:

```
${GET_ALL_EDGES_RESULT}
```

This is not very illuminating as this just shows the default `toString()` method on the sketch. To get value from it we need to call methods on the LongsSketch object. Let's get estimates of the frequencies of the values 1 and 9.

We can fetch all cardinalities for all the vertices using the following code:
${GET_FREQUENCIES_OF_1_AND_9_FOR_EDGE_A_B_SNIPPET}

The results are as follows. As 1000 edges were generated with a long randomly sampled from 0 to 9 then the occurrence of each is approximately 100.

```
${GET_FREQUENCIES_OF_1_AND_9_FOR_EDGE_A_B}
```