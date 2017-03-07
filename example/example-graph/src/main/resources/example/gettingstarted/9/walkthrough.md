${HEADER}

${CODE_LINK}

This example demonstrates how storing a HyperLogLogPlus object on each vertex in a graph allows us to quickly estimate its degree (i.e. the number of edges it is involved in). The estimate can be obtained without scanning through all the edges involving a node and so is very quick, even if the degree is very large.

To add properties to vertices we need to add an Entity to our schema. Entities are associated with a vertex and contain a set of properties about that vertex.

##### Data schema
This is our new data schema. You can we we have added a Cardinality Entity. This will be added to every vertex in the Graph. This Entity has a 'Cardinality' property that will hold the cardinality value.
${DATA_SCHEMA_JSON}

The data schema now also has 2 different edges, a red edge and a blue edge. To allow users to differentiate between the cardinality of each edge we have added an edgeGroup property to the Cardinality Entity. This property is included in the groupBy so that it prevents Cardinalities for the different edge colours being merged together.

##### Data types
We have added a new data type - hllp. This is a HyperLogLogPlus object.
${DATA_TYPES_JSON}


##### Store types
Here we have added in the serialiser and aggregator for the HyperLogLogPlus object. Gaffer will automatically aggregate the cardinalities, using the provided aggregator, so they will keep up to date as new elements are added to the graph.
${STORE_TYPES_JSON}


Here are all the edges loaded into the graph:

```
${GET_ALL_EDGES_RESULT}
```


We can fetch all cardinalities for all the vertices using the following operation:
${GET_ALL_CARDINALITIES_SNIPPET}

If we look at the cardinality value of the HyperLogLogPlus property the values are:

```
${ALL_CARDINALITIES_RESULT}
```

You can see we get a different cardinality value for the different colour edges at each vertex. If we want to merge these cardinalities together we can add 'groupBy=[]' to the operation view to override the groupBy defined in the schema.
${GET_ALL_SUMMARISED_CARDINALITIES_SNIPPET}
Now you can see the cardinality values have been merged together at each vertex:

```
${ALL_SUMMARISED_CARDINALITIES_RESULT}
```

For large Graphs it is not recommended to use the 'Get All' methods as this could result in a full scan of the data and may return a lot of results. This next snippet shows you how you can query for a single cardinality value.
${GET_RED_EDGE_CARDINALITY_SNIPPET}
As you can see the query just simply asks for an entities at vertex '1' and filters for only 'Cardinality' entities that have an edgeGroup property equal to 'red'. The result is:

```
${CARDINALITY_OF_1_RESULT}
```