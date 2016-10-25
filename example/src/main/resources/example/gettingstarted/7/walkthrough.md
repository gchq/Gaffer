${HEADER}

${CODE_LINK}

This example extends the previous operation chain example and demonstrates how a subgraph could be create using a single operation chain.

The schema and data is the same as the previous example. We start by loading the data into the graph as we have done previously.

The operation chain is built up using several ${GET_RELATED_EDGES_JAVADOC} operations. In between each of these operations we cache the results in memory using a LinkedHashSet (so this is only applicable to small subgraphs) by executing the ${UPDATE_EXPORT_JAVADOC} operation.
In order to chain the get related edges operations together we also have to extract the destination vertex of each edge first using an ${ENTITY_SEED_EXTRACTOR_JAVADOC}:

${EXTRACTOR_SNIPPET}

For each edge this will check the input is an edge, skipping any entities (we only have edges in our graph so this isn't a problem'), then return the destination vertex. Once we have a collection of vertices we are then able to pass the result into our next get related edges operation.

We can repeat this combination of operations for extract a subgraph contain 'x' number of hops around the graph. In this example we will just do 2 hops. We finish off by using a ${FETCH_EXPORT_JAVADOC} operation to return the set of edges.

The full chain looks like:

${GET_SNIPPET}

For each 'hop' we could use a different View, to specify the edges we wish to hop down and different filters to apply.

The result is:

```csv
${SUB_GRAPH}
```