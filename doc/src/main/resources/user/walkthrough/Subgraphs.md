${HEADER}

${CODE_LINK}

This example extends the previous dev.walkthrough and demonstrates how a subgraph could be created using a single operation chain.

We will start by loading the data into the graph as we have done previously.

The operation chain is built up using several ${GET_ELEMENTS_JAVADOC} operations. 
In between each of these operations we cache the results in memory using a LinkedHashSet by executing the ${EXPORT_TO_SET_JAVADOC} operation. 
For larger graphs we could simply swap the ExportToSet to ${EXPORT_TO_GAFFER_RESULT_CACHE_JAVADOC} operation.

In order to chain GetElements operations together we need to extract the destination vertex of each result Edge and wrap the destination vertex in an EntitySeed so that it can be used as a seed for the next operation.

We can repeat this combination of operations for extract a subgraph contain 'x' number of hops around the graph. In this example we will just do 2 hops as our graph is quite basic. 
We finish off by using a ${FETCH_EXPORT_JAVADOC} operation to return the set of edges.

Although this results several operations in chain, each operation is quite simple and this demonstrates the flexibility of the operations. 

The full chain looks like:

${GET_SNIPPET}

For each 'hop' we use a different View, to specify the edges we wish to hop down and different filters to apply. 
The export operations will export the currently result and pass the result onto the next operation. This is why we have the slightly strange DiscardOutput operation before we return the final results. 

The result is the full set of traversed Edges:

```csv
${SUB_GRAPH}
```

Here are some further export ${OPERATION_EXAMPLES_LINK} that demonstrate some more advanced features of exporting. 