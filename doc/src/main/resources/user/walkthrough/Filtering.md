${HEADER}

${CODE_LINK}

Filtering in Gaffer is designed so it can be applied server side and distributed across a cluster for performance.

In this example we’ll query for some Edges and filter the results based on the aggregated value of a property. 
We will use the same schema and data as the previous example.

If we query for the RoadUse Edges containing vertex `”10”` we get these Edges back with the counts aggregated:

```
${GET_ELEMENTS_RESULT}
```

Now let’s look at how to filter which Edges are returned based on the aggregated value of their count. For example, only return Edges containing vertex `”10”` where the `”count”` > 2.

We do this using a ${VIEW_JAVADOC} and ${VIEW_ELEMENT_DEF_JAVADOC} like this:

${GET_SNIPPET}

Our ViewElementDefinition allows us to perform post Aggregation filtering using a IsMoreThan Predicate.

If we run the query, we now get only those vertex `”10”` Edges where the `”count”` > 2:

```
${GET_ELEMENTS_WITH_COUNT_MORE_THAN_2_RESULT}
```