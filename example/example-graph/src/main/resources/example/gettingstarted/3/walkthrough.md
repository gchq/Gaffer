${HEADER}

${CODE_LINK}

In this example we’ll query for some Edges and filter the results based on the aggregated value of a property.

The Edges we generate are similar to example 1. We’ll just take some pairs of integers, create Edges with Group `”data”` and a `”count”` property:

```
${GENERATED_EDGES}
```

If we query for the Edges containing vertex `”1”` we get the Edges back with their counts aggregated:

```
${GET_RELATED_EDGES_RESULT}
```

Now let’s look at how to filter which Edges are returned based on the aggregated value of their count. For example, only return Edges containing vertex `”1”` where the `”count”` > 3.

We do this using a ${VIEW_JAVADOC} and ${VIEW_ELEMENT_DEF_JAVADOC} like this:

${GET_SNIPPET}

In the dataTypes file we specify validation conditions. You’ll see that we’ve specified there that the `”count”` property’s value must be >= 0:

${DATA_TYPES_JSON}

Our ViewElementDefinition allows us to override this by using a ${FILTER_FUNCTION_JAVADOC} function.

If we run the query, we now get only those vertex `”1”` Edges where the `”count”` > 3:

```
${GET_RELATED_ELEMENTS_WITH_COUNT_MORE_THAN_3_RESULT}
```