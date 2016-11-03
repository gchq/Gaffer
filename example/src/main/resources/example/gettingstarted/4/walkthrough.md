${HEADER}

${CODE_LINK}

In this example we’ll look at how to query for a some Edges and then add a new property to the Edges in the result set.

The consists of pairs of integers as usual, but this time we have an extra value associated with each pair:

${DATA}

From this we’ll create edges with a `”count”` property as usual and an additional property, we’ll call it `”thing”` derived from the third value in the data file:

```
${GENERATED_EDGES}
```

Then we run a ${GET_RELATED_EDGES_JAVADOC} query for vertex `”1”` like we’ve done before to get the aggregated results:

```
${GET_RELATED_EDGES_RESULT}
```

Nothing we haven’t seen before.

Now we’ll add a new property to these results at query time. This is an example of a transient property - a property that is not persisted, just created at query time by transform functions. We’ll calculate the mean value of `”thing”` for each Edge summary.

To do this we create an ${ELEMENT_TRANSFORMER_JAVADOC}:

${TRANSFORM_SNIPPET}

This `select`s the `”count”` and `”thing”` properties and maps, or `projects`, them into the `”mean”` property. The ${FUNCTION_JAVADOC} that does the mapping is a ${TRANSFORM_FUNCTION_JAVADOC}; in this case a ${MEAN_TRANSFORM_LINK}.

We add the new `”mean”` property to the result Edge set using a `View` and then execute the operation.

${GET_SNIPPET}

If you run the query you’ll get:

```
${GET_RELATED_ELEMENTS_WITH_MEAN_RESULT}
```

Comparing with the previous query we’ve now got a new `”mean”` property on the results.