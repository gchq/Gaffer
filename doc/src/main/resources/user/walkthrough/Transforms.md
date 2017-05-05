${HEADER}

${CODE_LINK}

In this example we’ll look at how to query for Edges and then add a new transient property to the Edges in the result set.
Again, we will just use the same schema and data as in the previous walkthough. 

A transient property is just a property that is not persisted, simply created at query time by a transform function. We’ll create a 'description' transient property that will summarise the contents of the aggregated Edges.

To do this we need a [Function](https://docs.oracle.com/javase/8/docs/api/java/util/function/Function.html). Here is our ${DESCRIPTION_TRANSFORM_LINK}.

This transform function takes 3 values, the `”SOURCE”` vertex, the `”DESTINATION”` vertex and `”count”` property and produces a description string.

This transform function then needs to be configured using an [ElementTransformer](http://gchq.github.io/Gaffer/uk/gov/gchq/gaffer/data/element/function/ElementTransformer.html):

${TRANSFORM_SNIPPET}

Here you can see we `select` the `”SOURCE”` vertex, the `”DESTINATION”` vertex and `”count”` property and `project`, them into the new `”description”` transient property.

We add the new `”description”` property to the result Edge using a `View` and then execute the operation.

${GET_SNIPPET}

This produces the following result:

```
${GET_ELEMENTS_WITH_DESCRIPTION_RESULT}
```

As you can see we’ve now got a new `”description”` property on each Edge.