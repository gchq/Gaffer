${HEADER}

${CODE_LINK}

This time we'll see at what happens when we have more than one Group.

The data file for this example has simple pairs of integers, as before, but now each pair has a colour as well. It's worth noting that in the data file the same pairs sometimes appear more than once but with different colour labels:

${DATA}

#### Graph Elements

Like the previous example, we use a DataGenerator to turn each line of the file into a Gaffer Edge. The generator for this example is:
${DATA_GENERATOR_JAVA}

As before, we add a "count" property to each Edge but this time we use the colour label on each row of the file as the Group of the Edge. We get these Gaffer Edges from our data file:

```
${GENERATED_EDGES}
```

#### Schemas

Because we now have multiple Groups, the DataSchema, is different to the previous example. We've still got the same types defined in the DataTypes and StoreTypes files, but we've now got more Edges:

${DATA_SCHEMA_JSON}

The StoreProperties haven't changed because we are using exactly the same Store as before, we're just putting different data into it.

#### Loading and Querying the Data

We create a Graph and load the data using the ${ADD_ELEMENTS_JAVADOC} Operation exactly the same as in the previous example.

This time we'll run 2 queries using ${GET_RELATED_EDGES_JAVADOC}.

The first one is exactly the same as last time. We ask for all of the Edges containing the Vertex "1". The result is:

```
${GET_RELATED_EDGES_RESULT}
```

The `”count”` properties are still aggregated but only within each Group.

Our second query is to return all "red" Edges. We still use the ${GET_RELATED_EDGES_JAVADOC} Operation like before but this time we add a ${VIEW_JAVADOC} to it:

${GET_SNIPPET}

and only get the "red" Edges containing Vertex "1" with their counts aggregated:

```
${GET_RELATED_RED_EDGES_RESULT}
```

Compare these results with the ones from the previous query.

We'll explore the ${VIEW_JAVADOC} in more detail over the next few examples.