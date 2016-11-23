${HEADER}

${CODE_LINK}

In this example we'll add a visibility to our Edges so that we can control access to them.

Each row in our data file is a pair of integers with a colour label and a visibility. The visibility is either `"public"` or `"private"`. Anyone should be able to see Edges labelled `"public"` but only those with the correct credentials should be able to see `"private"` Edges. Of course, those allowed to access `"private"` Edges can also see `"public"` Edges.
${DATA}

First we'll have a step through of the code and see what happens when we run it. Then we'll go into a bit more detail about how it all works.

We have updated the generated to add the visibility label as a new property, a Java String, on the Edges:
${DATA_GENERATOR_JAVA}

The generated elements are:

```
${GENERATED_EDGES}
```

Because we've added an extra property we need to add it to the DataSchema - all the edges get a new `"visibility"` property. We have also told Gaffer that whenever it sees a property called 'visibility' this is a special property and should be used for restricting a user's visibility of the data.:
${DATA_SCHEMA_JSON}

and we've defined a new `"visibility.string"` type in our DataTypes, which is a Java String and must be non-null to be loaded into the Graph.:
${DATA_TYPES_JSON}

We've also updated the StoreTypes and specified that the visibility property is serialised using the custom ${VISIBILITY_SERIALISER_LINK} aggregated using the ${VISIBILITY_AGGREGATOR_LINK} function.
${STORE_TYPES_JSON}


After creating a Graph and adding our Edges to it we run a simple query to get back all edges containing vertex `"1"`... and we get nothing back.
This is because the user we ran the query with was not allowed to see "public" or "private", no edges were returned.

We can create a user that can see `public` Edges (and therefore not `private` edges) and update the query to use this user.

${GET_PUBLIC_SNIPPET}

If we rerun the query with a public user, we just get back the `public` Edges:

```
${GET_PUBLIC_RELATED_EDGES_RESULT}
```

We can also create a user that can see `private` Edges (and therefore `public` ones as well):

${GET_PRIVATE_SNIPPET}

If we rerun the query with the private user, we get back all of the Edges:

```
${GET_PRIVATE_RELATED_EDGES_RESULT}
```

Notice that the Edges have been aggregated. The `public` and `private` edges have been merged into a single Edge with `private` visibility.