${HEADER}

${CODE_LINK}

In this example we'll add a visibility property to our Edges so that we can control access to them.

Let's assume that any road use information about junctions greater than 20 is somehow private and only user that have the private data access authorization are allowed to view them.

We will use the same data as previously but we need to modify the schema to add the new visibility property.

Here is the new data schema:

${DATA_SCHEMA_JSON}

we've add the new `"visibility"` property to just the RoadUse Edge. We have also told Gaffer that whenever it sees a property called 'visibility' this is a special property and should be used for restricting a user's visibility of the data.

and we've defined a new `"visibility"` type in our DataTypes, which is a Java String and must be non-null to be loaded into the Graph.:
${DATA_TYPES_JSON}

We've also updated the StoreTypes and specified that the visibility property is serialised using the custom ${VISIBILITY_SERIALISER_LINK} aggregated using the ${VISIBILITY_AGGREGATOR_LINK} binary operator.
${STORE_TYPES_JSON}


We have updated the generated to add the visibility label as a new property, a Java String, on the Edges:
${ELEMENT_GENERATOR_JAVA}.

After creating a Graph and adding our Edges to it we run a simple query to get back all RoadUse Edges containing vertex `"20"`... and we get nothing back.
This is because the user we ran the query with was not allowed to see "public" or "private", no edges were returned.

We can create a user that can see `public` Edges (and therefore not `private` edges) and update the query to use this user.

${GET_PUBLIC_SNIPPET}

If we rerun the query with a public user, we just get back the `public` Edges:

```
${GET_PUBLIC_EDGES_RESULT}
```

We can also create a user that can see `private` Edges (and therefore `public` ones as well):

${GET_PRIVATE_SNIPPET}

If we rerun the query with the private user, we get back all of the Edges:

```
${GET_PRIVATE_EDGES_RESULT}
```

As you saw here we did a query with 2 seeds. You can actually provide as many seeds here as you like and the Gaffer store will just handle it for you, batching them up if required.