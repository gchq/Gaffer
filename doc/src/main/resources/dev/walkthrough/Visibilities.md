${HEADER}

${CODE_LINK}

Another one of Gaffer's key features is visibility filtering, fine grained data access and query execution controls. 

In this example we'll add a visibility property to our edges so that we can control access to them.

Let's assume that any road use information about junctions greater than 20 is private and only users that have the `private` data access authorization are allowed to view them.

We will use the same data as before but we need to modify the schema to add the new visibility property.

Here is the new data schema:

${DATA_SCHEMA_JSON}

We've added the new `"visibility"` property to the RoadUse edge. We have also told Gaffer that whenever it sees a property called 'visibility' that this is a special property and should be used for restricting a user's visibility of the data.

We've defined a new `"visibility"` type in our DataTypes, which is a Java String and must be non-null in order for the related edge to be loaded into the Graph:

${DATA_TYPES_JSON}

We've also updated the StoreTypes and specified that the visibility property is serialised using the custom ${VISIBILITY_SERIALISER_LINK} and aggregated using the ${VISIBILITY_AGGREGATOR_LINK} binary operator. In our example, the serialiser is responsible for writing the visibility property into the store. This includes the logic which determines any hierarchy associated with the visibility properties (for example, `public` edges may be viewed by users with the `public` or `private` visibility label). The aggregator is responsible for implementing the logic which ensures that edges maintain the correct visibility as they are combined (i.e that if a `public` edge is combined with a `private` edge, the result is also `private`).

${STORE_TYPES_JSON}

We have updated the generator to add the visibility label as a new property (a Java String) on the edges:

${ELEMENT_GENERATOR_JAVA}

After creating a Graph and adding our edges to it we run a simple query to get back all RoadUse edges containing vertex `"20"`... and we get nothing back.
This is because the user we ran the query as was not allowed to see edges with a visibility of `public` or `private`, so no edges were returned.

We can create a user that can see `public` edges only (and not `private` edges) and then run the query as this user.

${GET_PUBLIC_SNIPPET}

If we rerun the query with a public user, we just get back the `public` edges:

```
${GET_PUBLIC_EDGES_RESULT}
```

We can also create a user that can see `private` edges (and therefore `public` ones as well):

${GET_PRIVATE_SNIPPET}

If we rerun the query as the private user, we get back all of the edges:

```
${GET_PRIVATE_EDGES_RESULT}
```

Here we performed a query with just 2 seeds. You can provide as many seeds here as you like and the Gaffer store will handle it for you, batching them up if required.