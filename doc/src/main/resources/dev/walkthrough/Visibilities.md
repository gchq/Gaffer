${HEADER}

${CODE_LINK}

Another one of Gaffer's key features is visibility filtering, fine grained data access and query execution controls. 

In this example we'll add a visibility property to our edges so that we can control access to them.

Let's assume that any road use information about junctions greater than 20 is private and only users that have the `private` data access authorization are allowed to view them.

We will use the same data as before but we need to modify the schema to add the new visibility property.

Here is the new elements schema:

${ELEMENTS_JSON}

We've added the new `"visibility"` property to the RoadUse edge. We have also told Gaffer that whenever it sees a property called 'visibility' that this is a special property and should be used for restricting a user's visibility of the data.

We've defined a new `"visibility"` type in our Types, which is a Java String and must be non-null in order for the related edge to be loaded into the Graph.
We also specified that the visibility property is serialised using the custom ${VISIBILITY_SERIALISER_LINK} and aggregated using the ${VISIBILITY_AGGREGATOR_LINK} binary operator.
In our example, the serialiser is responsible for writing the visibility property into the store. This includes the logic which determines any hierarchy associated with the visibility properties (for example, `public` edges may be viewed by users with the `public` or `private` visibility label).
The aggregator is responsible for implementing the logic which ensures that edges maintain the correct visibility as they are combined (i.e that if a `public` edge is combined with a `private` edge, the result is also `private`).

${TYPES_JSON}

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

The visibility property as defined by the visibilityProperty field in the Schema is special case of a groupBy property.
- When ingest aggregation is carried out the visibilityProperty is treated as groupBy property.
- When query aggregation is carried out the visibilityProperty is no longer treated as a groupBy property.


To further demonstrate this here is another example:

You add these Edges:
```
1 -> 2   count = 1, visibility = "public"
1 -> 2   count = 2, visibility = "public"
1 -> 2   count = 10, visibility = "private"
1 -> 2   count = 20, visibility = "private"
```

These are persisted keeping the Edges with different visibilities separate, you can see the counts have been aggregated as they are not a groupBy property:
```
1 -> 2   count = 3, visibility = "public"
1 -> 2   count = 30, visibility = "private"
```

Then if a user with just "public" access to the system does a query they will just get back:
```
1 -> 2   count = 3, visibility = "public"
```

A user with "private" access, who by definition can also see "public" data, will get back both edges aggregated together:
```
1 -> 2   count = 33, visibility = "private"
```

