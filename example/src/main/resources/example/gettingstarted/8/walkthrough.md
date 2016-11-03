${HEADER}

${CODE_LINK}

This example demonstrates the aggregation functionality in Gaffer. Aggregation is applied at ingest and query time.

1. Ingest aggregation permanently aggregates similar elements together. Elements will be aggregated if:
    1. An entity has the same group, vertex, visibility and any specified groupBy property values are identical.
    2. An edge has the same group, source, destination, directed and any specified groupBy property values are identical.
2. Query aggregation allows additional summarisation depending on a user's visibility permissions and any overridden groupBy properties provided at query time.

The data file for this example has simple pairs of integers, as before and each pair has a timestamp and a visibility (public or private):

${DATA}

The schema is similar to what we have seen before, but we have added a start date and a end date property. These properties have been added to the groupBy field. Properties in the groupBy will be used to determine whether elements should be aggregated together at ingest.

##### Data schema
${DATA_SCHEMA_JSON}

##### Data types
${DATA_TYPES_JSON}

##### Store types
${STORE_TYPES_JSON}

Once we have loaded the data into Gaffer, we can fetch all the edges using a user that can see both public and private data.
${GET_SNIPPET}

All edges:

```
${GET_ALL_EDGES_RESULT}
```

You will see in the results, the timestamp has been converted into start and end dates - the midnight before and after the timestamp. This effectively creates a time bucket with day granularity. So all edges with a timestamp on the same day have been aggregated together (ingest aggregation). These aggregated edges have had all their properties aggregated together using the aggregate functions specified in the schema. At query time, the query time aggregation merged together similar edges with the same visibility, so 'public' and 'private' edges have become 'private' edges.


Next we will further demonstrate query time aggregation and get all the 'data' edges with the same source to the same destination to be aggregated together, regardless of the start and end dates. This is achieved by overriding the groupBy field with empty array so no properties are grouped on. Here is the operation:
${GET_ALL_EDGES_SUMMARISED_SNIPPET}

The summarised edges are as follows:

```
${GET_ALL_EDGES_SUMMARISED_RESULT}
```

We now have 2 'data' edges, 1 to 2 and 1 to 3. These edges are summaries of all the 'data' edges from 1 to 2 or 1 to 3. You can see the count property has been aggregated.


If we apply some pre aggregation filtering, we can return a time window summary of the edges. The new operation looks like:
${GET_ALL_EDGES_SUMMARISED_IN_TIME_WINDOW_SNIPPET}

The time window summaries are:

```
${GET_ALL_EDGES_SUMMARISED_IN_TIME_WINDOW_RESULT}
```

Again we have 2 edges, but this time the edges only contain data from within the time window of Jan 01 2016 to Jan 02 2016 (inclusive), demonstrating all edges within that window have been summarised together.

Finally we can repeat the previous time window query, with a user who can only see public data (not private). In this case the summarised edges don't contain any private data.
The public time window summaries are:

```
${GET_PUBLIC_EDGES_SUMMARISED_IN_TIME_WINDOW_RESULT}
```
