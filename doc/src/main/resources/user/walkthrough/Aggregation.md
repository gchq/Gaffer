${HEADER}

${CODE_LINK}

Aggregation is a key powerful feature in Gaffer and we will guide you through it in this walkthrough. 

Aggregation is applied at ingest and query time.

1. Ingest aggregation permanently aggregates similar elements together. Elements will be aggregated if:
    1. An entity has the same group, vertex, visibility and any specified groupBy property values are identical.
    2. An edge has the same group, source, destination, directed and any specified groupBy property values are identical.
2. Query aggregation allows additional summarisation depending on a user's visibility permissions and any overridden groupBy properties provided at query time.

For this dev.walkthrough we have added a timestamp to our csv data:
${DATA}

The schema is similar to what we have seen before, but we have added a start date and a end date property. 
These properties have been added to the groupBy field. 
Properties in the groupBy will be used to determine whether elements should be aggregated together at ingest. 
Property values in the groupBy are required to be identical for Gaffer to aggregate them at ingest.

##### Elements schema
${ELEMENTS_JSON}

##### Types schema
${TYPES_JSON}

Once we have loaded the data into Gaffer, we can fetch all the edges using a GetAllElements operation.
Note this operation is not recommended for large Graphs as it will do a full scan of your database and could take a while to finish.
${GET_SNIPPET}

All edges:

```
${GET_ALL_EDGES_RESULT}
```

You will see in the results, the timestamp has been converted into start and end dates - the midnight before the timestamp and a millisecond before the following midnight, i.e. the start and end of the day. This effectively creates a time bucket with day granularity. 
So all edges with a timestamp on the same day have been aggregated together (ingest aggregation).
These aggregated edges have had all their properties aggregated together using the aggregate functions specified in the schema. 

Next we will further demonstrate query time aggregation and get all the 'RoadUse' edges with the same source to the same destination to be aggregated together, regardless of the start and end dates. 
This is achieved by overriding the groupBy field with empty array so no properties are grouped on. Here is the operation:

${GET_ALL_EDGES_SUMMARISED_SNIPPET}

The summarised edges are as follows:

```
${GET_ALL_EDGES_SUMMARISED_RESULT}
```

Now you can see the edges from 10 to 11 have been aggregated together and their counts have been summed together.

If we apply some pre-aggregation filtering, we can return a time window summary of the edges. The new operation looks like:
${GET_ALL_EDGES_SUMMARISED_IN_TIME_WINDOW_SNIPPET}

The time window summaries are:

```
${GET_ALL_EDGES_SUMMARISED_IN_TIME_WINDOW_RESULT}
```

Now we have all the RoadUse edges that fall in the time window May 01 2000 to May 03 2000. This filtered out all edges apart from 2 occuring between junction 10 and 11. Therefore, the count is has been aggregated to just 2 (instead of 3 as seen previously).

Aggregation also works nicely alongside visibilities. Data at different visibility levels is stored separately then at query time, the query time aggregation will summarise just the data that a given user can see.

There is another more advanced feature to query time aggregation.
When executing a query you can override the logic for how Gaffer aggregates properties together. 
So by default the count property is aggregated with Sum. 
At query time we could change that, to force the count property is aggregated with the Min aggregator, therefore finding the minimum daily count.
This feature doesn't affect any of the persisted values and any store aggregation that has already occurred will not be modified.
So in this example the Edges have been summarised into daily time buckets and the counts have been aggregated with Sum.
Now at query time we are able to ask: What is the minimum daily count?

Here is the java code:
${GET_ALL_EDGES_SUMMARISED_IN_TIME_WINDOW_WITH_MIN_COUNT_SNIPPET}

So, you can see we have just added an extra 'aggregator' block to the Operation view.
This can be written in json like this:

```json
${GET_ALL_EDGES_SUMMARISED_IN_TIME_WINDOW_RESULT_WITH_MIN_COUNT_JSON}
```
 
We have increased the time window to 3 days just so there are multiple edges to demonstrate the query time aggregation.
The result is:

```
${GET_ALL_EDGES_SUMMARISED_IN_TIME_WINDOW_RESULT_WITH_MIN_COUNT}
```
 
