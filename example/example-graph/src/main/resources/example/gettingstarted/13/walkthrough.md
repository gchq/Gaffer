${HEADER}

${CODE_LINK}

This example demonstrates how the Union sketch from the Data Sketches library can be used to maintain estimates of the cardinalities of sets. The Union sketch is similar to a HyperLogLogPlusPlus, but it can also be used to create the intersections of sets. We give an example of how this can be used to monitor the changes to the number of edges in the graph over time.

##### Data schema
This is our new data schema. The edge has properties called 'startDate' and 'endDate'. These will be set to the midnight before the time of the occurrence of the edge and to midnight after the time of the occurrence of the edge. There is also a size property which will be a Union. This property will be aggregated over the 'groupBy' properties of startDate and endDate.
${DATA_SCHEMA_JSON}

##### Data types
We have added a new data type - 'union'. This is a com.yahoo.sketches.theta.Union object.
${DATA_TYPES_JSON}

##### Store types
Here we have added in the serialiser and aggregator for the Union object. Gaffer will automatically aggregate these sketches, using the provided aggregator, so they will keep up to date as new edges are added to the graph.
${STORE_TYPES_JSON}

1000 different edges were added to the graph for the day 09/01/2017 (i.e. the startDate was the midnight at the start of the 9th, and the endDate was the midnight at the end of the 9th). For each edge, an Entity was created, with a vertex called "graph". This contained a Union object to which a string consisting of the source and destination was added. 500 edges were added to the graph for the day 10/01/2017. Of these, 250 were the same as edges that had been added in the previous day, but 250 were new. Again, for each edge, an Entity was created for the vertex called "graph".

Here is the Entity for the different days:
```
${GET_ENTITIES}
```

This is not very illuminating as this just shows the default `toString()` method on the sketch. To get value from it we need to call a method on the Union object:
${GET_ESTIMATE_SEPARATE_DAYS_SNIPPET}
The result is:
```
${GET_ESTIMATE_OVER_SEPARATE_DAYS}
```

Now we can get an estimate for the number of edges in common across the two days:
${INTERSECT_ACROSS_DAYS_SNIPPET}
The result is:
```
${PRINT_ESTIMATE}
```

We now get an estimate for the number of edges in total across the two days, by simply aggregating overall the properties:
${UNION_ACROSS_DAYS_SNIPPET}

The result is:

```
${UNION_ESTIMATE}
```