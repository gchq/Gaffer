${HEADER}

${CODE_LINK}

This example demonstrates how the BoundedTimestampSet property can be used to maintain a set of the timestamps at which an element was seen active. If this set becomes larger than a size specified by the user then a uniform random sample of the timestamps is maintained. In this example we record the timestamps to minute level accuracy, i.e. the seconds are ignored, and specify that at most 25 timestamps should be retained.

##### Data schema
This is our new data schema. The edge has a property called 'boundedTimestampSet'. This will store the BoundedTimestampSet object, which is actually a 'BoundedTimestampSet'.
${DATA_SCHEMA_JSON}

##### Data types
We have added a new data type - 'bounded.timestamp.set'. This is a uk.gov.gchq.gaffer.time.BoundedTimestampSet object.
${DATA_TYPES_JSON}

##### Store types
Here we have added in the serialiser and aggregator for the BoundedTimestampSet object. Gaffer will automatically aggregate these sets together to maintain a set of all the times the element was active. Once the size of the set becomes larger than 25 then a uniform random sample of size at most 25 of the timestamps is maintained.
${STORE_TYPES_JSON}

There are two edges in the graph. Edge A-B was added 3 times, and each time it had the 'boundedTimestampSet' property containing a randomly generated timestamp from 2017. Edge A-C was added 1000 times, and each time it also had the 'boundedTimestampSet' property containing a randomly generated timestamp from 2017. Here are the edges:

```
${GET_ALL_EDGES_RESULT}
```

You can see that edge A-B has the full list of timestamps on the edge, but edge A-C has a sample of the timestamps.
