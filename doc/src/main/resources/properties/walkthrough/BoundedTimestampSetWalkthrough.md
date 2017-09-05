${HEADER}

${CODE_LINK}

This example demonstrates how the [BoundedTimestampSet](https://github.com/gchq/Gaffer/blob/master/library/time-library/src/main/java/uk/gov/gchq/gaffer/time/BoundedTimestampSet.java) property can be used to maintain a set of the timestamps at which an element was seen active. If this set becomes larger than a size specified by the user then a uniform random sample of the timestamps is maintained. In this example we record the timestamps to minute level accuracy, i.e. the seconds are ignored, and specify that at most 25 timestamps should be retained.

##### Elements schema
This is our new schema. The edge has a property called 'boundedTimestampSet'. This will store the [BoundedTimestampSet](https://github.com/gchq/Gaffer/blob/master/library/time-library/src/main/java/uk/gov/gchq/gaffer/time/BoundedTimestampSet.java) object, which is actually a 'BoundedTimestampSet'.
${ELEMENTS_JSON}

##### Types schema
We have added a new type - 'bounded.timestamp.set'. This is a uk.gov.gchq.gaffer.time.BoundedTimestampSet object. We have added in the [serialiser](https://github.com/gchq/Gaffer/blob/master/library/time-library/src/main/java/uk/gov/gchq/gaffer/time/serialisation/BoundedTimestampSetSerialiser.java) and [aggregator](https://github.com/gchq/Gaffer/blob/master/library/time-library/src/main/java/uk/gov/gchq/gaffer/time/binaryoperator/BoundedTimestampSetAggregator.java) for the BoundedTimestampSet object. Gaffer will automatically aggregate these sets together to maintain a set of all the times the element was active. Once the size of the set becomes larger than 25 then a uniform random sample of size at most 25 of the timestamps is maintained.

${TYPES_JSON}

There are two edges in the graph. Edge A-B was added 3 times, and each time it had the 'boundedTimestampSet' property containing a randomly generated timestamp from 2017. Edge A-C was added 1000 times, and each time it also had the 'boundedTimestampSet' property containing a randomly generated timestamp from 2017. Here are the edges:

```
${GET_ALL_EDGES_RESULT}
```

You can see that edge A-B has the full list of timestamps on the edge, but edge A-C has a sample of the timestamps.
