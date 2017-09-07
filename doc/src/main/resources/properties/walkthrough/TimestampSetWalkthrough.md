${HEADER}

${CODE_LINK}

This example demonstrates how the [TimestampSet](https://github.com/gchq/Gaffer/blob/master/library/time-library/src/main/java/uk/gov/gchq/gaffer/time/TimestampSet.java) property can be used to maintain a set of the timestamps at which an element was seen active. In this example we record the timestamps to minute level accuracy, i.e. the seconds are ignored.

##### Elements schema
This is our new elements schema. The edge has a property called 'timestampSet'. This will store the [TimestampSet](https://github.com/gchq/Gaffer/blob/master/library/time-library/src/main/java/uk/gov/gchq/gaffer/time/TimestampSet.java) object, which is actually a [RBMBackedTimestampSet](https://github.com/gchq/Gaffer/blob/master/library/time-library/src/main/java/uk/gov/gchq/gaffer/time/RBMBackedTimestampSet.java).

${ELEMENTS_JSON}

##### Types schema
We have added a new type - 'timestamp.set'. This is a [uk.gov.gchq.gaffer.time.RBMBackedTimestampSet](https://github.com/gchq/Gaffer/blob/master/library/time-library/src/main/java/uk/gov/gchq/gaffer/time/RBMBackedTimestampSet.java) object.
We also added in the [serialiser](https://github.com/gchq/Gaffer/blob/master/library/time-library/src/main/java/uk/gov/gchq/gaffer/time/serialisation/RBMBackedTimestampSetSerialiser.java) and [aggregator](https://github.com/gchq/Gaffer/blob/master/library/time-library/src/main/java/uk/gov/gchq/gaffer/time/binaryoperator/RBMBackedTimestampSetAggregator.java) for the RBMBackedTimestampSet object. Gaffer will automatically aggregate these sets together to maintain a set of all the times the element was active.

${TYPES_JSON}

Only one edge is in the graph. This was added 25 times, and each time it had the 'timestampSet' property containing a randomly generated timestamp from 2017. Here is the Edge:

```
${GET_ALL_EDGES_RESULT}
```

You can see the list of timestamps on the edge. We can also get just the earliest, latest and total number of timestamps using methods on the TimestampSet object to get the following results:

```
${GET_FIRST_SEEN_LAST_SEEN_AND_NUMBER_OF_TIMES_FOR_EDGE_A_B}
```
