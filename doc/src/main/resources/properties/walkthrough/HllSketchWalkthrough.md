${HEADER}

${CODE_LINK}

This example demonstrates how the [HllSketch](https://github.com/DataSketches/sketches-core/blob/master/src/main/java/com/yahoo/sketches/hll/HllSketch.java) sketch from the Data Sketches library can be used to maintain an estimate of the degree of a vertex. Every time an edge A -> B is added to graph, we also add an Entity for A with a property of a HllSketch containing B, and an Entity for B with a property of a HllSketch containing A. The aggregator for the HllSketches merges them together so that after querying for the Entity for vertex X the HllSketch property would give us an estimate of the approximate degree.

##### Elements schema
This is our new elements schema. The edge has a property called 'approx_cardinality'. This will store the HllSketch object.

${ELEMENTS_JSON}

##### Types schema
We have added a new type - 'hllsketch'. This is a [com.yahoo.sketches.hll.HllSketch](https://github.com/DataSketches/sketches-core/blob/master/src/main/java/com/yahoo/sketches/hll/HllSketch.java) object.
We also added in the [serialiser](https://github.com/gchq/Gaffer/blob/master/library/sketches-library/src/main/java/uk/gov/gchq/gaffer/sketches/datasketches/cardinality/serialisation/HllSketchSerialiser.java) and [aggregator](https://github.com/gchq/Gaffer/blob/develop/library/sketches-library/src/main/java/uk/gov/gchq/gaffer/sketches/datasketches/cardinality/binaryoperator/HllSketchAggregator.java) for the HllSketch object. Gaffer will automatically aggregate these sketches, using the provided aggregator, so they will keep up to date as new entities are added to the graph.

${TYPES_JSON}

Only one entity is in the graph. This was added 1000 times, and each time it had the 'approxCardinality' property containing a vertex that A had been seen in an Edge with. Here is the Entity:

```
${GET_ALL_ENTITIES_RESULT}
```

This is not very illuminating as this just shows the default `toString()` method on the sketch.

We can fetch the cardinality for the vertex using the following code:
${GET_THE_APPROXIMATE_DEGREE_OF_A_SNIPPET}

The results are as follows. As an Entity was added 1000 times, each time with a different vertex, then we would expect the degree to be approximately 1000.

```
${GET_APPROX_DEGREE_FOR_ENTITY_A}
```