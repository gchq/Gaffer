${HEADER}

${CODE_LINK}

This example demonstrates how the [DoublesSketch](https://github.com/DataSketches/sketches-core/blob/master/src/main/java/com/yahoo/sketches/quantiles/DoublesSketch.java) sketch from the Data Sketches library can be used to maintain estimates of the quantiles of a distribution of doubles. Suppose that every time an edge is observed, there is a double value associated with it, for example a value between 0 and 1 giving the score of the edge. Instead of storing a property that contains all the doubles observed, we can store a DoublesSketch which will allow us to estimate the median double, the 99th percentile, etc.

##### Elements schema
This is our new elements schema. The edge has a property called 'doublesSketch'. This will store the DoublesSketch object.
${ELEMENTS_JSON}

##### Types schema
We have added a new type - 'doubles.sketch'. This is a [com.yahoo.sketches.quantiles.DoublesSketch](https://github.com/DataSketches/sketches-core/blob/master/src/main/java/com/yahoo/sketches/quantiles/DoublesSketch.java) object.
We also added in the [serialiser](https://github.com/gchq/Gaffer/blob/master/library/sketches-library/src/main/java/uk/gov/gchq/gaffer/sketches/datasketches/quantiles/serialisation/DoublesSketchSerialiser.java) and [aggregator](https://github.com/gchq/Gaffer/blob/master/library/sketches-library/src/main/java/uk/gov/gchq/gaffer/sketches/datasketches/quantiles/binaryoperator/DoublesSketchAggregator.java) for the DoublesSketch object. Gaffer will automatically aggregate these sketches, using the provided aggregator, so they will keep up to date as new edges are added to the graph.

${TYPES_JSON}

```
${GET_ALL_EDGES_RESULT}
```

This is not very illuminating as this just shows the default `toString()` method on the sketch. To get value from it we need to call methods on the DoublesSketch object. We can get an estimate for the 25th, 50th and 75th percentiles on edge A-B using the following code:
${GET_0.25_0.5_0.75_PERCENTILES_FOR_EDGE_A_B_SNIPPET}

The results are as follows. This means that 25% of all the doubles on edge A-B had value less than -0.66, 50% had value less than -0.01 and 75% had value less than 0.64 (the results of the estimation are not deterministic so there may be small differences between the values below and those just quoted).

```
${GET_0.25,0.5,0.75_PERCENTILES_FOR_EDGE_A_B}
```

We can also get the cumulative density predicate of the distribution of the doubles:
${GET_CDF_SNIPPET}

The results are:

```
${GET_CDF_FOR_EDGE_A_B_RESULT}
```