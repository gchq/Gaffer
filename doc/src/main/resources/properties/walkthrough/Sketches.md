## Sketches

A sketch is a compact data structure that gives an approximate answer to a question. For example, a HyperLogLog sketch can estimate the cardinality of a set with billions of elements with a small relative error, using orders of magnitude less storage than storing the full set.

Gaffer allows sketches to be stored on Entities and Edges. These sketches can be continually updated as new data arrives. Here are some example applications of sketches in Gaffer:

- Using a HyperLogLogPlusPlus sketch to provide a very quick estimate of the degree of a node.
- Using a quantiles sketch to estimate the median score associated to an edge, or the 99th percentile of the scores seen on an edge.
- Using a reservoir items sketch to store a sample of all the distinct labels associated to an edge.
- Using theta sketches to estimate the number of distinct edges seen on a particular day, the number seen on the previous day and the overlap between the two days.

Gaffer provides serialisers and aggregators for sketches from two different libraries: the [Clearspring](https://github.com/addthis/stream-lib) library and the [Datasketches](https://datasketches.github.io/) library.

For the Clearspring library, a serialiser and an aggregator is provided for the `HyperLogLogPlus` sketch. This is an implementation of the HyperLogLog++ algorithm described [in this paper](http://static.googleusercontent.com/external_content/untrusted_dlcp/research.google.com/en/us/pubs/archive/40671.pdf).

For the Datasketches library, serialisers and aggregators are provided for several sketches. These include:

- A HyperLogLog sketch for estimating the cardinality of a set;
- frequencies.LongsSketch and frequencies.StringsSketch - these are used for estimating the frequencies of longs and strings respectively;
- quantiles.DoublesSketch and quantiles.StringsSketch - these are used for estimating the quantiles of doubles or strings respectively;
- sampling.ReservoirLongsSketch - used for maintaining samples of longs seen on an element;
- theta sketches - used for estimating the union and intersection of sets.

Most of the Datasketches sketches come in two forms: a standard sketch form and a "union" form. The latter is technically not a sketch. It is an operator that allows efficient union operations of two sketches. It also allows updating the sketch with individual items. In order to obtain estimates from it, it is necessary to first obtain a sketch from it, using a method called `getResult()`. There are some interesting trade-offs in the serialisation and aggregation speeds between the sketches and the unions. If in doubt, use the standard sketches. Examples are provided for the standard sketches, but not for the unions.
