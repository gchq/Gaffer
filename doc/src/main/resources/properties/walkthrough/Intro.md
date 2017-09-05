## Introduction 

Gaffer allows properties to be stored on Entities and Edges. As well as simple properties, such as a String or Integer, Gaffer allows rich properties such as sketches and sets of timestamps to be stored on Elements. Gaffer's ability to continuously aggregate properties on elements allows interesting, dynamic data structures to be stored within the graph. Examples include storing a HyperLogLog sketch on an Entity to give very quick estimates of the degree of a node, storing a uniform random sample of the timestamps that an edge was seen active.

Gaffer allows any Java object to be used as a property. If the property is not natively supported by Gaffer, then you will need to provide a serialiser, and possibly an aggregator.

The properties that Gaffer natively supports can be divided into three categories:

- Standard simple Java properties.
- Sketches from the clearspring and datasketches library.
- The RoaringBitmap.
- Sets of timestamps.

This documentation gives some examples of how to use all of the above types of property.

## Running the Examples

The example can be run in a similar way to the user and developer examples. 

You can download the doc-jar-with-dependencies.jar from [maven central](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22uk.gov.gchq.gaffer%22%20AND%20a%3A%22doc%22). Select the latest version and download the jar-with-dependencies.jar file.
Alternatively you can compile the code yourself by running a "mvn clean install -Pquick". The doc-jar-with-dependencies.jar file will be located here: doc/target/doc-jar-with-dependencies.jar.

```bash
# Replace <DoublesUnion> with your example name.
java -cp doc-jar-with-dependencies.jar uk.gov.gchq.gaffer.doc.properties.dev.walkthrough.DoublesUnion
```