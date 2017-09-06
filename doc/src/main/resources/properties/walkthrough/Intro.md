## Introduction 

Gaffer allows properties to be stored on Entities and Edges. As well as simple properties, such as a String or Integer, Gaffer allows rich properties such as sketches and sets of timestamps to be stored on Elements. Gaffer's ability to continuously aggregate properties on elements allows interesting, dynamic data structures to be stored within the graph. Examples include storing a HyperLogLog sketch on an Entity to give a very quick estimate of the degree of a node or storing a uniform random sample of the timestamps that an edge was seen active.

Gaffer allows any Java object to be used as a property. If the property is not natively supported by Gaffer, then you will need to provide a serialiser, and possibly an aggregator.

The properties that Gaffer natively supports can be divided into three categories:

- Standard simple Java properties.
- Sketches from the clearspring and datasketches libraries.
- Sets of timestamps.

This documentation gives some examples of how to use all of the above types of property.
