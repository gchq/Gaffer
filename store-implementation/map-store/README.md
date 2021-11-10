Copyright 2017-2020 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

The master copy of this page is the README in the map-store module.

Map Store
===================

The map store is a simple in-memory store. 
Any class that implements Java's Map interface can be used to store the data.
If using a simple map implementation, like HashMap, then data stored in this store is not persistent, i.e. when the JVM is shut down the data will disappear. It is designed to support aggregation of properties efficiently.  

Optionally, an index is maintained so that Elements can be found quickly from EntityIds or EdgeIds.  This option in the store properties (`gaffer.store.mapstore.createIndex`) is enabled by default.

The map store is not currently designed to be a very high-performance, scalable in-memory store. 
Future versions may include implementations that allow better scalability, for example by using off-heap storage. 
The current version stores the elements as objects in memory and so is not efficient in its memory usage.

Some examples of how this can be used are:

- To demonstrate some of Gaffer's APIs and functionality.
- As an in-memory cache of some data that has been retrieved from a larger store.
- For the aggregation of graph data within a streaming process.

It allows very quick calculation of the total number of elements in the graph subject to the default view.

Note that this store requires that the classes used for the vertices, and for all the group-by properties, have an implementation of the hashCode() method.

## Differences to Accumulo Store
There are a few key differences between the current implementations of the map store and Accumulo store.

### Non-static
Map stores are, by default, non-static. This means that when you create a new store with the same store properties as an existing map store, a new map is made and old data will not be accessible.  
This is unlike Accumulo where using the same store properties would link you to the same store.  
To make map stores work like Accumulo stores in this regard, then you must set the `gaffer.store.mapstore.static` to `true` in the store properties.

### Lack of store validation
Map stores do not have the `STORE_VALIDATION` trait, meaning they do not implement store validation like Accumulo stores do.  
What this means is that Elements will not be validated continuously and removed if they are found to be invalid based on the Schema.   

## Configuration
To configure your choice of Map implementation you can either:

- Set this "gaffer.store.mapstore.map.class" store property to be your chosen Map implementation class. This will require that java class to be on the classpath.

or

- For more complex Map implementation that require additional configuration and tuning you can use the "gaffer.store.mapstore.map.factory" map factory store property.
This allows you to implement your own map factory to use different Map implementations like Hazelcast and MapDB. 
However due to the nature of having to query-update-put in order to add a new element other implementations may be slow. 
In addition you can provide you map factory with configuration using the "gaffer.store.mapstore.map.factory.config" store property.

This map store does not attempt to handle concurrent adding of elements. Elements should be added from a single thread.

