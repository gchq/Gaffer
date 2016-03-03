  Copyright 2016 Crown Copyright

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

<img src="logos/logoWithText.png" width="300">

Gaffer
======

Gaffer is a framework for building systems that can store and process graphs. It is designed to be as flexible, scalable and extensible as possible,
allowing for rapid prototyping and transition to production systems or for enterprises where multiple graphs need to work together within a common framework.

Gaffer is different from other graph frameworks because:

 - Its data model is very general. It should be possible to store simple graphs where the nodes are just integers as well as complex, directed or undirected multigraphs,
 property graphs and so on. In fact, Gaffer does not even require there to be any edges so it could be used for machine learning applications where it is necessary to
 keep feature vectors describing a set of items up to date.
 - Nodes and edges in the store can have statistics such as counts, histograms and sketches that represent their properties.
Gaffer provides a pluggable way of using any suitable libraries to serialise, summarise, filter and transform these statistics.
 - Application developers can turn any database into a Gaffer store by implementing a small number of simple methods. This means that they can easily tailor the store to their particular needs by choosing a suitable underlying database.
 - A Gaffer graph is managed using simple schemas. The data schema describes the structure of the nodes and edges and the libraries used to summarise, filter,
 transform and validate them. The store schema describes how nodes and edges are serialised and mapped into the store. These schemas enable new node or edge types to be
 easily added to the graph and make it extremely simple to update the libraries used for managing existing data. Separation of the schemas also allows the same data
 to be easily migrated or stored and queried across multiple different stores.
 - Query and processing of data in the Gaffer store is done using Gaffer Operations. These allow the user to specify flexible views on the data. For example, we may
 have a graph containing a range of edge types such as red, green and blue. At query time we can choose to view only red edges within a certain time window.

Gaffer On Accumulo
------------------

Gaffer's Accumulo Store is optimised for:

 - Ingesting large amounts of data efficiently. For example, if the edges in a graph have a count statistic, then when there is a new
observation of an edge, the edge can simply be inserted into the graph with a count of 1. If this
edge already exists in the graph then this count of 1 will be added onto the existing edge. The ability to
do these updates without the need for query-update-put is key to the ability to ingest large volumes
of data.
 - Dealing with data at different security levels - nodes and edges could have a visibility property, and this is used to restrict who can
see data based on their authorizations.
 - Summarising data within the store itself using Accumulo's iterator stack for efficient server-side merging of properties and for query time filtering of results.
Properties are stored separately
for different time windows, e.g. we may choose to store daily summaries of properties. This allows age-off
of old properties, or of edges that have not been seen for a given period. It also allows the user
to specify a time period of interest at query-time, and Gaffer will aggregate the properties over
that time window before returning the results to the user.

Relationship to Gaffer1
-----------------------

Gaffer1 was a large scale graph database built on Accumulo that allowed the properties stored on edges and entities in the graph to be efficiently maintained on ingest
and dynamically summarised at query time inside the store itself. While it performed extremely well, Gaffer1 had a few drawbacks. For example, the schema for
representing graph elements was fixed, the serialisation of the elements and the functions used to manage their properties were not easily extended and the implementation
was quite tightly coupled to Accumulo.

One of the guiding principles during development of Gaffer2 has been that it should be possible to use it to completely reproduce the functionality and performance of Gaffer1.
As a result, Gaffer1 now becomes a specific configuration of Gaffer, namely the 'type-value' graph where the Store is Accumulo.

The name Gaffer now generally refers to Gaffer2. Gaffer1 and Gaffer2 will only be explicitly referenced where a clear differentiation is needed.

Status
------

Gaffer is still under active development. As a result, it shouldn't be considered a finished product. There is still performance testing and bug fixing to do, new features
to be added and additional documentation to write. Please contribute.

Building and Deploying
----------------------

To build Gaffer run `mvn clean package` in the top-level directory. This will build all of Gaffer's core libraries, the Accumulo store and some examples of how to load and query data and write other stores.

The Accumulo store needs to run on a Hadoop cluster with Accumulo installed. After building the project, the following jars need to be installed on all of Accumulo's tablet servers' classpaths.

 - The Accumulo iterators jar, located at `accumulo-store/target/accumulo-store-iterators-X.jar`
 - The jar containing the functions used for managing the graph data in Accumulo, located at `simple-function-library/target/simple-function-library-X.jar`
 - The jar containing the serialisers for the data, located at `simple-serialisation-library/target/simple-serialisation-library-X.jar`
 - Any jars that contain custom functions or serialisers you want to use to serialise or manage the data in Accumulo.

Adding files to Accumulo's tablet server classpaths is typically done by putting the jar in the lib directory within Accumulo's directory.

[Javadoc] (http://governmentcommunicationsheadquarters.github.io/Gaffer/)

We are working on a more detailed user guide.
