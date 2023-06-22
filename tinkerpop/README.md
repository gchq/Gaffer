Copyright 2016-2023 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


# GafferPop

GafferPop is a lightweight Gaffer implementation of TinkerPop, where TinkerPop methods are delegated to Gaffer graph operations.

It is still in development. The implementation is basic and its performance is unknown in comparison to using Gaffer OperationChains.


## Setup
You must provide a GafferPop configuration file containing a path to a Gaffer store.properties file and comma separated list of of paths for Gaffer schema files, e.g:
```
gremlin.graph=uk.gov.gchq.gaffer.gaffer.gafferpop.GafferPopGraph
gaffer.storeproperties=conf/gaffer/map-store.properties
gaffer.schemas=conf/gaffer/schema/
```

### Local example
To run a demo local GafferPop graph, first download gremlin console: `apache-tinkerpop-gremlin-console-3.6.4.zip`

To get going with the tinkerpop-modern dataset backed by a MapStore you can do the following:
```bash
# Build the code
mvn clean install -Pquick -pl :tinkerpop -am

gremlinConsolePath=[gremlin-console-path]

# Create the necessary directories in the gremlin console folder
mkdir -p $gremlinConsolePath/ext/gafferpop/plugin
mkdir -p $gremlinConsolePath/conf/gafferpop

# Copy the required files into the gremlin console folder
cp -R tinkerpop/src/test/resources/* $gremlinConsolePath/conf/gafferpop
cp -R tinkerpop/target/tinkerpop-*.jar tinkerpop/target/gafferpop-*.jar  $gremlinConsolePath/ext/gafferpop/plugin

# Change from MiniAccumuloStore to MapStore properties
sed -i 's/store.properties/map-store.properties/g' $gremlinConsolePath/conf/gafferpop/gafferpop-tinkerpop-modern.properties

# Start gremlin
cd $gremlinConsolePath
./bin/gremlin.sh

# Activate the GafferPop plugin
:plugin use gafferpop
```


Within the gremlin console, load the tinkerpop modern data set:
```
graph = GraphFactory.open('conf/gafferpop/gafferpop-tinkerpop-modern.properties')
graph.io(graphml()).readGraph('data/tinkerpop-modern.xml')
g = graph.traversal()
```
do some queries:
```
g.V('1').hasLabel('person')
g.V('1', '2').hasLabel('person').outE('knows').values().is(lt(1))
```
calculate the paths from 1 to 3 (max 6 loops):
```
start = '1';
end = '3';
g.V(start).repeat(bothE().otherV().simplePath()).until(hasId(end).or().loops().is(6)).path()
```

## Gaffer mapping to TinkerPop terms

 - Group -> Label
 - Vertex -> Vertex with label 'id'
 - Entity -> Vertex
 - Edge -> Edge
 - Edge ID -> [dest, source]


## Limitations

There are several restrictions with this implementation. The following TinkerPop features are not yet implemented in GafferPop:
 - property index for unseeded queries (not yet implemented)
 - use of TraversalStrategy to speed up queries (not yet implemented)
 - Removal of entities (Gaffer cannot do this)
 - Updating properties (Gaffer cannot do this)
 - Undirected edges (needs to be verified)
 - Entity group 'id' is reserved for an empty group containing only the vertex id (needs to be verified)
 - When you get the in or out Vertex directly off an Edge it will not contain any actual properties - it just returns the ID vertex. This is due to Gaffer allowing multiple entities to be associated with the source and destination vertices of an Edge. (needs to be verified)
