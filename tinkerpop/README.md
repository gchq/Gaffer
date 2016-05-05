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


GafferPop
==================================

GafferPop is a lightweight Gaffer implementation of TinkerPop, where TinkerPop methods are delegated to Gaffer graph operations.

It is still experimental and should be used with caution.


Setup
------------------
Create a GafferPopGraph using GraphFactory.open(...)

You must provide a configuration file containing a path to a Gaffer store.properties file and comma separated list of of paths for Gaffer schema files, e.g:

    gremlin.graph=gaffer.gafferpop.GafferPopGraph
    gaffer.storeproperties=conf/gaffer/store.properties
    gaffer.schemas=conf/gaffer/schema/dataSchema.json,conf/gaffer/schema/dataTypes.json,conf/gaffer/schema/storeTypes.json

To use the gremlin console download 'apache-gremlin-console-3.2.0-incubating-bin.zip'

To get going with the tinkerpop-modern dataset backed by a MockAccumuloStore you can do the following:

    run: mvn clean install -Pgafferpop
    add the files in tinkerpop/src/test/resources to <gremlin-console>/conf/gafferpop
    add tinkerpop/target/tinkerpop-<version>.jar and tinkerpop/target/gafferpop-jar-with-dependencies.jar to <gremlin-console>/ext/gafferpop/plugin

open the gremlin shell:
    cd <gremlin-console>
    ./bin/gremlin.sh

active the GafferPop plugin:

    :plugin use gaffer.gafferpop.GafferPopGraph

load the data:

    graph = GraphFactory.open('conf/gafferpop/gafferpop-tinkerpop-modern.properties')
    graph.io(graphml()).readGraph('data/tinkerpop-modern.xml')
    g = graph.traversal(standard())

do some queries:

    g.V('1').hasLabel('person')
    g.V('1', '2').hasLabel('person').outE('knows').values().is(lt(1))

calculate the shortest path from 1 to 2 (max 6 loops):

    start = '1'; end = '3'; g.V(start).hasLabel('id').store('v').repeat(bothE().where(without('e')).store('e').inV().hasLabel('id').where(without('v'))).until{it.get().id() == end || it.loops() == 6}.path().filter{it.get().last().id() == end}

Gaffer mapping to TinkerPop terms
------------------
 - Group -> Label
 - Vertex -> Vertex with label 'id'
 - Entity -> Vertex
 - Edge -> Edge
 - Edge ID -> gaffer.gafferpop.EdgeId(sourceId, destinationId)


Limitations
------------------

There are several restrictions with this implementation. The following is not supported by GafferPop:
 - Removal
 - Updating properties
 - Fetching all Vertices or Edges - you must provide at least 1 seed. This means you cannot use Gremlin's export capability.
 - Undirected edges
 - Entity group 'id' is reserved for an empty group containing only the vertex id

Gaffer allows for graphs containing no entities. In order to traverse the graph in TinkerPop
the result of all vertex queries will also contain an empty Vertex labeled 'id' (even if no entities are found in Gaffer).

Gaffer allows multiple entities associated with each vertex. This means that in TinkerPop, edges can have multiple vertices at either end.