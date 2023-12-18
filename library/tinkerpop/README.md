# GafferPop

GafferPop is a lightweight Gaffer implementation of the
[TinkerPop framework](https://tinkerpop.apache.org/), where TinkerPop methods
are delegated to Gaffer graph operations.

It is still in development. The implementation is basic and its performance is
unknown in comparison to using standard Gaffer `OperationChains`.

## Setup

Please see the [official Gaffer documentation](https://gchq.github.io/gaffer-doc/latest/)
on how to set up a Gremlin console connection using GafferPop.

## Basic Queries

Via the Gremlin console, you can load a standard TinkerPop dataset like below:

```groovy
graph = GraphFactory.open('conf/gafferpop/gafferpop-tinkerpop-modern.properties')
graph.io(graphml()).readGraph('data/tinkerpop-modern.xml')
g = graph.traversal()
```

Some basic queries can be carried out on the data:

```groovy
g.V('1').hasLabel('person')
g.V('1', '2').hasLabel('person').outE('knows').values().is(lt(1))
```

The following example calculates the paths from 1 to 3 (max 6 loops):

```groovy
start = '1';
end = '3';
g.V(start).repeat(bothE().otherV().simplePath()).until(hasId(end).or().loops().is(6)).path()
```

## Mapping Gaffer to TinkerPop

Some of the terminology is slightly different between TinkerPop and Gaffer,
a table of how different parts are mapped is as follows:

| Gaffer | TinkerPop |
| --- | --- |
| Group | Label |
| Vertex | Vertex with default label of `id` |
| Entity | Vertex |
| Edge | Edge |
| Edge ID | A list with the source and destination of the Edge e.g. `[dest, source]` |

## Limitations

There are several restrictions with the current implementation. Many of these
can be attributed to the fundamental differences between the two technologies
but some features may also be yet to be implemented.

Current TinkerPop features not present in the GafferPop implementation:

- Property index for allowing unseeded queries.
- Using a Gaffer user for authentication.
- Removal of entities/edges or properties - base Gaffer currently does not
  support this.
- TinkerPop Graph Computer is not supported.
- Graph Variables can't be updated - there is limited support for this in base
  Gaffer as usually requires shutting down and restarting the Graph.
- TinkerPop Transactions are not supported.

Current known limitations or bugs:

- The entity group `id` is reserved for an empty group containing only the
  vertex ID, this is currently used as a workaround for other limitations.
- When you get the in or out Vertex directly off an Edge it will not contain any
  actual properties or be in correct group/label - it just returns a vertex in
  the `id` group. This is due to Gaffer allowing multiple entities to be
  associated with the source and destination vertices of an Edge.
- Limited support for updating properties, this has partial support via Gaffers
  ingest aggregation mechanism.
- Performance compared to standard Gaffer OperationChains is hampered due to
  a custom `TraversalStratergy` not being implemented.
- The ID of an Edge follows a specific format that is made up of its source and
  destination IDs like `[dest, source]`.
- Issues seen using `hasKey()` and `hasValue()` in same query.
- May experience issues using the `range()` query function.
- May experience issues using the `where()` query function.

## Copyright

```text
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
```
