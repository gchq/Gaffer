Gaffer
======

Introduction
------------

Gaffer is a framework that makes it easy to store large-scale graphs in which the nodes and edges have
statistics such as counts, histograms and sketches. These statistics summarise the properties of the
   nodes and edges over time windows, and they can be dynamically updated over time.

Gaffer is a graph database, rather than a graph processing system. It is optimised for retrieving
data on nodes of interest.

Gaffer is distinguished from other graph storage systems by its ability to update properties within
the store itself. For example, if the edges in a graph have a count statistic, then when there is a new
observation of an edge, the edge can simply be inserted into the graph with a count of 1. If this
edge already exists in the graph then this count of 1 will be added onto the existing edge. The ability to
do these updates without the need for query-update-put is key to the ability to ingest large volumes
 of data. Many types of statistics are available, including maps, sets, histograms, hyperloglog sketches
 and bitmaps used to store timestamps.

To avoid the problem of the graph continually growing without limit, the properties are stored separately
for different time windows, e.g. we may choose to store daily summaries of properties. This allows age-off
of old properties, or of edges that have not been seen for a given period. It also allows the user
to specify a time period of interest at query-time, and Gaffer will aggregate the properties over
that time window before returning the results to the user.

Gaffer allows the user to specify flexible views on the data. For example, we may have a graph
containing a range of edge types, e.g. red, blue and green, and at query time we may choose to
view only red edges within a certain time window.

Gaffer does not require there to be any edges, and therefore it can be used for machine learning
applications where it is necessary to keep feature vectors describing a set of items up-to-date.

Gaffer uses Accumulo for storing data, although other stores are possible within the framework. It
exploits the power of Accumulo's iterator stack for efficient server-side merging of properties, and
for query-time filtering of results. It is designed to be easy to use without needing any knowledge
 of Accumulo.

Gaffer was designed to meet the following requirements:

- Allow the creation of graphs with summarised properties within Accumulo with a very minimal amount of coding.
- Allow flexibility of statistics that describe the entities and edges.
- Allow easy addition of new types of nodes and edges.
- Allow quick retrieval of data on nodes of interest.
- Deal with data of different security levels - all data has a visibility, and this is used to restrict who can
see data based on their authorizations.
- Support automatic age-off of data.

For more details of how to use Gaffer, see the User Guide.

Building
--------

Gaffer is built using maven: after cloning the source run `mvn clean package`. This will produce 3 jars
in the target directory: one contains just Gaffer, one contains Gaffer and its dependencies, one is to
be placed on Accumulo's tablet servers' classpath. For more details see the User Guide.

License
-------

Gaffer is licensed under the Apache 2 license - see the LICENSE file for more information.

Gaffer2
-------

The version of Gaffer in this repo is no longer under active development because a project called
Gaffer2 is in development. This aims to create a more general framework that offers the best of Gaffer
but improves it in the following areas:

- Gaffer has a fixed idea of what a node and edge is - for example, a node is identified by a type and value and each edge
has a start and end date. This means that it is not possible to store more general graphs in Gaffer.
- It has a built-in library of statistics. These statistics are not tied to Accumulo - they can be used
for example in MapReduce jobs or streaming summarisation jobs. It is possible for developers to create their
own statistics, but it is not possible to use new libraries of properties, or to configure the serialisation
of properties.
- Nothing in Gaffer limits the implementation options to Accumulo, but it does not contain any implementations
other than Accumulo.
- Gaffer has no schema. This allows a great deal of flexibility in the addition of new statistics, and new edge
types, but it also imposes some limitations, e.g. on the efficiency of the serialisation.

Gaffer2 is a project that aims to take the best parts of Gaffer, and resolve some of the above flaws, to create a
more general purpose graph database system. This can be used for both large and small scale graphs,
for graphs with properties that are summaries, or just static properties, and for many other use cases. Note
that Gaffer2 is a complete rewrite - the API is very different to this version of Gaffer.

Gaffer2 will be released shortly. We chose to release version 1 so that the community could see where version 2
originated from, and so that we can perform testing to avoid significant performance regressions in version 2. We
advise interested parties to wait for version 2 rather than investing much time in learning to use version 1 or
in working on pull requests.

We are currently working on a contributor license agreement which will need to be signed before we can accept
pull requests to Gaffer2.
