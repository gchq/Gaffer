/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

Gaffer User Guide
=================

This document describes Gaffer version 1.3.0.

Contents:

- Overview
- Use cases
- Building the Gaffer software
- Requirements
- Entities, Edges and Statistics
- Streaming, MapReduce and Accumulo
- A simple example
- Inserting data into Accumulo
- The Gaffer shell
- Extracting data from the command line
- Queries and the AccumuloBackedGraph
- Filtering and aggregation within Accumulo
- Large-scale analytics
- Design considerations
- Related projects

Overview
--------

Given a set of data, many data mining, machine learning and graph algorithms require transforming that data into
entities, or relationships between entities, that are described by a feature vector. The feature vector summarises
the properties of the entity or relationship. These feature vectors typically contain simple information such as
counts or histograms, but can contain more complex information like timeseries or hyperloglog sketches.

In operational systems, these summaries need to be updated over time. Old parts of the summary need to be aged off,
and existing summaries need to be updated. The user needs to be able to view summaries over time windows they care
about. For example, we may have daily summaries of the nodes and edges in a graph, but the user may choose to focus
on a particular week of interest. It is then desirable for the system to give the user a view of the
summaries aggregated over that week.

Some entities or relationships will have multiple types of data associated to them. For example, there may be
multiple types of edges between two nodes, and these edges may have different properties. It needs to be possible to
store separate summaries with a node or edge, and the user should be able to choose which of the summaries they view.
This means we can have rich data sets with a range of properties, and allow users to decide at query-time which
of the properties they view.

The summaries need to be stored by visibility level, e.g. there may be properties of an entity
derived from 'unsensitive' data, and also properties derived from 'sensitive' data. Some users will be able to
see all the data, others only the unsensitive data. When a user can see all of it, it should be merged across
the different visibilities to give the overall summary of the entity.

Gaffer is designed to achieve all of the above. It allows the storage and updating of rich, large-scale
graph data sets, which include detailed features and temporal information. Summaries are automatically updated
as new observations of the same nodes and edges are inserted. This updating happens within Accumulo, and avoids
the need for the user to retrieve the properties, update them and then insert them back into Accumulo. This allows
updating the properties at high data rates.

Gaffer stores data in Accumulo, but inserting data and retrieving it again requires the user to have no knowledge
of Accumulo. As Gaffer stores data in Accumulo, it is horizontally scalable so that very large data sets can be
dealt with. It has an API that allows users to retrieve the data they care about, filtered according to their
requirements and aggregated over the time window of interest. It supports bulk update and continuous update.

Use cases
---------

In this section we list some general use cases for Gaffer:

###Graphs###
Maintain a large-scale graph with rich features describing the nodes and edges, with detailed temporal information.

###Machine learning###
Produce feature vectors describing entities. Keep these feature vectors updated in near real-time. Query for the
feature vector for an entity of interest and use it to produce a classification based on a model. Find out how
that classification would vary based on the feature vector for different time windows.

###Time-series###
Maintain a dynamically updated data set of entities and the volume of their activity over hourly time windows.

Building the Gaffer software
----------------------------

To build the software run `mvn clean package`. This should create 3 jars in the target directory, e.g.
gaffer-1.3.0-full-2.6.0-accumulo-1.6.4.jar, gaffer-1.3.0-iterators-2.6.0-accumulo-1.6.4.jar,
gaffer-1.3.0.jar. The first of these should be used in place of gaffer.jar in the commands in the following sections.

Requirements
------------

Gaffer requires a Hadoop cluster with Accumulo 1.5 or later installed. The iterators jar should be installed on
all of Accumulo's tablet servers' classpaths. This is typically done by putting the jar in the lib directory within
Accumulo's directory.

Entities, Edges and Statistics
------------------------------

Gaffer is about the storage of feature vectors summarising the properties of either entities or the relationship between
entities. We want our data set to be able to contain multiple different types of summaries, to contain summaries that can
be viewed over different time windows, and to permit different users to see different sensitivities of summaries. Therefore
a summary of an entity or an edge needs to be defined over three dimensions: type, visibility and time window.

The fundamental concepts within Gaffer are the Entity, the Edge and the SetOfStatistics. We will describe each of these in turn.

A node in the graph is identified by a pair of strings - a type and a value.

###Entities###

An Entity is an object containing a node (i.e. a type and a value), with other values to form a key to which some
properties can be associated. The fields in an Entity are:

- the type of the node
- the value of the node
- the summary type
- the summary subtype
- the visibility of the entity
- the start and end times of the summary

The type and value are arbitrary strings. The unit tests for Gaffer use a running example of customers purchasing products
at stores in order to create Entities, Edges and statistics for testing. An example would be to have a type of "customer"
and a value of "John Smith".

The summary type and subtype are arbitrary strings that are used to specify the type of information that is being summarised.
For example, we might want to summarise all the purchases that John makes in store, so the summary type could be "purchases"
and the summary subtype could be "instore". In applications where multiple types of information are being stored about
the same node then specifying the type of summary helps keep different summary information separate. Gaffer makes no
restrictions on the types and subtypes that are allowed.

The visibility field determines who can see this Entity (and its associated statistics) - some special customers may have
their purchase information protected so that only store managers can see them.

The start and end times of the summary are arbitrary dates, with the restriction that the start date must not be after the
end date. It is common to use daily summaries, in which case the start time would be the midnight at the start of a day,
and the end date would be midnight at the end of that day, and this entity would refer to summarised information about the
customer's purchases that day.

In the section on querying of data, we will explain how Gaffer allows the summaries of a given node to automatically
be combined over different time periods and how this requires the time windows of the summary to be non-overlapping.

Note that it is typical to have several Entities for the same (type, value) pair, corresponding to different time periods,
different types of summary and different time windows. It is possible that the name Entity is slightly misleading - an
Entity is really a (type, value) pair with summary type / time window / visibility attributes. Within Gaffer an object
of class "TypeValue" is used to store a (type, value) pair.

###Edges###

An Edge is an object containing the following fields:

- the type and value of the first node
- the type and value of the second node
- the summary type and subtype
- whether the edge is directed or not
- the visibility of the entity
- the start and end times of the summary

There are two nodes in the edge. Each of these is identified by a type and a value.

The summary type and subtype are arbitrary strings that are used to specify the type of information that is being summarised.

An Edge can be either directed or undirected. If it is directed then the source is the first node and the destination is
the second node.

As with Entities, the start and end times of the summary are arbitrary dates.

Note that Gaffer does not require the creation of both Entities and Edges - it is possible to have a Gaffer instance that
consists solely of Entities, or solely of Edges or a mixture. Thus, it is wrong to think of Gaffer as purely a graph
framework.

When Gaffer does contain summaries of both entities and edges, it is typical for them to have matching summary types and
subtypes.

###Sets of statistics###

A set of statistics summarises the properties of an Entity or an Edge. This means that the information in the set of
statistics relates to information of the summary type and subtype, over the time window of interest, and is derived from
data that has the given visibility.

A set of statistics is really a sparse feature vector with named fields - a map from string to a Statistic. A statistic
is simply a value that can be merged with other values of the same type. The most obvious example is the Count statistic.
This contains an integer, and when two Counts are merged the counts are simply added. There is no requirement for two edges
to have the same types of statistics, or for there to be any statistics at all.

###Available statistics###

The following is a list of the statistics available in Gaffer, along with some examples of how they are used. At the end
of this list we describe user-defined statistics.

The most common statistics are the following:

- Count: This contains an integer. Two counts are merged by adding the integer. For example, used to count the number of
times an edge has been active within the summary period.
- FirstSeen: This contains a Date. When two are merged, the earliest date is taken. Used for recording the first time an
entity or edge is seen.
- LastSeen: This contains a Date. When two are merged, the latest date is taken. Used for recording the last time an entity
or edge is seen.
- MinuteBitMap: Stores a list of minutes, e.g. 12:34 on January 1st 2014. When two of these are merged, the union of the
two lists is formed. Typically used to record, to minute granularity, when an edge is active. Internally this is stored as
a RoaringBitMap of integers, which are the number of minutes since the epoch, which is a very efficient representation that
is much more compact than storing a list of dates or integers.
- MapOfCounts: A map from strings to integers. When two of these are merged, the maps are merged - if the two maps contain
the same key then the corresponding integers are added.

The following statistics are also available:

- CappedMinuteCount: This stores counts by minutes, e.g. a count of 10 for 9:01 on January 1st, 2015. When two of
these are merged, the counts for each minute are summed. As this could take a lot of storage, a maximum number of minutes
can be specified. When more minutes than this have positive counts, then the statistic is marked as being full, and the
counts are removed.
- CappedSetOfStrings: This contains a set of at most n strings, where n is specified by the user. Strings can be added to
the set. If the set contains n elements, and a new string is added, then the set will be cleared and a flag will be set
indicating that the set is full. Two CappedSetOfStrings can only be merged if they have the same maximum size n; if they do
then the union of the two sets is taken. If this results in a set containing more than n elements then it is marked as full.
- DailyCount: Stores count by day, e.g. a count of 20 for January 17th 2015 and a count of 30 for January 19th 2015. When
two of these are merged, the counts are summed.
- DoubleCount: This contains a double. When two are merged, they are added.
- DoubleProduct: This contains a double. When two are merged, the product is taken.
- DoubleProductViaLogs: This contains a double, but internally the logarithm of the double is stored. When two are merged,
the sum of the logs is formed. Useful for combining probabilities.
- HourlyBitMap: Contains hours, e.g. the hours starting at 2pm on January 1st, and 5pm on January 12th. When two are merged,
the union of the two lists of hours is taken.
- HourlyCount: Stores count by hour, e.g. a count of 5 for the hour 12:00-13:00 on January 1st 2015, and a count of 17 for
the hour 14:00-15:00 on January 2nd 2015. When two of these are merged, the counts are summed.
- HyperLogLogPlusPlus: Stores a sketch to estimate the cardinality of a set of objects. When two are merged, the sketches
are merged, so that the estimate derived from the merged statistic is a good estimate of the estimate of the merged set.
A common use-case is to store an estimate of the number of neighbours of a node.
- IntArray: Stores an array of integers. When two are merged, the values in corresponding positions are summed. Two arrays
of different lengths cannot be merged. This is typically used to record the number of times an edge was active in the 24
different hours of the day.
- IntMax: Stores an integer. When two are merged the maximum is taken.
- IntMin: Stores an integer. When two are merged the minimum is taken.
- IntSet: Stores a list of integers. When two of these are merged, the two lists are merged and deduped.
- LongCount: This contains a long. They are merged by adding. For example, used to count the number of times an edge has
been active within the summary period.
- LongMax: Stores a long. When two are merged the maximum is taken.
- LongMin: Stores a long. When two are merged the minimum is taken.
- MapFromDoubleToCount: Stores a map from double to integer counts. When two are merged, the maps are merged - if the two
maps contain the same key then the two counts are summed.
- MapFromIntToCount: Stores a map from integers to integer counts. When two are merged, the maps are merged - if the two
maps contain the same key then the two counts are summed.
- MapFromIntToLongCount: Stores a map from integers to long counts. When two are merged, the maps are merged - if the
two maps contain the same key then the two counts are summed.
- MapFromStringToSetOfStrings: A map from strings to sets of strings. When two of these are merged, the maps are merged -
if the two maps contain the same key then the union of the corresponding sets is formed.
- MapOfLongCounts: A map from strings to longs. When two of these are merged, the maps are merged - if the two maps contain
the same key then the corresponding longs are added.
- MapOfMinuteBitMaps: A map from strings to MinuteBitMaps. When two of these are merged, the maps are merged - if the two
maps contain the same key then the corresponding MinuteBitMaps are merged.
- MaxDouble: Stores a double. When two are merged the maximum is taken.
- MinDouble: Stores a double. When two are merged the minimum is taken.
- SetOfStrings: This contains a set of strings. Strings can be added to the set. When two SetOfStrings are merged, the union
of the two sets is taken.
- ShortMax: Stores a short. When two are merged the maximum is taken.
- ShortMin: Stores a short. When two are merged the minimum is taken.

Developers can define their own statistics. These must implement Gaffer's Statistic interface, and
should provide a sensible no-args constructor. These statistics must be available to Accumulo's iterators - this means
that they must be on Accumulo's classpath on all the tablet servers. Note that in general it is preferable to use
Gaffer's own statistics, as these are serialised more efficiently.

Streaming, MapReduce and Accumulo
---------------------------------

The Entities, Edges and SetOfStatistics described above are simple Java objects. This means that they can be used in a range of
contexts. As we will see shortly, Gaffer makes it easy to add these objects to Accumulo and quickly extract them, subject to
desired filtering and aggregation rules. But these objects can also be used in MapReduce jobs (e.g. to produce a set of
feature vectors without having to create custom objects to store different types of features), or in streaming jobs
(where the statistics can be held in memory and automatically merged as new data arrives).

A simple example
----------------

The class gaffer.example.Example contains a very simple example of how to insert some data into a MockAccumulo instance,
and then execute some queries.

Inserting data into Accumulo
----------------------------

Once summaries have been generated, it is often necessary to be able to store them in such a way that they can be quickly
retrieved and analytics run against them. In Gaffer, Accumulo is used to store the data. Once the data is in Accumulo,
Entities and Edges and their associated statistics can be retrieved extremely quickly.

Gaffer does not require the user to have any knowledge of Accumulo in order to store data in it or to retrieve data from it.
Developers can think in terms of Entities, Edges and SetOfStatistics, without having to know anything about Accumulo's
underlying data structures. It makes it easy for developers to take advantage of the power of Accumulo without having to
have a detailed understanding of it.
 
There are two ways of inserting data into Accumulo: bulk import and continuous import. The former is recommended
for inserting large amounts of data; the latter when it is required that data is available for query soon after it
is generated.

###Table initialisation###

Before data can be inserted into Accumulo, it is necessary to create a table with the correct properties. Currently this
requires you to be able to estimate the distribution of keys in your data so that the servers in Accumulo can be loaded
roughly equally.

In order to estimate the split points it is necessary to have sequence files of (GraphElement, SetOfStatistic) pairs. A
GraphElement is a wrapper for either an Entity or Edge. Then run the following command:

	hadoop jar gaffer.jar gaffer.accumulo.splitpoints.EstimateSplitPointsDriver tmp_output 0.01 num_tablet_servers splitfile graphdata

where:

- gaffer.jar is the full gaffer jar (see the Building Gaffer section above)
- tmp_output is a directory in HDFS where some temporary output will be put
- 0.01 is the proportion of data to sample (1% is reasonable)
- num_tablet_servers is the number of tablet servers in your Accumulo cluster (this can be found by looking at the master
webpage)
- splitfile is the name of a file in HDFS where the split points will be written to
- graphdata is the name of the directory containing the sequence files of (GraphElement, SetOfStatistic) pairs. If you have
multiple directories containing data that you want to sample, simply list all these directories at the end of the command
line.

Now create a configuration file that consists of the following lines:

	accumulo.instance=instance_name_of_your_accumulo_installation
	accumulo.zookeepers=server1:2181,server2:2181,server3:2181
	accumulo.table=insert_a_table_name_of_your_choice
	accumulo.user=your_accumulo_user_name
	accumulo.password=your_accumulo_password
	accumulo.ageofftimeindays=30

The last line contains the age-off period - here any data older than 30 days will automatically be removed.

Then initialise the table using:

	hadoop jar gaffer.jar gaffer.accumulo.tablescripts.InitialiseTable accumulo_properties_file splitfile

where:

- accumulo_properties_file is the configuration file described above
- splitfile is the file of split points created by running the EstimateSplitPointsDriver as described above.

This creates the table, adds the appropriate iterators to ensure that data is rolled-up and aged off, and configures Bloom filters
for higher-performance queries.

Note that the above two steps are only required when the table is initialised - it is not required for future imports to this
table.

###Bulk import###

If you have written a MapReduce job that creates SequenceFiles containing (GraphElement, SetOfStatistic) pairs, then Gaffer
provides all the code necessary to add this data into Accumulo.

To convert the sequence files of (GraphElement, SetOfStatistic) pairs into files of key-value pairs ready for bulk import
into Accumulo, run:

	hadoop jar gaffer.jar gaffer.accumulo.bulkimport.BulkImportDriver graphdata graphdata_ready_for_accumulo accumulo_properties_file

where:

- graphdata is the HDFS directory containing the sequence files of (GraphElement, SetOfStatistic) pairs
- graphdata_ready_for_accumulo is a HDFS directory where the files of Accumulo key-value pairs will go
- accumulo_properties_file is the configuration file from above

What does this code do? It connects to Accumulo to check the current number of tablets in the table, and their split points,
so that it can use appropriate reducers to ensure the split of the data matches Accumulo's current configuration. This
minimises the load on Accumulo during the import and compaction process. It converts the Gaffer (GraphElement, SetOfStatistics)
pairs into Accumulo (key, value) pairs suitable for bulk import into Accumulo.

Finally this data needs to be given to Accumulo:

	hadoop jar gaffer.jar gaffer.accumulo.bulkimport.MoveIntoAccumulo graphdata_ready_for_accumulo/data_for_accumulo failure accumulo_properties_file

This generally takes a few seconds for the data to be imported into Accumulo and it is then available for query.

Here failure is a HDFS directory where any files that fail to import are moved to. Note that Accumulo needs to have rwx
permissions on the files in the graphdata_ready_for_accumulo directory.

NB: After this stage the Accumulo web page may report that the number of entries in the table has not increased. This is
because Accumulo does not know how much data was in the files until a major compaction has been performed. You can trigger
a major compaction manually from Accumulo's shell; you should then see the amount of data start to increase. You can also
check that your data has been added by running a scan from Accumulo's shell - if this returns no results then it is likely
that you do not have the appropriate authorizations to see the data you added. Ask the administrator of your Accumulo
cluster to give you the necessary authorizations.

###Continuous import###

This can be used to insert data from a file or from a stream.

The following code illustrates how data can be added to Accumulo from a simple Java class - this would typically be
used for testing purposes, but similar code could be called from a streaming framework to continually insert data. Here
`accumuloPropertiesFile` is a file that contains the configuration described above, and we assume that the table has
already been created.

	// Create config from configuration file, and use it to create a Connector to Accumulo
	AccumuloConfig accConf = new AccumuloConfig(accumuloPropertiesFile);
	Connector connector = Accumulo.connect(accConf);
	String tableName = accConf.getTable();

    // Create AccumuloBackedGraph
    AccumuloBackedGraph graph = new AccumuloBackedGraph(connector, tableName);

    // Create set of data
    Set<GraphElementWithStatistics> data = new HashSet<GraphElementWithStatistics>();

    // Populate this set - could be by reading from a file or a stream
    // ... convert your data into GraphElementWithStatistics objects
    graph.addGraphElementsWithStatistics(data);

###Age-off###

Ageing-off of old data happens automatically in Gaffer based on the age-off time specified in the initial
configuration file. An Entity or Edge, and its associated SetOfStatistics, are aged off as soon as the start time
becomes older than the specified age.

###Security###

The visibility field within the Entity or Edge is used to set the visibility of the Accumulo (key, value) pair, and this
means that a user with direct access to Accumulo will only see data they have authorization to see.

The Gaffer shell
----------------

The Gaffer shell is very basic. It allows interactive query of data from any server that can talk to Accumulo. The user
must have an Accumulo account and access to the table that the Gaffer data is stored in. The shell is started using:

	java -cp hadoop-common.jar:gaffer.jar gaffer.accumulo.Shell accumulo_properties_file

where the `accumulo_properties_file` contains the following information:

	accumulo.instance=your_instance_name
	accumulo.zookeepers=server1:2181,server2:2181,server3:2181
	accumulo.table=the_table_name
	accumulo.user=your_user_name
	accumulo.password=your_password

Once the shell is started, you can type in (type, value) pairs in the form type|value. All data involving that node
is then written to the screen, using the default toString() methods on the objects.

You can also set some different views on the data:

- Type `entity` to only see Entities
- Type `edge` to only see Edges
- Type `both` to see both Entities and Edges
- Type `roll` to toggle rolling up over time and visibility on and off

It is not currently possible to set the time window from within the shell.

Please note that the shell is very basic, and not intended for analysis of data.

Extracting data from the command line
-------------------------------------

Run the following command:

	hadoop jar gaffer.jar gaffer.analytic.impl.GetElements accumulo_properties_file seeds_file true both

Here:

- accumulo_properties_file is the standard properties file used above
- seeds_file contains one seed per line, with the type and value separated by a pipe ('|') character
- The third argument can be either 'true' or 'false' which determines whether elements are rolled up over time and visibility.
- The fourth argument can be 'entity', 'edge' or 'both' to indicate whether you want to receive only Entitys, only Edges or
both.

The results are printed to standard out and simply consist of string representations of the underlying objects. It is
possible to supply a customer formatter to format the results as you choose.

Queries and the AccumuloBackedGraph
-----------------------------------

In this section we discuss interacting with Gaffer data in Accumulo from Java code. The AccumuloBackedGraph class should
be used for all interactions with Gaffer data in Accumulo. It is created from an Accumulo Connector and the table name
(this is the name of the table that the data was inserted into, i.e. the accumulo.table field in the configuration file
used in the bulk import). The code that uses the AccumuloBackedGraph needs to run on a server which can contact all
the servers in the Accumulo cluster.

The AccumuloBackedGraph class provides an API for retrieving data subject to the desired view. The following code
illustrates a typical way of retrieving data from Gaffer:

	Set<TypeValue> seeds = new HashSet<TypeValue>();
	seeds.add(new TypeValue("customer", "A"));
	seeds.add(new TypeValue("product", "P"));
	CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatistics(seeds);
	for (GraphElementWithStatistics elementWithStats : retriever) {
		SetOfStatistics statistics = elementWithStats.getSetOfStatistics();
		if (elementWithStats.getGraphElement().isEntity()) {
			Entity entity = elementWithStats.getGraphElement().getEntity();
			// Do something with entity and statistics
		} else {
			Edge edge = elementWithStats.getGraphElement().getEdge();
			// Do something with edge and statistics
		}
	}
	retriever.close();

The main steps are: create a set of TypeValues of interest, create a retriever (which is an iterable of
GraphElementWithStatistics), iterate through the retriever doing something with the data that is returned,
then close the retriever.

Behind the scenes, this is batching the provided set of seeds into suitably-sized sets for querying Accumulo
(this uses a BatchScanner which is Accumulo's way of querying for multiple things in parallel). As the results are
returned as an Iterable, there is no requirement for them to fit in memory on the client-side - the client could,
for example, simply write them to disk.

Note that in general it is better to query for a set of 1000 TypeValues at once, rather than making 1000 separate
calls to a getGraphElements() method.

The type of data retrieved can be configured using options on AccumuloBackedGraph. The most common options are:

- `setSummaryTypes(Set<String> summaryTypes)`: Only Entities and Edges with a summary type in the provided set will be
returned.
- `setReturnEntitiesOnly()` or `setReturnEdgesOnly()`: This causes either only entities or only edges to be
returned. Note that it is much more efficient to specify these options rather than filter out edges yourself in the
loop through the results of the retriever. This is because Gaffer will perform this work for you on Accumulo's servers
rather than sending all the data back to the client.
- `setTimeWindow(Date startTimeWindow, Date endTimeWindow)`: Sets the time range of data that is required. Note that
for an Entity or Edge to be included in the results, its start date must be equal to or after `startTimeWindow` and
its end date must be equal to or before `endTimeWindow`.
- `rollUpOverTimeAndVisibility(boolean rollUp)`: Controls whether rolling up over time and visibility is on. If true is
passed then graph elements will be rolled up over time and visibility, e.g. if there is an edge A->B with summary type
X and subtype Y and start date of January 1st and end date of January 2nd of visibility public, and if there is also an
edge A->B with summary type X and subtype Y and start date of January 5th and end date of January 6th of visibility
private, then these will be rolled up into an edge A->B with start date of January 1st, end date of January 6th,
visibility of public&private, and with all of the associated statistics merged together. It is most common for this
option to be set to true (and this is the default).
- `setStatisticsToKeepByName(Set<String> statisticsToKeepByName)`: Sets the statistics that the user wants to see.

Other common options are:

- `setAuthorizations()`: Only Entities and Edges with visibilities within the set of Authorizations will be returned.
This allows a processing user to execute queries on behalf of a user with the authorizations of that user.
- `setSummaryTypePredicate(SummaryTypePredicate summaryTypePredicate)`: Only Entities and Edges whose summary type and
subtype match the provided predicate will be returned. A SummaryTypePredicate can be formed from the predicates in the
gaffer.predicate.summarytype.impl package.
- `setUndirectedEdgesOnly()` or `setDirectedEdgesOnly()`: Used to request that only undirected or only directed edges
are returned. Note that entities will still be returned - if only edges are required then `setReturnEdgesOnly()`
should be called as well.
- `setOutgoingEdgesOnly()` or `setIncomingEdgesOnly()`: The first of these specifies that only edges out from the
queried TypeValues are returned. The latter specifies that only edges into the queried TypeValues are
returned. As above, entities will still be returned. Thus if the user wants to only receive directed edges out from
the queried TypeValues, and no Entities, then `setReturnEdgesOnly()`, `setDirectedEdgesOnly()` and
`setOutgoingEdgesOnly()` should all be called.
- `setOtherEndOfEdgeHasType(Set<String> allowedTypes)`: Used to filter edges by the type at the non-query end, e.g.
can be used if we are querying for customer|A, but only want to receive edges to products.
- `setOtherEndOfEdgeValueMatches(Pattern pattern)`: Used to filter edges based on whether the value at the non-query
end matches a given regular expression.
- `setPostRollUpTransform(Transform transform)`: Used to specify a `Transform` that is applied to the data before it
is returned to the user. This is applied client-side (not on Accumulo's tablet servers) and thus there is no
requirement for the `Transform` to be on the classpath of the tablet servers.

For a full list of options, see the javadoc of `AccumuloBackedGraph`.

It is also possible to search for ranges of seeds using the method
`getGraphElementsWithStatisticsFromRanges(Iterable<TypeValueRange> typeValueRanges)`.

Note that there is a method called `getGraphElementsWithStatisticsWithinSet(Iterable<TypeValue> typeValues, boolean loadIntoMemory)`
which can be used to find all edges between members of a set. This queries for the type-values and passes a Bloom
filter to the filtering iterator to avoid returning edges for which one end is not in the set - this greatly reduces
the number of edges that are returned to the client. If the `loadIntoMemory` flag is set to true then all of the set
`typeValues` is loaded into memory (in the JVM that called the method) and this is used to secondary test the
results returned from Accumulo. With this option, it is guaranteed that no false positives are returned to the user.
If the `loadIntoMemory` flag is set to false then the set is queried for in batches, and a second, large Bloom filter
is used client-side to further reduce the chances of sending false positives back to the user. In this case, it is
not guaranteed that no false positives will be passed to the user, but it is extremely unlikely.

For an example of how to create an Accumulo connection, instantiate an AccumuloBackedGraph and query it from Java, see the
main method in gaffer.analytic.impl.GetElements.

Although Gaffer can be used with no knowledge of Accumulo, it still helps to have a little understanding of
what is actually happening when a query is made to avoid making unrealistic demands on the system. Each query for a
node causes a request from the client code to the tablet server that is responsible for data in the range the node
is in. The tablet server then performs a disk seek to the nearest index term, and then reads forward until it finds the right
row. The typical time for a disk seek is 8-10ms, and for reading forward some amount another 10ms, so a very rough estimate
of the lower-bound on the response time would be 20ms (although Accumulo will cache frequently accessed data, which can
reduce these times considerably). This means that each disk could perform 50 look-ups per second. Multiplying this by the
number of disks in the cluster gives an estimate of the maximum number of distinct queries than can be done per second. In
general, querying for thousands or tens of thousands seeds should be performant, querying for millions or billions
would require a different approach.

Note that AccumuloBackedGraph contains advanced options that allow you to configure the BatchScanners that are used
behind the scenes. These include `setMaxEntriesForBatchScanner()` and `setMaxThreadsForBatchScanner()`. There are
 similar options for continuous inserts: `setBufferSizeForInserts()`, `setTimeOutForInserts()`.

Filtering and aggregation within Accumulo
-----------------------------------------

Accumulo allows users to define iterators that are run either at compaction time or query time (or both). These can
be used for aggregation and filtering of data. Gaffer uses Accumulo's iterators extensively.

Within Gaffer, iterators are used for the following tasks:

- Age-off of old data
- Aggregation of statistics for the same entities and edges. Suppose we have an edge from A to B of a certain summary type and
subtype and a particular date range, and suppose this has been inserted into Accumulo. Later, we receive some more data and
this contains the exact same edge (i.e. from A to B with the same summary type and subtype and the same date range) with some
statistics. This is then inserted into Accumulo. These two instances of the same edge will be merged together, and their
statistics merged, automatically when Accumulo performs a compaction (it also happens at scan time so that the user
will always receive the edge with the two SetOfStatistics merged together).
- Filtering of results - several types of filtering are available. When a user specifies the TypeValues (i.e.
type-value pairs) they care about, they can choose to receive data involving those that has been filtered, e.g. they can
specify that they only want data with a certain summary type or subtype, or only from a particular time period. This
filtering happens using an iterator. See the section "Queries and the AccumuloBackedGraph" for how these
 filters are configured - the user does not have to configure (or know anything about) Accumulo's iterators in order
 to take advantage of them.
- Statistics transform - this is used to remove statistics from the results of a query. For example, the edges in a
graph may have multiple statistics, but the user may only care about the Count statistic. Specifying a transform
that removes all other statistics will improve query times because less data will be returned to the client.
- Rolling-up of statistics over different time windows and visibilities: when analysing summary data it is frequently necessary
to view the summaries over different time windows. For example, what was the count for this Entity during January? What was it
in the first week of February? Gaffer lets you specify a time window of interest and receive statistics that are aggregated
across that time window. E.g. if daily summaries are stored, then you can specify a time window of January 1st to 7th inclusive
and receive statistics that are aggregated across that time window. They are also aggregated across different visibilities of
data that you are authorized to see. This aggregation (or roll-up) is also performed by an iterator.
- Post roll-up transform: Allows the user to specify a final transform or filter that can be applied to the data
before they receive it.

Large-scale analytics
---------------------

Within the AccumuloBackedGraph there are methods to add the view (time window, summary type, entity only or edge
only or entity and edge only, authorizations, etc) to a Hadoop configuration. This will cause a MapReduce job over the
table to be subject to the same views as a query. It is possible to run over the entire data set or to specify a list of
type-values of interest.

An example showing how to run a Hadoop MapReduce job against Gaffer is contained in
`gaffer.accumulo.inputformat.example.ExampleDriver`.

Design considerations
---------------------

This section contains some general observations and notes about building a system on top of Gaffer.

###One table or several?###
Unless you have very large volumes of data, then putting all your data into one table is preferred. This will allow the
fastest query performance. If you have very large volumes, then it might be necessary to split the data across multiple
tables, either by time (e.g. one table for January's data, one for February's data) or by summary type (e.g. one table
for purchases and one for store visits). Currently Gaffer does not handle this partitioning into tables for you.

###Multiple sources of data and split points###
If building a Gaffer table from multiple sources of data then when initialising the table it is best to estimate the
split points from all the types.

###New statistics for old data###
Gaffer's merging of statistics allows the addition of new statistics for old data. For example, if Gaffer data is
extracted from some log data with a certain set of statistics, and later it is decided that it would have been
better to include some additional statistics, then the log data can be reprocessed and only the new statistics
output. When this is inserted into the Gaffer table, the SetOfStatistics will automatically be joined together
so that the new statistics appear alongside the old ones.

###Processing users###
To build systems where users can query Gaffer data via a web interface, it is typical to create a processing user
in Accumulo which has access to all visibilities of data. When a user with less than the full set of visibilities
visits the webpage, their visibilities are passed to AccumuloBackedGraph using the `setAuthorizations()` method,
which means that only data they are allowed to see will be returned.

###Bulk import or continuous import?###
Bulk import or continuous insert? The best option depends on the volume of data, the size of your cluster and the
latency requirements (i.e. whether it is necessary for data to be queryable immediately after it is generated).
If bulk import is used then ideally it should be scheduled to run when the query workload is low.

###Batching seeds for queries###
When querying Gaffer, it is best to batch the seeds up into a set, rather than run multiple individual queries. For
example, if performing a breadth-first search around some nodes, then all nodes in the frontier of the search should
be expanded simultaneously.

###Wildcarding types###
Gaffer natively supports querying by type and a value with a trailing wildcard, e.g. type=customer, value=A*. If
querying by wildcard types, e.g. for type of anything and value of X, is required then this can be achieved as follows.
For each entity in the data, create another entity with type of some fixed arbitrary string (e.g. 'ANY') and value
of the value of the entity. Create a SetOfStatistics for this containing a SetOfStrings statistic which contains
the type. If the data contains an entity with type T1 and value X, and an entity with type T2 and value X, then we
will generate an Entity with type 'ANY' and value X, which has a SetOfStrings statistic containing T1 and T2. To
perform a wildcard type search for value X, query for 'ANY'-X, then look in the SetOfStrings statistics, find T1
and T2 and then query for T1-X, T2-Y.

###Monitoring what data is in Gaffer###
Sometimes it might be useful to know all the different types of entity in the data, or approximately how many
different Entities are in the data. This can be done using Gaffer itself. For example to keep track of how many
different Entities there are, create an Entity with some arbitrary type-value such as 'ADMIN|Entity', with a date
range the same as the granularity for the rest of your data (e.g. daily in the case of daily summaries) and in the
set of statistics create a HyperLogLogPlusPlus sketch. When generating data, every time you see an entity, output
the ADMIN entity with a sketch containing the entity. These will be rolled up automatically, and by querying for the
ADMIN entity over a particular time range, you will be able to answer questions such as 'approximately how many
distinct entities were seen between January 1st and January 6th?'. Similar approaches allow you to estimate the
number of distinct types or visibilities of data.

###High degree nodes###
Many real-world graphs have a power-law degree distribution. This means that there are a lot of nodes with low degree
and a small number with very high degree. If you query Gaffer for a high-degree node then it can take a long time to
return all the edges. It is possible to get a very quick estimate of the number of edges that a node has by adding
a HyperLogLogPlusPlus sketch to the Entity. Setting the return entities only option on AccumuloBackedGraph means that
retrieving the entity for say customer|A will be extremely quick and the statistic will give an estimate of
the number of edges. If this is small, then the edges can be retrieved next, otherwise this node could be ignored.
(Note that this is an atypical use of a sketch in that it is not being used to save space, but instead to give quick
 approximate results - the accurate answer is in the data, but it will take a long time to compute.)
