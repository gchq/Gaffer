Copyright 2016-2017 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

The master copy of this page is the README in the accumulo-store module.

Accumulo Store
===================

1. [Introduction](#introduction)
2. [Use cases](#use-cases)
3. [Accumulo Set Up](#accumulo-set-up)
4. [Properties file](#properties-file)
5. [Schema](#schema)
6. [Inserting data](#inserting-data)
7. [Queries](#queries)
8. [Visibility](#visibility)
9. [Timestamp](#timestamp)
10. [Validation and age-off of data](#validation-and-age-off-of-data)
11. [Key packages](#key-packages)
12. [Trouble shooting](#trouble-shooting)
13. [Implementation details](#implementation-details)
14. [Tests](#tests)
15. [Accumulo 1.8.0 Support](#accumulo-1.8.0-support)
16. [Migration](#migration)


Introduction
-----------------------------------------------

Gaffer contains a store implemented using Apache Accumulo. This offers the following functionality:

- Scalability to large volumes of data;
- Resilience to failures of hardware;
- The ability to store any properties (subject to serialisers being provided if Gaffer is not aware of the objects);
- User-configured persistent aggregation of properties for the same vertices and edges;
- Flexibly query-time filtering, aggregation and transformation;
- Integration with Apache Spark to allow Gaffer data stored in Accumulo to be analysed as either an RDD or a Dataframe.

Use cases
-----------------------------------------------

Gaffer's `AccumuloStore` is particularly well-suited to graphs where the properties on vertices and edges are formed by aggregating interactions over time windows. For example, suppose that we want to produce daily summaries of the interactions between vertices, e.g. on January 1st 2016, 25 interactions between A and B were observed, on January 2nd, 10 interactions between A and B were observed. Gaffer allows this data to be continually updated (e.g. if a new interaction between A and B is observed on January 2nd then an edge for January 2nd between A and B with a count of 1 can be inserted and this will automatically be merged with the existing edge and the count updated). This ability to update the properties without having to perform a query, then an update, then a put, is important for scaling to large volumes.

Accumulo set up
-----------------------------------------------

Gaffer has been extensively tested with Accumulo version 1.8.1. It is recommended to use this version, although it should work with any of the 1.8.* versions of Accumulo as well.

For the purposes of unit testing and very small-scale ephemeral examples, Gaffer offers a [MockAccumuloStore](src/main/java/uk/gov/gchq/gaffer/accumulostore/MockAccumuloStore.java). This uses Accumulo's `MockInstance` to create an in-memory Accumulo store that runs within the same JVM as the client code. All data in this store will disappear when the JVM is shut down.

Gaffer can also be used with a `MiniAccumuloCluster`. This is an Accumulo cluster that runs in one JVM. To set up a `MiniAccumuloCluster` with Gaffer support, see the [mini-accumulo-cluster](https://github.com/gchq/gaffer-tools/tree/master/mini-accumulo-cluster) project in the Gaffer tools repository.

All real applications of Gaffer's `AccumuloStore` will use an Accumulo cluster running on a real Hadoop cluster consisting of multiple servers. Instructions on setting up an Accumulo cluster can be found in [Accumulo's User Manual](http://accumulo.apache.org/1.8/accumulo_user_manual).

To use Gaffer's Accumulo store, it is necessary to add a jar file to the class path of all of Accumulo's tablet servers. This jar contains Gaffer code that runs inside Accumulo's tablet servers to provide functionality such as aggregation and filtering at ingest and query time. 

The Accumulo store iterators.jar required can be downloaded from [maven central](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22uk.gov.gchq.gaffer%22%20AND%20a%3A%22accumulo-store%22). 
This jar file will then need to be installed on Accumulo's tablet servers by putting it in the `lib/ext` folder within the Accumulo distribution on each tablet server. Accumulo should load this jar file without needing to be restarted, but if you see error messages due to classes not being found, try restarting Accumulo. Alternatively, these files can be put into the `lib` directory and Accumulo can be restarted.

In addition, if you are using custom serialisers, properties or functions then a jar of these should be created and installed.

At this stage you have installed Gaffer into your Accumulo cluster. It is now ready for loading data.

Properties file
-----------------------------------------------

The next stage is to create a properties file that Gaffer will use to instantiate a connection to your Accumulo cluster. This requires the following properties:

- `gaffer.store.class`: The name of the Gaffer class that implements this store. For a full or mini Accumulo cluster this should be `gaffer.accumulostore.AccumuloStore`. To use the `MockAccumuloStore`, it should be `gaffer.accumulostore.MockAccumuloStore`.
- `gaffer.store.properties.class`: This is the name of the Gaffer class that contains the properties for this store. This should always be `gaffer.accumulostore.AccumuloProperties`.
- `accumulo.instance`: The instance name of your Accumulo cluster.
- `accumulo.zookeepers`: A comma separated list of the Zookeeper servers that your Accumulo cluster is using. Each server should specify the hostname and port separated by a colon, i.e. host:port.
- `accumulo.user`: The name of your Accumulo user.
- `accumulo.password`: The password for the above Accumulo user.

A typical properties file will look like:

```sh
gaffer.store.class=gaffer.accumulostore.AccumuloStore
gaffer.store.properties.class=gaffer.accumulostore.AccumuloProperties
accumulo.instance=myInstance
accumulo.zookeepers=server1.com:2181,server2.com:2181,server3.com:2181
accumulo.user=myUser
accumulo.password=myPassword
```

Note that if the graph does not exist, it will be created when a `Graph` object is instantiated using these properties and the schema (see next section). In this case the user must have permission to create a table. If the graph already exists then the user simply needs permission to read the table. For information about protecting data via setting the visibility, see [Visibilty](#visibility).

Other properties can be specified in this file. For details see [Advanced Properties](#advanced-properties). To improve query performance, see the property `accumulo.batchScannerThreads`. Increasing this from the default value of 10 can significantly increase the rate at which data is returned from queries.

Schema
-----------------------------------------------

See [Getting Started](https://gchq.github.io/gaffer-doc/getting-started/dev-guide.html#schemas) for details of how to write a schema that tells Gaffer what data will be stored, and how to aggregate it. Once the schema has been created, a `Graph` object can be created using:

```java
Graph graph = new Graph.Builder()
      .config(new GraphConfig.Builder()
            .graphId(uniqueNameOfYourGraph)
            .build())
      .addSchemas(schemas)
      .storeProperties(storeProperties)
      .build();
```

As noted above, if this is the first time that a `Graph` has been created with these properties, then the corresponding Accumulo table will be created and the correct iterators set-up.

Inserting data
-----------------------------------------------

There are two ways of inserting data into a Gaffer `AccumuloStore`: continuous load, and bulk import. To ingest large volumes of data, it is recommended to first set appropriate split points on the table. The split points represent how Accumulo partitions the data into multiple tablets. This is best done for continuous load, and essential for bulk imports.

**Setting appropriate split points**

The `SampleDataForSplitPoints` operation can be used to produce a file of split points that can then be used to set the split points on the table. It runs a MapReduce job that samples a percentage of the data, which is sent to a single reducer. That reducer simply writes the sorted data to file. This file is then read and sampled to produce split points that split the data into approximately equally sized partitions.

To run this operation, use:

```java
SampleDataForSplitPoints sample = new SampleDataForSplitPoints.Builder()
        .addInputPath(inputPath)
        .splitsFile(splitsFilePath)
        .outputPath(outputPath)
        .jobInitialiser(jobInitialiser)
        .validate(true)
        .proportionToSample(0.01F)
        .mapperGenerator(myMapperGeneratorClass)
        .build();
graph.execute(sample, new User());
```
where:

- `inputPath` is a string giving a directory in HDFS containing your data;
- `splitsFilePath` is a string giving a file in HDFS where the output of the operation will be stored (this file should not exist before the operation is run);
- `outputPath` is a string giving a directory in HDFS where the output of the MapReduce job will be stored;
- `jobInitialiser` is an instance of the `JobInitialiser` interface that is used to initialise the MapReduce job. If your data is in text files then you can use the built-in `TextJobInitialiser`. An `AvroJobInitialiser` is also provided;
- The `true` option in the `validate` method indicates that every element will be validated;
- The `0.01F` option in the `proportionToSample` method causes 1% of the data to be sampled. This is the amount of data that will be sent to the single reducer so it should be small enough for a single reducer to handle;
- `myMapperGeneratorClass` is a `Class` that extends the `MapperGenerator` interface. This is used to generate a `Mapper` class that is used to convert your data into `Element`s. Gaffer contains two built-in generators: `TextMapperGenerator` and `AvroMapperGenerator`. The former requires your data to be stored in text files in HDFS; the latter requires your data to be stored in Avro files.

To apply these split points to the table, run:

```java
SplitStore splitTable = new SplitStore.Builder()
        .inputPath(splitsFilePath)
        .build();
graph.execute(splitTable, new User());
```

**Continuous load**

This is done by using the `AddElements` operation and is as simple as the following where `elements` is a Java `Iterable` of Gaffer `Element`s that match the schema specified when the graph was created:

```java
AddElements addElements = new AddElements.Builder()
        .elements(elements)
        .build();
graph.execute(addElements, new User());
```

Note that here `elements` could be a never-ending stream of `Element`s and the above command will continuously ingest the data until it is cancelled or the stream stops.

**Bulk import**

To ingest data via bulk import, a MapReduce job is used to convert your data into files of Accumulo key-value pairs that are pre-sorted to match the distribution of data in Accumulo. Once these files are created, Accumulo moves them from their current location in HDFS to the correct directory within Accumulo's data directory. The data in them is then available for query immediately.

Gaffer provides code to make this as simple as possible. The `AddElementsFromHdfs` operation is used to bulk import data.


Create the `AddElementsFromHdfs`operation using:

```java
AddElementsFromHdfs addElementsFromHdfs = new AddElementsFromHdfs.Builder()
        .inputPaths(inputDirs)
        .outputPath(outputDir)
        .failurePath(failureDir)
        .mapperGenerator(myMapperGeneratorClass)
        .jobInitialiser(jobInitialiser)
        .build();
```

where:

- `inputDirs` is a `List` of strings specifying the directory in HDFS containing your data;
- `outputDir` is a string specifying the directory in HDFS where output from the MapReduce job will temporarily be stored (this directory does not need to exist);
- `failureDir` is a string specifying the directory in HDFS which Accumulo will use to store files that were not successfully imported;
- `myMapperGeneratorClass` is a `Class` that extends the `MapperGenerator` interface. This is used to generate a `Mapper` class that is used to convert your data into `Element`s. Gaffer contains two built-in generators: `TextMapperGenerator` and `AvroMapperGenerator`. The former requires your data to be stored in text files in HDFS; the latter requires your data to be stored in Avro files;
- `jobInitialiser` is an instance of the `JobInitialiser` interface that is used to initialise the MapReduce job. If your data is in text files then you can use the built-in `TextJobInitialiser`. An `AvroJobInitialiser` is also provided.

The operation can then be executed as normal using:

```java
graph.execute(addElementsFromHdfs, new User());
```

However, note that the Java jar file that contains this code must be executed using the `hadoop` command to ensure that the Hadoop configuration is available.

By default the number of reducers used in the MapReduce job that converts data into the correct form is the same as the number of tablets. There are at least two scenarios where this is not ideal. First, if a large amount of data is being added into a table with a small number of tablets, then the number of reducers will be too small (i.e. each reducer will have a very large amount of work to do). Second, if a small amount of data is being added into a table with a large number of tablets, then the number of reducers will be too great, and there will be a large amount of unnecessary task creation. There are two options on the `AddElementsFromHdfs` operation that can be used to set the minimum and maximum number of reducers that are used in the MapReduce job:

- `AccumuloStoreConstants.OPERATION_BULK_IMPORT_MIN_REDUCERS` specifies the minimum number of reducers to use.
- `AccumuloStoreConstants.OPERATION_BULK_IMPORT_MAX_REDUCERS` specifies the maximum number of reducers to use.

Queries
-----------------------------------------------

The Accumulo store supports all the standard queries. See the [Operation Examples](https://gchq.github.io/gaffer-doc/getting-started/operation-examples.html) for more details.

Visibility
-----------------------------------------------

Gaffer can take advantage of Accumulo's built-in fine-grained security to ensure that users only see data that they have authorisation to. This is done by specifying a "visibilityProperty" in the schema. This is a string that tells Accumulo which key in the `Properties` on each `Element` should be used for the visibility. The value of this property will be placed in the column visibility in each Accumulo key. This means that a user must have authorisations corresponding to that visibility in order to see the data.

If no "visibilityProperty" is specified then the column visibility is empty which means that anyone who has read access to the table can view it.

See [the visibility example](https://gchq.github.io/gaffer-doc/getting-started/dev-guide.html#visibilities) in the [Dev Guide](https://gchq.github.io/gaffer-doc/getting-started/dev-guide.html) guide for an example of how properties can be aggregated over different visibilities at query time.

Timestamp
-----------------------------------------------

Accumulo keys have a timestamp field. The user can specify which property is used for this by setting "timestampProperty" in the schema to the name of the property. If this is not specified then the time when the conversion to a key-value happens is used.

Validation and age-off of data
-----------------------------------------------

In production systems where data is continually being ingested, it is necessary to periodically remove data so that the total size of the graph does not become too large. The `AccumuloStore` allows the creator of the graph to specify custom logic to decide what data should be removed. This logic is applied during compactions, so that the data is permanently deleted. It is also applied during queries so that even if a compaction has not happened recently, the data that should be removed is still hidden from the user.

A common approach is simply to delete data that is older than a certain date. In this case, within the properties on each element there will be a time window specified. For example, the properties may contain a "day" property, and the store may be configured so that once the day is more than one year ago, it will be deleted. This can be implemented as follows:

- Each element has a property called, for example, "day", which is a `Long` which contains the start of the time window. Every time an element is observed this property is set to the previous midnight expressed in milliseconds since the epoch.
- In the schema the validation of this property is expressed as follows:
```sh
"long": {
    "class": "java.lang.Long",
    "validateFunctions": [
        {
            "function": {
                "class": "gaffer.function.simple.filter.AgeOff",
                "ageOffDays": "100"
              }
        }
    ]
}
```
- Then data will be aged-off whenever it is more than 100 days old.

Key-packages
-----------------------------------------------

In Gaffer's `AccumuloStore` a key-package contains all the logic for:

- Converting `Element`s into Accumulo key-value pairs, and vice-versa;
- Generating ranges of Accumulo keys that correspond to the seeds of a query;
- Creating the iterator settings necessary to perform the persistent aggregation that happens at compaction time, and the filtering and aggregation that happens during queries;
- Creating the `KeyFunctor` used to configure the Bloom filters in Accumulo.

A key-package is an implementation of the `AccumuloKeyPackage` interface. Gaffer provides two implementations: `ByteEntityKeyPackage` and `ClassicKeyPackage`. These names are essentially meaningless. The "classic" in `ClassicKeyPackage` refers to the fact that it is similar to the implementation in the first version of Gaffer (known as "Gaffer1").

Both key-packages should provide good performance for most use-cases. There will be slight differences in performance between the two for different types of query. The `ByteEntityKeyPackage` will be slightly faster if the query specifies that only out-going or in-coming edges are required. The `ClassicKeyPackage` will be faster when querying for all edges involving a pair of vertices. See the Key-Packages part of the [Implementation details](#implementation-details) section of this guide for more information about these key-packages.

Advanced properties
-----------------------------------------------

The following properties can also be specified in the properties file. If they are not specified, then sensible defaults are used.

- `gaffer.store.accumulo.keypackage.class`: The full name of the class to be used as the key-package. By default `ByteEntityKeyPackage` will be used.
- `accumulo.batchScannerThreads`: The number of threads to use when `BatchScanner`s are created to query Accumulo. The default value is 10.
- `accumulo.entriesForBatchScanner`: The maximum number of ranges that should be given to an Accumulo `BatchScanner` at any one time. The default value is  50000.
- `accumulo.clientSideBloomFilterSize`: The size in bits of the Bloom filter used in the client during operations such as `GetElementsBetweenSets`. The default value is 838860800, i.e. 100MB.
- `accumulo.falsePositiveRate`: The desired rate of false positives for Bloom filters that are passed to an iterator in operations such as `GetElementsBetweenSets`. The default value is 0.0002.
- `accumulo.maxBloomFilterToPassToAnIterator`: The maximum size in bits of Bloom filters that will be created in an iterator on Accumulo's tablet server during operations such as `GetElementsBetweenSets`. By default this will be 8388608, i.e. 1MB.
- `accumulo.maxBufferSizeForBatchWriterInBytes`: The size of the buffer in bytes used in Accumulo `BatchWriter`s when data is being ingested. The default value is 1000000.
- `accumulo.maxTimeOutForBatchWriterInMilliseconds`: The maximum latency used in Accumulo `BatchWriter`s when data is being ingested. Th default value is 1000, i.e. 1 second.
- `accumulo.numThreadsForBatchWriter`: The number of threads used in Accumulo `BatchWriter`s when data is being ingested. The default value is 10.
- `accumulo.file.replication`: The number of replicas of each file in tables created by Gaffer. If this is not set then your general Accumulo setting will apply, which is normally the same as the default on your HDFS instance.
- `gaffer.store.accumulo.enable.validator.iterator`: This specifies whether the validation iterator is applied. The default value is true.

Trouble shooting
-----------------------------------------------

**Data hasn't appeared after I performed a bulk import**

Accumulo's UI often shows that there are zero entries in a table after a bulk import. This is generally because Accumulo does not know how many entries have been added until it has performed a major compaction. Open the Accumulo shell, change to the table you specified in your Accumulo properties file, and type `compact`. You should see compactions starting for that table in Accumulo's UI, and the number of entries should then start to increase.

If this has not solved the problem, look at the logs of your bulk import MapReduce job and check that the number of entries output by both the Mapper and the Reducer was positive.

Next check that the elements you generate pass your validation checks.

If all the above fails, try inserting a small amount of data using `AddElements` to see whether the problem is your bulk import job or your data generation.

**Queries result in no data**

Check that you have the correct authorisations to see the data you inserted. Check with the administrator of your Accumulo cluster.

Implementation details
-----------------------------------------------

This section contains brief details on the implementation of the `AccumuloStore`. The core of the functionality is implemented in the key-packages, the iterators and the retrievers. Each of these is described in some detail below. It is assumed that the reader has some familiarity with the design of Accumulo (see [Accumulo's User Manual](http://accumulo.apache.org/1.8/accumulo_user_manual)). The important features for Gaffer are:

- Accumulo stores data in key-value pairs. A key has multiple parts, namely a row ID, a column family, a column qualifier, a column visibility, and a timestamp. Each of these is simply a byte array, with the exception of the timestamp which is a long. A value is simply a byte array.
- Data in Accumulo is stored ordered by key. Keys are stored sorted by increasing row ID, then column family, then column qualifier, then column visibility, then by decreasing timestamp.
- Accumulo allows locality groups to be set which group together column families. This means that scans that only need to read certain column families can skip families they do not need to read.
- Accumulo allows data to be tagged with a visibility which restricts which users can view it.
- Accumulo allows the user to configure iterators that run at scan time, at compaction time or both. Gaffer adds iterators to scans to filter data. It uses compaction time iterators to persistently aggregate the properties of elements together, and to continually validate data.
- Accumulo provides an `InputFormat` that allows data to be retrieved via MapReduce jobs.

**Key-packages**

As noted in the [Key-packages](#key-packages) section above, key-packages are responsible for converting `Element`s to and from key-value pairs, for creating ranges of keys containing all data relevant to a particular query, and for configuring the iterators. Gaffer provides two key-packages: `ByteEntityKeyPackage` and `ClassicKeyPackage`. Advanced users are able to create their own key-packages if they wish --- see [Options for future key-packages](Options for future key-packages) for some ideas.

Before these key-packages are described, we review the main design goals:

- To be able to retrieve all `Edge`s for a vertex by seeking to a single point in the table and scanning forwards.
- To be able to retrieve all `Entity`s for a vertex by seeking to a single point in the table, and reading only relevant key-value pairs, i.e. not reading any of the `Edge`s associated to the vertex.
- A vertex should be uniquely identified by its serialised value. It should not be necessary to consult an external source to find the value that identifies a vertex. In particular unlike most graph databases we do not use longs to identify vertices.
- To ensure that there are no "fat" rows, i.e. that there are not very large numbers of key-value pairs with the same row-key.
- To allow efficient aggregation of properties.

Both key-packages convert an `Entity` into a single Accumulo key-value pair and an `Edge` into two key-value pairs. The row ID (also known as the row-key) of the key-value formed from the `Entity` is the vertex serialised to a byte array, followed by a flag to indicate that this is an `Entity`. This allows the `Entity`s associated to a vertex to be quickly retrieved. It is necessary to store each `Edge` as two key-values so that it can found from both the source vertex and the destination vertex: one key-value has a row ID consisting of the source vertex serialised to a byte array, followed by a delimiter, followed by the destination vertex serialised to a byte array; the other key-value has the opposite, with the destination vertex followed by the source vertex. A flag is also stored to indicate which of these two versions the key is so that the original `Edge` can be recreated.

An important feature of the row IDs created by both key-packages is that it is possible to create ranges of keys that either only contain the `Entity`s or only contain the `Edge`s or contain both. This means that if, for example, a user states that they only want to retrieve the `Entity`s for a particular vertex then only relevant key-value pairs need to be read. In the case of a high-degree vertex, this means that queries for just the `Entity`s will still be very quick.

The two key-packages differ in subtle details of how the row ID is created. In the following descriptions the notation `(serialised_vertex)` refers to the vertex serialised to a byte array with any occurrences of the zero byte removed. This is necessary so that the zero byte delimiter can be used to separate different parts of the row-key. The zero bytes are removed in such a way that the original byte array can be recreated, and so that ordering is preserved.

***`ClassicKeyPackage` details***

The `ClassicKeyPackage` constructs the following Accumulo key-value pair for an `Entity`:

<table>
<tr>
    <th>Row ID</th>
    <th>Column Family</th>
    <th>Column Qualifier</th>
    <th>Visibility</th>
    <th>Timestamp</th>
    <th>Value</th>
</tr>
<tr>
    <td>(serialised_vertex)</td>
    <td>group</td>
    <td>group by properties</td>
    <td>visibility property</td>
    <td>timestamp</td>
    <td>all other properties</td>
</tr>
</table>

The following Accumulo key-value pairs are created for an `Edge`:

<table>
<tr>
    <th>Row ID</th>
    <th>Column Family</th>
    <th>Column Qualifier</th>
    <th>Visibility</th>
    <th>Timestamp</th>
    <th>Value</th>
</tr>
<tr>
    <td>(serialised_source_vertex)0(serialised_destination_vertex)0x</td>
    <td>group</td>
    <td>group by properties</td>
    <td>visibility property</td>
    <td>timestamp</td>
    <td>all other properties</td>
</tr>
<tr>
    <td>(serialised_destination_vertex)0(serialised_source_vertex)0y</td>
    <td>group</td>
    <td>group by properties</td>
    <td>visibility property</td>
    <td>timestamp</td>
    <td>all other properties</td>
</tr>
</table>

If the `Edge` is undirected then `x` and `y` are both 1 for both key-values. If the `Edge` is directed then `x` is 2 and `y` is 3.

This is very similar to the design of the key-value pairs in version 1 of Gaffer, with the exception that version 1 did not store a delimiter or flag at the end of the row-key for an `Entity`. This necessitated a scan of the row-key counting the number of delimiters to determine whether it was an `Entity` or `Edge`. If it is an `Entity` the vertex could be created directly from the row-key. For the `ClassicKeyPackage`, this scan is not needed but an array copy of the row-key minus the delimiter and flag is needed. In practice, the difference in performance between the two is likely to be negligible.

***`ByteEntityKeyPackage` details***

The ByteEntity key-package constructs the following Accumulo key-value pair for an `Entity`:

<table>
<tr>
    <th>Row ID</th>
    <th>Column Family</th>
    <th>Column Qualifier</th>
    <th>Visibility</th>
    <th>Timestamp</th>
    <th>Value</th>
</tr>
<tr>
    <td>(serialised_vertex)01</td>
    <td>group</td>
    <td>group by properties</td>
    <td>visibility property</td>
    <td>timestamp</td>
    <td>all other properties</td>
</tr>
</table>

In the row ID the 0 is a delimiter to split the serialised vertex from the 1. The 1 indicates that this is an `Entity`. By having this flag at the end of the row id it is easy to determine if the key relates to an `Entity` or an `Edge`.

The following Accumulo key-value pairs are created for an `Edge`:

<table>
<tr>
    <th>Row ID</th>
    <th>Column Family</th>
    <th>Column Qualifier</th>
    <th>Visibility</th>
    <th>Timestamp</th>
    <th>Value</th>
</tr>
<tr>
    <td>(serialised_source_vertex)0x0(serialised_destination_vertex)0x</td>
    <td>group</td>
    <td>group by properties</td>
    <td>visibility property</td>
    <td>timestamp</td>
    <td>all other properties</td>
</tr>
<tr>
    <td>(serialised_destination_vertex)0y0(serialised_source_vertex)0y</td>
    <td>group</td>
    <td>group by properties</td>
    <td>visibility property</td>
    <td>timestamp</td>
    <td>all other properties</td>
</tr>
</table>

If the `Edge` is undirected then both `x` and `y` are 4. If the `Edge` is directed then `x` is 2 and `y` is 3.

The flag is repeated twice to allow filters that need to know whether the key corresponds to a `Entity` or an `Edge` to avoid having to fully deserialise the row ID. For a query such as find all out-going edges from this vertex, the flag that is directly after the source vertex can be used to restrict the range of row IDs queried for.

Note that in a range query filtering to restrict the results to say only out-going edges happens in an iterator.

***Options for future key-packages***

Numerous variations on the above key-packages could be implemented. These would generally improve the performance for some types of query, at the expense of decreasing the performance for other types of query. Some examples are:

- The row-keys could be sharded. The current design is optimised for retrieving all `Edge`s for a given vertex, when there are relatively few such `Edge`s. If there are a million edges for a vertex then all of these have to be read by a small number of tablet servers (typically one, unless the range spans multiple tablets). This limits the query performance. An alternative approach is to introduce a shard key at the start of the row-key to cause different edges for the same vertex to be spread uniformly across the table. This would increase the parallelism for queries which would lead to better performance when large numbers of edges need to be retrieved for a vertex. The trade-off is that all queries would need to query all shards which would reduce the performance when a vertex has only a small number of edges.
- If there are a very large number of `Edge`s with the same source, destination and group-by properties then this could cause unbalanced tablets. A sharding scheme similar to the above would deal with this.
- Remove the flag at the end of the row-key that indicates whether it corresponds to an `Entity` or an `Edge`. This is used to quickly determine whether it is an `Entity` or an `Edge`. This is actually superfluous information as the group is stored in the column family and that indicates whether the key-value is an `Entity` or an `Edge`. Storing the flag there creates the need for an array copy when an `Entity` is created from the key-value. Instead of storing the group string in the column family, two bytes could be stored. The first would indicate whether this is an `Entity` or an `Edge`, and if an `Edge` whether it needs reversing or not; the second would indicate what group it is.
- Store each group in a separate table. This should slightly improve the performance of queries that only require a subset of the groups, especially if the query scans lots of data (as Accumulo's locality groups are set in the above key-packages the performance improvement will probably be minor). It would worsen the query performance when multiple groups are being retrieved.
- If the vertices serialise to a fixed length, or if a maximum length is known, then the row-keys could be of fixed length. This would eliminate the need for the use of delimiters which forces the escaping of the zero byte inside the serialised value. This would potentially provide a small increase in ingest and query speed.

**Iterators**

Gaffer makes substantial use of Accumulo's iterator functionality to perform permanent aggregation and validation of data at compaction time, and filtering and aggregation at query time. See the [Iterators](http://accumulo.apache.org/1.8/accumulo_user_manual.html#_iterators) section of Accumulo's User Guide for more information on iterators.

The following subsections describes the iterators that are used in Gaffer. They are listed in decreasing order of priority, i.e. the first iterator runs first. The text in brackets after the name of the iterator gives the scopes that the iterator is applied in. Some iterators that are only used for very specific operations are not listed here.

***`AggregatorIterator` (compaction, scan)***

This iterator aggregates together all properties that are not group-by properties for `Element`s that are otherwise identical. As the non-group-by properties are stored in the `Value` this means that all `Value`s for identical keys are merged together.

***`ValidatorFilter` (compaction, scan)***

The `ValidatorFilter` iterator validates every `Element` using the validation logic defined in the schema. When this is run during a compaction it causes invalid data to be deleted. This is typically used to delete data that is older than a certain date.

***`ClassicEdgeDirectedUndirectedFilterIterator` (scan)***

NB. This is only used in the `ClassicKeyPackage`.

This is used to filter out edges that are not required because the user has specified filters relating to edge direction (outgoing or incoming) and edge "directedness" (directed or undirected) in their query. Note that it is possible to ask for various combinations of these, e.g.:

- Directed edges only: if the seed is A then directed edges A->B and B->A would be returned, but an undirected edge A-B wouldn't be.
- Directed outgoing edges only: if the seed is A then a directed edge A->B would be returned, but a directed edge B->A wouldn't be, nor would an undirected edge A-B.
- Directed incoming edges only: if the seed is A then a directed edge B->A would be returned, but a directed edge A->B wouldn't be, nor would an undirected edge A-B.
- Undirected edges only: if the seed is A then an undirected edge A-B would be returned, but directed edges A->B and B->A wouldn't be.
- Undirected outgoing edges only: if the seed is A then an undirected edge A-B would be returned, but directed edges A->B and B->A wouldn't be.
- Undirected incoming edges only: if the seed is A then an undirected edge A-B would be returned, but directed edges A->B and B->A wouldn't be.

In the latter two examples, note that an undirected edge A-B is defined to be both outgoing from, and incoming to, both A and B.

***`ElementPreAggregationFilter` (scan)***

This iterator filters out `Element`s that are not valid according to the `View`. This filtering happens before the aggregation.

***`CoreKeyGroupByAggregatorIterator` (scan)***

This iterator aggregates together all properties according to the group-by in the view.

***`ElementPostAggregationFilter` (scan)***

This iterator filters out `Element`s that are not valid according to the `View`. This filtering happens after the aggregation.

**Locality groups**

Accumulo's ability to have a large number of different column families allows Gaffer to store lots of different types of data in the same table. Specifying the locality groups means that when a query for a particular group is made, graph elements from other groups do not need to be read.

## Tests

### Running the integration tests

Update the following store properties files to point to the location of the Accumulo store to test against:
- [src/test/resources/store.properties](src/test/resources/store.properties)
- [src/test/resources/accumuloStoreClassicKeys.properties](src/test/resources/accumuloStoreClassicKeys.properties)

Ensure that one of the following classes is being used for the `gaffer.store.class` property:
- uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore
- uk.gov.gchq.gaffer.accumulostore.SingleUseAccumuloStore

Ensure that the Accumulo user specified by the `accumulo.user` property has the `System.CREATE_TABLE` permission and the following scan authorisations:

| Authorisation     | Required by |
| ----------------- | ----------- |
| vis1              | [VisibilityIT](../../integration-test/src/test/java/uk/gov/gchq/gaffer/integration/impl/VisibilityIT.java) |
| vis2              | [VisibilityIT](../../integration-test/src/test/java/uk/gov/gchq/gaffer/integration/impl/VisibilityIT.java) |
| publicVisibility  | [AccumuloAggregationIT](src/test/java/uk/gov/gchq/gaffer/accumulostore/integration/AccumuloAggregationIT.java) |
| privateVisibility | [AccumuloAggregationIT](src/test/java/uk/gov/gchq/gaffer/accumulostore/integration/AccumuloAggregationIT.java) |

Run the integration tests:

```
mvn verify
```

## Accumulo 1.8.0 Support

Gaffer can be compiled with support for Accumulo 1.8.0. Clear your Maven repository of any Gaffer artifacts and compile Gaffer with the Accumulo-1.8 profile:

```
mvn clean install -Paccumulo-1.8
```

## Migration

The Accumulo Store also provides a utility [AddUpdateTableIterator](https://github.com/gchq/Gaffer/blob/master/store-implementation/accumulo-store/src/main/java/uk/gov/gchq/gaffer/accumulostore/utils/AddUpdateTableIterator.java)
to help with migrations - updating to new versions of Gaffer or updating your schema.

The following changes to your schema are allowed:
- add new groups
- add new non-groupBy properties (including visibility and timestamp), but they must go after the other properties
- rename properties
- change aggregators (your data may become inconsistent as any elements that were aggregated on ingest will not be updated.)
- change validators
- change descriptions

But, you cannot do the following:
- rename groups
- remove any properties (groupBy, non-groupBy, visibility or timestamp)
- add new groupBy properties
- reorder any of the properties. In the Accumulo store we don't use any property names, we just rely on the order the properties are defined in the schema.
- change the way properties or vertices are serialised - i.e don't change the serialisers.
- change which properties are groupBy

Please note, that the validation functions in the schema can be a bit dangerous. 
If an element is found to be invalid then the element will be permanently deleted from the table. 
So, be very careful when making changes to your schema that you don't accidentally make all your existing elements invalid as you will quickly realise all your data has been deleted. 
For example, if you add a new property 'prop1' and set the validateFunctions to be a single Exists predicate. 
Then when that Exists predicate is applied to all of your existing elements, those old elements will fail validation and be removed.

To carry out the migration you will need the following:

- your schema in 1 or more json files.
- store.properties file contain your accumulo store properties
- a jar-with-dependencies containing the Accumulo Store classes and any of your custom classes. 
If you don't have any custom classes then you can just use the accumulo-store-[version]-utility.jar. 
Otherwise you can create one by adding a build profile to your maven pom:
```xml
<build>
    <plugins>
        <plugin>
            <artifactId>maven-shade-plugin</artifactId>
            <version>${shade.plugin.version}</version>
            <executions>
                <execution>
                    <id>utility</id>
                    <phase>package</phase>
                    <goals>
                        <goal>shade</goal>
                    </goals>
                    <configuration>
                        <shadedArtifactAttached>true
                        </shadedArtifactAttached>
                        <shadedClassifierName>utility
                        </shadedClassifierName>
                    </configuration>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```


If you have existing data, then before doing any form of upgrade or change to your table we strongly recommend using the accumulo shell to clone the table so you have a backup so you can easily restore to that version if things go wrong. 
Even if you have an error like the one above where all your data is deleted in your table you will still be able to quickly revert back to your backup. 
Cloning a table in Accumulo is very simple and fast (it doesn't actually copy the data). If you have a table called 'table1', you can do something like the following in the Accumulo shell:

```bash
> offline -t table1
> clone table table1 table1-backup
> offline -t table1-backup

# Do your upgrades
#   - deploy new gaffer jars to Accumulo's class path on each node in your cluster
#   - run the AddUpdateTableIterator class to update table1

> online -t table1

# Check table1 is still healthy:
#   - run a query and check the iterators are successfully aggregating and filtering elements correctly.

> droptable -t table1-backup
```

You will need to run the AddUpdateTableIterator utility providing it with your graphId, schema and store properties.
If you run it without any arguments it will tell you how to use it.

```bash
java -cp [path to your jar-with-dependencies].jar uk.gov.gchq.gaffer.accumulostore.utils.AddUpdateTableIterator
```