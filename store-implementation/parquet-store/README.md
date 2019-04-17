Copyright 2017-2019 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

# Parquet Store

Contents:
1. [Notice](#notice)
2. [Introduction](#introduction)
3. [Design overview](#design-overview)
4. [Use cases](#use-cases)
5. [Parquet version](#parquet-version)
6. [Properties file](#properties-file)
4. [Schema](#schema)
5. [Inserting data](#inserting-data)
6. [Queries](#queries)
7. [Custom serialisers](#custom-serialisers)
7. [Optimisations](#optimisations)
7. [Troubleshooting](#troubleshooting)
8. [Implementation details](#implementation-details)
   - [Code overview](#code-overview)
   - [Graph folder structure](#graph-folder-structure)
   - [Partitioning strategy](#partitioning-strategy)
   - [Conversion between Gaffer elements and Parquet](#conversion-between-gaffer-elements-and-parquet)
   - [Add elements operation](#add-elements-operation)
   - [Import RDD of elements operation](#import-rdd-of-elements-operation)

## Notice

This store is experimental. The implementation is unstable and may require breaking changes.

## Introduction

Gaffer contains a store implemented using [Apache Parquet](https://parquet.apache.org/) version 1.8.3. Graph elements are stored in Parquet files (typically in HDFS). This offers the following functionality:
- A scalable data store of entities and edges;
- A very compact data store using a columnar file format;
- Fast full scan and random access queries on the same store;
- Integration with Apache Spark to allow Gaffer data stored in Parquet to be analysed as a `Dataframe`;
- User-configured persistent aggregation of properties for the same entities and edges;
- The ability to customise how vertices and properties are converted into columns in the Parquet files.

## Design overview

Elements are stored in Parquet files.

Each vertex and property is stored in one or more columns. Properties that are simple primitives that are natively supported by Parquet are stored as primitives (rather than serialised objects). Properties such as `HyperLogLogPlus` are stored both as a serialised byte array and as a primitive column which gives the current state of the `HyperLogLogPlus`; in this case the current estimate of the sketch is stored. The serialised form allows it to be updated with new data. The primitive column allows high-performance scans over the current value of the sketch.

The store properties require a directory to be provided. That directory contains a subdirectory for each snapshot of the graph. Each time data is added to the graph a new copy of the graph is created containing the old and new data merged together. Within each snapshot directory, there is a graph directory containing a directory per group. This directory contains Parquet files that contain elements of that group. This allows analytics that only require a subset of the groups to avoid the cost of reading unnecessary data.

Within each group, the elements are stored globally, sorted by vertex in the case of Entities or by source in the case of Edges. This means that a query for all edges of a given group involving a vertex only needs to open a small number of the files in order to find the required data. As the data is sorted, Parquet's statistics allow it to avoid reading unnecessary row groups. This allows low latency random access queries.

There is also a directory called reversedEdges which contains subdirectories for each edge group within which the elements are stored sorted by destination vertex.

In addition, the top-level directory contains a file called graphPartitioner which contains information about how the elements are partitioned across the files, i.e. the first entry in each file.

When new data is added to the store, [Apache Spark](https://spark.apache.org/) is used to read, sort and aggregate both the existing data and the new data to form a new directory containing the new data. Concurrent updates are not supported. Thus this store is suited for occasional bulk updates, rather than continual updates.

## Use cases

Gaffer's `ParquetStore` is particularly well suited to graphs with lots of properties on the entities and edges where you want to perform both full table scans and random access queries. The columnar format allows excellent performance for queries such as what is the average value of the count property over all edges? In this case only the count column needs to be read - all the other properties are not read from disk.

This store can be used as a source of data for frameworks such as GraphX, e.g. the graph of all edges where the count is greater than 10 is loaded from the `ParquetStore` into memory, and then the PageRank algorithm is run using GraphX.

The `ParquetStore` could be used as part of a hybrid system where you use a Gaffer store that continually ingests new data. Overnight a snapshot of that data is written into a `ParquetStore` to benefit from the smaller storage requirements and fast full scan capabilities while maintaining fast random access.

## Parquet version

As many of the operations on the Parquet store use Spark to implement the partitioning and sorting logic, the version of Parquet used is determined by the version that Spark uses. Gaffer currently uses Spark version 2.3.2 which has a dependency on Parquet 1.8.3. Parquet 1.11.0 contains upgrades to the version of parquet-format which improve the performance of "indexed" queries against Parquet files - this should increase the performance of random access queries against the Parquet store. Future work should investigate this further, although this may have to wait until Spark has updated the version of Parquet it uses.

## Properties file

The `ParquetStoreProperties` class contains all properties relating to the configuration of the `ParquetStore`. It can be created from a properties file. It has sensible defaults for all parameters but users may want to tune these properties to suit their data. The following properties can be set:

- `spark.master`: The string that sets what mode to run Spark in. By default, if Spark is installed on the machine it will use Spark's defaults, otherwise it will run in local mode using all available threads;
- `parquet.data.dir`: The directory used to save the graph;
- `parquet.temp_data.dir`: The directory to use as a working space for temporary data generated whilst add operations are being executed;
- `parquet.threadsAvailable`: The number of threads to make available to operations (this is for operations that do not use Spark);
- `parquet.add_elements.row_group.size`: This parameter sets the maximum row group size in bytes before compression for the Parquet files, see [Parquet documentation](https://parquet.apache.org/documentation/latest/) for more information. By default this is set to 4MB;
- `parquet.add_elements.page.size`: This exposes the Parquet file format parameter controlling the maximum page and dictionary page size in bytes before compression, see [Parquet documentation](https://parquet.apache.org/documentation/latest/) for more information. By default this is set to 1MB;
- `parquet.add_elements.output_files_per_group`: This is the number of files that the output data is split into within a group. By default this is set to 10;
- `parquet.compression.codec`: This is the compression codec to use when writing Parquet files. Valid options are  UNCOMPRESSED, SNAPPY, GZIP, LZO.

A complete Gaffer properties file using a `ParquetStore` will look like:

```
gaffer.store.class=uk.gov.gchq.gaffer.parquetstore.ParquetStore
gaffer.store.properties.class=uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties
spark.master=yarn
parquet.data.dir=/User/me/my_gaffer_parquet_store
parquet.temp_data_dir=/tmp/my_gaffer_parquet_store_tmp
parquet.add_elements.threadsAvailable=9
parquet.add_elements.row_group.size=1073741824
parquet.add_elements.page.size=4194304
parquet.add_elements.output_files_per_group=2
```

## Schema

See the [Getting Started guide to schemas](https://gchq.github.io/gaffer-doc/getting-started/developer-guide/schemas.html) for details of how to write a schema that tells Gaffer what data will be stored, and how to aggregate it. Once the schema has been created, a `Graph` object can be created using:

```
Graph graph = new Graph.Builder()
      .config(new GraphConfig.Builder()
          .graphId(uniqueNameOfYourGraph)
          .build())
      .addSchemas(schemas)
      .storeProperties(storeProperties)
      .build();
```

To get the best performance you should allow Gaffer to detect the best serialiser or provide a serialiser class that implements `ParquetSerialisation`. See the section on [serialisers](custom-serialisers) for further information about serialisers optimised to work with the Parquet store.

## Inserting data

The `ParquetStore` has two ways in which you can insert data into the graph. The first method is via the standard Gaffer `AddElements` operation which allows data to be inserted from a Java `Iterable`. For the `ParquetStore` this iterable must be of finite size as it is fully consumed before being sorted and merged with the existing data.

```
AddElements addElements = new AddElements.Builder()
        .elements(elements)
        .build();
graph.execute(addElements, new User());
```

The other way to import data is to from an `RDD<Element>` using the `ImportRDDOfElements` operation.

```
RDD<Element> elements = ..// Create an RDD<Element> from your data
ImportRDDOfElements addElements = new ImportRDDOfElements.Builder()
        .input(elements)
        .build();
graph.execute(addElements, new User());
```

To insert large data sets the `ImportRDDOfElements` operation is recommended.

Note that the Parquet store is designed to support sequential bulk imports of data. It is not designed to continually ingest new data. It is also not designed to allow multiple concurrent ingest operations.

## Queries

The `ParquetStore` currently supports most of the [standard Gaffer queries](https://gchq.github.io/gaffer-doc/getting-started/spark-operations/contents.html).

The operations that are not currently supported are:
- `GetAdjacentEntitySeeds`
- `GetJavaRDDOfAllElements`
- `GetJavaRDDOfElements`
- `GetRDDOfAllElements`
- `GetRDDOfElements`

## Custom serialisers

The `ParquetStore` converts Java objects to Parquet types so that elements can be stored in Parquet files. Parquet primitive types include booleans, 32 and 64 bit integers, floats, doubles and byte arrays. Parquet also has logical types that are stored as primitive types with information on how to interpret them. Logical types include UTF-8 strings and timestamps.

Some Java objects, such as `Long` and `String`, do not require any conversion into Parquet types as they are natively supported. Others, such as `HyperLogLogPlus`, are not natively supported. For an object such as a `HyperLogLogPlus`, the approach is to serialise it to two Parquet types. The first is a byte array containing the object serialised to a byte array using the standard `ToBytesSerialiser`. This allows the full Java object to be reconstructed from the data stored in the Parquet file (this is necessary when elements are returned by `GetElements` queries and when properties are aggregated). The second type is a long specifying the current cardinality estimate of the sketch. This allows some operations to be able to use that value without the cost of reading and deserialising the byte array. For example, if the `GetDataFrameOfElements` operation is used to return the graph as a Spark DataFrame then a query against that DataFrame to return the row with the maximum current cardinality estimate will only need to read one column of longs. An application of this would be to find the vertex with the maximum degree or the degree distribution of the graph. 

The `ParquetSerialiser` interface contains three main methods. The first, `getParquetSchema`, specifies the Parquet types and column names that an object will be converted into. For example, the `LongParquetSerialiser` returns `"optional int64 " + colName + ";"` from this method indicating that the long will be serialised to an int64. Two `ParquetSerialiser`s for `HyperLogLogPlus` objects are available. The first, `InLineHyperLogLogPlusParquetSerialiser`, returns the following from the `getParquetSchema` method:

                "optional binary " + colName + "_raw_bytes;\n" +
                "optional int64 " + colName + "_cardinality;";
This shows that the `HyperLogLogPlus` is serialised to two columns, one a byte array and one an int64. The `NestedHyperLogLogPlusParquetSerialiser` uses the following schema:

                "optional group " + colName + " {\n" +
                "\toptional binary raw_bytes;\n" +
                "\toptional int64 cardinality;\n" +
                "}";
This shows that the `HyperLogLogPlus` is serialised to one column containing two subcolumns.

The other two methods on `ParquetSerialiser` of objects of class `T` are `Object[] serialise(final T object)` and `T deserialise(final Object[] objects)`. The `Object`s returned by serialise must be of Parquet types and the order must match the order specified in the Parquet schema.

## Optimisations

With Get operations there are optimisations performed to allow for the minimal number of files to be queried based on the inputs to the query.

This means that you will get significant improvements if you can formulate your view filters with the `Or` and `Not` filters as close to the leaves of the filter tree as possible. For example if you had an And filter, then the And filter would be the root of the filter tree with the two predicates being the leaves.

You will also gain significant improvements if you run two queried and merged/deduplicate the results locally rather than using an `Or` filter at the root of the filter tree. For example, you could write a ranged filter that can be run as a single query as:
```
ElementFilter rangedFilter = new ElementFilter().Builder()
  .select(ParquetStoreConstants.SOURCE, ParquetStoreConstants.DESTINATION)
  .execute(
    new Or.Builder()
      .select(0)
      .execute(
        new And.Builder()
          .select(0)
          .execute(new IsMoreThan(5, true))
          .select(0)
          .execute(new IsLessThan(10, true))
          .build())
      .select(1)
      .execute(
        new And.Builder()
          .select(0)
          .execute(new IsMoreThan(5, true))
          .select(0)
          .execute(new IsLessThan(10, true))
          .build())
      .build())
  .build())
```
This produces the correct results. However the filters would have to be passed to every file for the groups that it was applied to, whereas if you split the query into two queries as:
```
ElementFilter srcRangedFilter = new ElementFilter().Builder()
  .select(ParquetStoreConstants.SOURCE)
  .execute(
    new And.Builder()
      .select(0)
      .execute(new IsMoreThan(5, true))
      .select(0)
      .execute(new IsLessThan(10, true))
      .build())
  .build())
  
ElementFilter dstRangedFilter = new ElementFilter().Builder()
  .select(ParquetStoreConstants.SOURCE, ParquetStoreConstants.DESTINATION)
  .execute(
    new And().Builder()
      .select(0)
      .execute(
        new And.Builder()
          .select(0)
          .execute(new IsMoreThan(5, true))
          .select(0)
          .execute(new IsLessThan(10, true))
          .build())
      .select(1)
      .execute(
        new Not(
          new And.Builder()
            .select(0)
            .execute(new IsMoreThan(5, true))
            .select(0)
            .execute(new IsLessThan(10, true))
            .build()))
      .build())
  .build())
```
These two queries run separately with the results merged will get the answer much quicker as it can select only the relevant files to apply the filter too.

## Troubleshooting

When trying to filter a column you get `store.schema.ViewValidator ERROR  - No class type found for transient property HLLP.cardinality. Please ensure it is defined in the view.` If the column you are filtering on is actually a Gaffer column split into many columns or nested columns then your `View` will need to specify the column as a transient property.

For example to filter the "HLLP" property of type `HyperLogLogPlus` which has been serialised using the `InLineHyperLogLogPlusParquetSerialiser`:
```
View view = new View.Builder()
                .entity("BasicEntity",
                        new ViewElementDefinition.Builder()
                        .preAggregationFilter(
                            new ElementFilter.Builder()
                            .select("HLLP_cardinality")
                            .execute(new IsMoreThan(2L, false))
                            .build())
                        .transientProperty("HLLP_cardinality", Long.class)
                        .build())
                .build();
```
If you had used the `NestedHyperLogLogPlusParquetSerialiser` then you can replace the "HLLP_cardinality" with "HLLP.cardinality", where the "." indicates nested columns.

## Implementation details

This section contains brief details on the implementation of the `ParquetStore`.

### Graph folder structure

If the property `parquet.data.dir` is `mygraph` and the schema contains an `Entity` group called "BasicEntity" and an `Edge` group called "BasicEdge", then the folder structure will be:

```
mygraph
   |--snapshot=1234567890
      |--graphPartitioner
          |--graph
             |--group=BasicEntity
                |--partition-0000000.parquet
                |--partition-0000001.parquet
                |-- ...
             |--group=BasicEdge
                |--partition-0000000.parquet
                |--partition-0000001.parquet
                |-- ...            
          |--reversedEdges
             |--group=BasicEdge
                |--partition-0000000.parquet
                |--partition-0000001.parquet
                |-- ...
   |--snapshot=1234568888
      |--graphPartitioner
          |--graph
             |--group=BasicEntity
                |--partition-0000000.parquet
                |--partition-0000001.parquet
                |-- ...
             |--group=BasicEdge
                |--partition-0000000.parquet
                |--partition-0000001.parquet
                |-- ...            
          |--reversedEdges
             |--group=BasicEdge
                |--partition-0000000.parquet
                |--partition-0000001.parquet
                |-- ...
```

Each time new data is added to the graph, a new snapshot directory is created containing the entire graph, i.e. the existing data and the new data are merged together to form the new graph. Old snapshot directories are not deleted automatically because this allows queries that are in progress whilst data is being added to continue to work even after the add operation has completed.

Within a snapshot directory, there are two directories (called `graph` and `reversedEdges`) and a file (called `graphPartitioner`). The file contains the serialised `GraphPartitioner` object. This stores information about how data is partitioned across the files within the subdirectories. The directory `graph` contains a subdirectory for each group in the schema. Within the subdirectory for a group there are Parquet files containing elements of that group. If it's an entity group then the files are globally sorted by the vertex. If it's an edge group then the files are globally sorted by the source vertex. The `graphPartitioner` contains details of the boundaries between the files. The directory `reversedEdges` contains a subdirectory for each edge group. This contains Parquet files containing elements of that edge group globally sorted by the destination vertex.

###Partitioning strategy

The `ParquetStore` uses sorting and partitioning to allow it to quickly retrieve elements involving particular vertices.

For entity groups, the partitioning uses the vertex and the sorting uses the vertex and group-by properties. This means that all entities involving a vertex are in the same partition. Thus in order to retrieve the entities for a vertex, only one partition needs to be read (each partition corresponds to a file). The entities are globally sorted by vertex, i.e. the partitions can be ordered so that all elements in one partition have vertex less than all elements in the next partition. Having all entities involving a vertex in the same partition also allows the potential for query-time aggregation without having to merge data from different partitions. 

For edge groups, the partitioning uses the source and destination vertices and the directed field, i.e. everything with the same source, destination and directed value will be in the same partition. The data is sorted by the source, destination, directed flag and the group-by columns (in the reversedEdges folder, the elements are sorted by destination, source and directed flag).

Note that sorting the data by the source, destination, directed flag and the group-by columns could cause two edges which are identical apart from the group-by properties to appear in different partitions. This would make it difficult to perform query-time aggregation.

This is implemented in the `SortFullGroup` class by partitioning the data and then sorting within partitions (rather than just a global sort). The boundary points for the partitioning are estimated by taking a sample of the data and sorting those.

Parquet files contain statistics that record the minimum and maximum values of the columns for each row group. As entity groups are sorted by vertex within a partition the statistics allow row groups that do not contain data relevant to a query to be skipped. This means that only a small amount of data from within a file needs to be read in order to find all elements for a given vertex.

Note that the partitioning for edge groups described above means that the edges of a vertex with very high degree may be split across multiple partitions. If the partitioning approach simply partitioned edges by source vertex then partitions containing a high degree vertex may be very large relatively to other partitions.

###Conversion between Gaffer elements and Parquet

To store Gaffer elements in Parquet files, it is necessary to convert a Gaffer schema into a Parquet schema and to convert Gaffer elements into rows containing fields of Parquet types.

The `SchemaUtils` class is used to convert a Gaffer `Schema` into multiple Parquet `MessageType`s, one per group. The `MessageType` for a group is built-up by converting the type of each core property (i.e. vertex in the case of an Entity group and source, destination and directed flag in the case of an Edge group) and property into a Parquet type. The serialiser for a property is used to determine the Parquet type. If the serialiser is not an instance of a `ParquetSerialiser` then the Parquet type is binary, and the object will be serialised into a byte array. If the serialiser is an instance of a `ParquetSerialiser` then the `getParquetSchema` method on the serialiser is used to determine the Parquet schema. For example the `LongParquetSerialiser` specifies the Parquet schema for a long to be `optional int64` with name equal to the property name.

Elements are converted into Parquet rows in the class `GafferGroupObjectConverter`. The `gafferObjectToParquetObjects` method in this class is used to convert a value of a Gaffer vertex or property to the corresponding object or objects suitable for use in Parquet.

###Add elements operation

The `AddElements` operation adds an iterable of `Element`s to the store. The iterable of `Element`s needs to be fully consumed before any of the items are added (this contrasts with other stores such as the Accumulo store which can continually add elements from a never-ending iterable and make elements available for query very quickly after they have been removed from the iterable).

The new elements and the old elements are merged together to form a new copy of the graph in a new snapshot directory. This new snapshot directory is created atomically when all the necessary work has been done to produce the new version of the graph with the new data added. This means that whilst the elements are being added the graph is still available for query from the current snapshot.

The `AddElement`s operation is provided for completeness. It is envisaged that data will be imported using a bulk import option such as `ImportRDDOfElements`. Note that concurrent add operations are not supported.

The `AddElement`s operation writes out the new data split into the same partitions as the current data. The new data and old data are then read and aggregated and sorted. They are then put into a temporary directory which is atomically moved to a new snapshot directory. In more detail the steps are as follows:

1. Create a temporary directory (as specified by the `parquet.temp_data.dir` property).
2. Write new data split by group and partition (using the existing partitioner) into the temporary directory. This is done using the `WriteUnsortedData` function.
3. For every group and partition, the new data is aggregated with the old data and then sorted (using the AggregateAndSortData function).
4. For every edge group, the new data and old data are aggregated together and sorted by destination, source, etc, and then put into a reversed edges subdirectory of the temporary directory.
5. Move the results into the correct directory structure in the temporary directory.
6. Move the temporary directory to a new snapshot directory.
7. Update the snapshot value on the store to the new value.

###Import RDD of elements operation

The `ImportRDDOfElements` operation imports an `RDD` of `Element`s to the graph. As with the `AddElements` operation the import process creates a new copy of the graph containing the old and new data merged together. The import process again uses a temporary directory whilst it is producing the new graph and the current snapshot directory is available for query during this process. 

The steps are as follows:

1. Each partition of the `RDD` of `Element`s to be imported is processed in parallel. The elements of each partition are written to files, split by group. The elements are written in the order they are provided with no sorting or aggregation. Thus at the end of this step we have one file per group per input partition of the new data.
2. For each group that requires aggregation, the new data and the old data are aggregated together. This uses the `AggregateDataForGroup` class.
3. The elements in each group are now sorted (as described in the partitioning section).
4. The elements in edge groups are now sorted by destination and source and stored in a reversedEdges subdirectory of the temporary directory.
5. A new partitioner is calculated and written to disk.
6. A new snapshot directory is created within the temporary directory. This is then atomically renamed to a new snapshot directory within the main data directory. This atomic renaming is intended to ensure that a query against the graph does not see a half-full snapshot directory (which might happen if the files were moved into the directory).
7. The temporary directory is then deleted.

###Get elements operation

At a high level, the `GetElementsHandler` uses the `QueryGenerator` class to turn the operation into a `ParquetQuery` which contains a list of `ParquetFileQuery`s. Each `ParquetFileQuery` is used to create a `RetrieveElementsFromFile` object which opens a Parquet file, retrieves the needed elements and adds them to a queue.

Given a `GetElements` operation, the `QueryGenerator` uses the operation's view to identify groups that should appear in the results. For each such group, a Parquet `FilterPredicate` is created containing all filters from the view that can be implemented natively within Parquet. For example, a filter such as count > 10 can be specified to a Parquet reader, which can use that to reduce the amount of data read from disk (this can make the operation significantly more performant than if the filtering was done after all the data in the Parquet file was read from disk and converted into Elements). The `QueryGenerator` converts the seeds from the `GetElements` operation into `ParquetElementSeed`s. The `GraphPartitioner` is then used to identify which files contain information about the seeds. For each of these files, the relevant seeds are converted into `FilterPredicate`s which are joined with the `FilterPredicate` from the view. These are then used to create a `ParquetFileQuery`. These `ParquetFileQuery`s are then added to a `ParquetQuery`.

Each `ParquetFileQuery` is used to create a `RetrieveElementsFromFile` which opens a Parquet file with the necessary filters, converts the rows back to `Element`s and applies any further filters that cannot be directly applied within the Parquet file reader.
