Copyright 2017 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

# Parquet Store User Guide

1. [Introduction](./README.md#introduction)
2. [Use cases](./README.md#use-cases)
3. [Properties file](./README.md#properties-file)
4. [Schema](./README.md#schema)
5. [Inserting data](./README.md#inserting-data)
6. [Queries](./README.md#queries)
7. [Optimisations](README.md#optimisations)
7. [Troubleshooting](./README.md#troubleshooting)
8. [Implementation details](./README.md#implementation-details)
   - [Graph folder structure](./README.md#graph-folder-structure)
   - [High level operations process](./README.md#high-level-operations-process)

## Notice
This store is experimental, the API is unstable and may require breaking changes.

## Introduction

Gaffer contains a store implemented using [Apache Parquet](https://parquet.apache.org/) version 1.8.2. Graph elements are stored in Parquet files (typically in HDFS). This offers the following functionality:
- A scalable data store of entities and edges;
- A very compact data store using a columnar file format;
- Fast full scan and random access queries on the same store;
- Flexible query time filtering of data;
- Integration with Apache Spark to allow Gaffer data stored in Parquet to be analysed as a `Dataframe`;
- User-configured persistent aggregation of properties for the same entities and edges;
- The ability to customise how vertices and properties are converted into columns in the Parquet files (using classes that implement `ParquetSerialisation` if the default serialisers are not suitable) - both nested columns and multiple columns are supported.

## Design overview

Elements are stored in Parquet files. There is a top-level directory which contains one subdirectory per group in the schema. Within each subdirectory, the elements are stored sorted by the vertex, in the case of an entity, or by the source vertex, in the case of an edge. There is another top-level directory that contains the edges sorted by the destination vertex. Together these directories allow queries for elements involving particular vertices to quickly find the right data (only the relevant files need to be read, and within those files only blocks containing relevant data need to be read).

Each property is stored in one or more columns. Properties that are simple primitives that are natively supported by Parquet are stored as primitives (rather than serialised objects). Properties such as `HyperLogLogPlus` are stored both as a serialised byte array and as a primitive column which gives the current state of the `HyperLogLogPlus`; in this case the current estimate of the sketch is stored. The serialised form allows it to be updated with new data. The primitive column allows high-performance scans over the current value of the sketch.

When new data is added to the store, [Apache Spark](https://spark.apache.org/) is used to read, sort and aggregate both the existing data and the new data to form a new directory containing the new data. Concurrent updates are not supported. Thus this store is suited for occasional bulk updates, rather than continual updates.

## Use cases

Gaffer's `ParquetStore` is particularly well suited to graphs with lots of properties on the entities and edges where you want to perform both full table scans and random access queries. The columnar format allows excellent performance for queries such as what is the average value of the count property over all edges? In this case only the count column needs to be read - all the other properties are not read from disk.

This store can be used as a source of data for frameworks such as GraphX, e.g. the graph of all edges where the count is greater than 10 is loaded from the `ParquetStore` into memory, and then the PageRank algorithm is run using GraphX.

The `ParquetStore` could be used as part of a hybrid system where you use a Gaffer store that continually ingests new data. Overnight a snapshot of that data is written into a `ParquetStore` to benefit from the smaller storage requirements and fast full scan capabilities while maintaining fast random access.

## Properties file

The `ParquetStoreProperties` class contains all properties relating to the configuration of the `ParquetStore`. It can be created from a properties file. It has sensible defaults for all parameters but users may want to tune these properties to suit their data. The following properties can be set:

- `spark.master`: The string that sets what mode to run Spark in. By default, if Spark is installed on the machine it will use Spark's defaults, otherwise it will run in local mode using all available threads;
- `parquet.data.dir`: The file path to save the graph files under, by default this will be a relative path \<current path\>/parquet_data;
- `parquet.temp_data.dir`: The file path to save the temporary graph files under, by default this will be a relative path \<current path\>/.gaffer/temp_parquet_data. Warning: this directory will automatically be deleted at the start and end of any `AddElements` operation;
- `parquet.threadsAvailable`: The number of threads to make available to the operations to increase the parallelism, by default this is set to 3 which will provide maximum parallelism when adding a single Gaffer group;
- `parquet.add_elements.row_group.size`: This parameter sets the maximum row group size in bytes before compression for the Parquet files, see [Parquet documentation](https://parquet.apache.org/documentation/latest/) for more information. By default this is set to 4MB;
- `parquet.add_elements.page.size`: This just exposes the Parquet file format parameter controlling the maximum page and dictionary page size in bytes before compression, see [Parquet documentation](https://parquet.apache.org/documentation/latest/) for more information. By default this is set to 1MB;
- `parquet.add_elements.output_files_per_group`: This is the number of files that the output data is split into per Gaffer group. By default this is set to 10.
- `parquet.add_elements.aggregate`: This is a boolean flag of whether to aggregate the data on ingest. By default this is true.
- `parquet.add_elements.sort_by_splits`: This is a boolean flag of whether to sort the source and vertex sorted data on a per group, per split basis. By default this is false.

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
parquet.add_elements.aggregate=true
parquet.add_elements.sort_by_splits=false
```

Note that apart from the first two lines which are required by Gaffer so it knows which store to use, the rest of the lines are optional.

## Schema

See the [Getting Started](https://gchq.github.io/gaffer-doc/getting-started/dev-guide.html#schemas) for details of how to write a schema that tells Gaffer what data will be stored, and how to aggregate it. Once the schema has been created, a `Graph` object can be created using:

```
Graph graph = new Graph.Builder()
      .config(new GraphConfig.Builder()
          .graphId(uniqueNameOfYourGraph)
          .build())
      .addSchemas(schemas)
      .storeProperties(storeProperties)
      .build();
```
Note that the `ParquetStore` currently does not make use of the `timestampProperty`. Also to get the best performance you should allow Gaffer to detect the best serialiser or provide a serialiser class that implements `ParquetSerialisation`.

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
        .sparkSession(<SparkSession>)
        .build();
graph.execute(addElements, new User());
```

Inserting the data via the `ImportRDDOfElements` operation will generally be the faster of the two methods.

## Queries

The `ParquetStore` currently supports most of the [standard Gaffer queries](https://gchq.github.io/gaffer-doc/getting-started/spark-operation-examples.html).

The operations that are not currently supported are:
- `GetAdjacentEntitySeeds`
- `GetJavaRDDOfAllElements`
- `GetJavaRDDOfElements`
- `GetRDDOfAllElements`
- `GetRDDOfElements`

The current limitations on the queries are based on the Gaffer View's that you can set, see [Getting started guide](https://gchq.github.io/gaffer-doc/getting-started/user-guide.html#filtering).
Currently those limitations are:
- Query time aggregation is not supported;
- Transformations are not supported;
- Only the preAggregationFilter's will be applied;
- The best Gaffer filters to use are those listed below which translate well to Parquet filters, which means they can be pushed down to the file readers. Other filters can be used but will take longer to run:
  - `IsEqual`
  - `IsLessThan`
  - `IsMoreThan`
  - `IsTrue`
  - `IsFalse`
  - `And`
  - `Or`
  - `Not`

## Writing a custom serialiser

For the `ParquetStore` to be able to make the most out of the Parquet file format, it needs to know how to convert a Java object into primitive Java types that Parquet knows how to serialise efficiently.

A simple example which is already part of the default serialisers, is a serialiser for `Long` objects. The `ParquetSerialiser` interface has three methods that need to be implemented.

```
public interface ParquetSerialiser<INPUT> extends Serialiser<INPUT, Object[]> {

    /**
     * This method provides the user a way of specifying the Parquet schema for this object. Note that the
     * root of this schema must have the same name as the input colName.
     *
     * @param colName The column name as a String as seen in the Gaffer schema that this object is from
     * @return A String representation of the part of the Parquet schema that this object will be stored as
     */
    String getParquetSchema(final String colName);

    /**
     * This method provides the user a way of specifying how to convert a Plain Old Java Object (POJO)
     * into the Parquet primitive types.
     *
     * @param object The POJO that you will convert to Parquet primitives
     * @return An object array of Parquet primitives, if this serialiser is used as the vertex serialiser
     * then the order of the objects will determine the sorting order
     * @throws SerialisationException If the POJO fails to be converted to ParquetObjects then this will be thrown
     */
    @Override
    Object[] serialise(final INPUT object) throws SerialisationException;

    /**
     * This method provides the user a way of specifying how to recreate the Plain Old Java Object (POJO)
     * from the Parquet primitive types.
     *
     * @param objects An object array of Parquet primitives
     * @return The POJO that you have recreated from the Parquet primitives
     * @throws SerialisationException If the ParquetObjects fails to be converted to a POJO then this will be thrown
     */
    @Override
    INPUT deserialise(final Object[] objects) throws SerialisationException;
}
```

Therefore to simply store this object as a long you would have the following class as your serialiser, where the last five methods are required by Gaffer's Serialisation interface but only the first one is used by the `ParquetStore`:

```
public class LongParquetSerialiser implements ParquetSerialiser<Long> {

    private static final long serialVersionUID = 1336116011156359680L;

    @Override
    public String getParquetSchema(final String colName) {
        return "optional int64 " + colName + ";";
    }

    @Override
    public Object[] serialise(final Long object) throws SerialisationException {
        final Object[] parquetObjects = new Object[1];
        parquetObjects[0] = object;
        return parquetObjects;
    }

    @Override
    public Long deserialise(final Object[] objects) throws SerialisationException {
        if (objects.length == 1) {
            if (objects[0] instanceof Long) {
                return (Long) objects[0];
            } else if (objects[0] == null) {
                return null;
            }
        }
        throw new SerialisationException("Could not de-serialise objects to a Long");
    }

    @Override
    public Long deserialiseEmpty() throws SerialisationException {
        throw new SerialisationException("Could not de-serialise the empty bytes to a Long");
    }

    @Override
    public boolean preservesObjectOrdering() {
        return true;
    }

    @Override
    public Object[] serialiseNull() {
        return new Object[0];
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return Long.class.equals(clazz);
    }
}
```

An example of a more complex serialiser where it is preferable to store the Java object in multiple columns is with a `HyperLogLogPlus` sketch. The reason for this is because having to deserialise the object from bytes will take longer than reading a long that represents the cardinality, however you still need to be able to get back the `HyperLogLogPlus` object to be able to aggregate the property. Therefore the `HyperLogLogPlus` sketch serialiser would look like:

```
public class HyperLogLogPlusParquetSerialiser implements ParquetSerialiser<HyperLogLogPlus> {

    private static final long serialVersionUID = -898356489062346070L;

    @Override
    public String getParquetSchema(final String colName) {
        return "optional binary " + colName + "_raw_bytes;\n" +
                "optional int64 " + colName + "_cardinality;";
    }

    @Override
    public Object[] serialise(final HyperLogLogPlus object) throws SerialisationException {
        try {
            if (object != null) {
                final Object[] parquetObjects = new Object[2];
                parquetObjects[0] = object.getBytes();
                parquetObjects[1] = object.cardinality();
                return parquetObjects;
            }
        } catch (final IOException e) {
            throw new SerialisationException("Failed to get bytes from the HyperLogLogPlus object.");
        }
        return new Comparable[0];
    }

    @Override
    public HyperLogLogPlus deserialise(final Object[] objects) throws SerialisationException {
        try {
            if (objects.length == 2) {
                if (objects[0] instanceof byte[]) {
                    return HyperLogLogPlus.Builder.build(((byte[]) objects[0]));
                } else if (objects[0] == null) {
                    return null;
                }
            }
            throw new SerialisationException("Could not de-serialise the HyperLogLogPlus object from objects");
        } catch (final IOException e) {
            throw new SerialisationException("Could not de-serialise the HyperLogLogPlus object from byte[]");
        }
    }

    @Override
    public HyperLogLogPlus deserialiseEmpty() throws SerialisationException {
        throw new SerialisationException("Could not de-serialise the empty bytes to a HyperLogLogPlus");
    }

    @Override
    public boolean preservesObjectOrdering() {
        return true;
    }

    @Override
    public Object[] serialiseNull() {
        return new Object[0];
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return HyperLogLogPlus.class.equals(clazz);
    }
}
```

We have seen that the Parquet serialiser can split an object into multiple columns. If however the serialiser is only to be used for properties on an Element, then you could simply change the Parquet schema to make it store that object in a tree structure using Parquet's nested columns. An example of this for the HyperLogLogPlus sketch is:

```
public String getParquetSchema(final String colName) {
    return "optional group " + colName + " {\n" +
            "\toptional binary raw_bytes;\n" +
            "\toptional int64 cardinality;\n" +
            "}";
}
```

It is also worth noting that the order of the `Object[]` created by the `getParquetObjectsFromPOJO` method and used by the`getPOJOFromObjects` method should match the ordering of the leaf nodes in the Parquet schema, so in the example the raw_bytes is first and the cardinality is second.

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
This would work, however the filters would have to be passed to every file for the groups that it was applied to, where as if you split the query into two queries as:
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
If you had used the `NestedHyperLogLogPlusParquetSerialiser` then you can replace the "HLLP_cardinality" with "HLLP.cardinality".

## Implementation details

This section contains brief details on the implementation of the `ParquetStore`. The first part shows the high level folder structure of how the data is stored. The second part gives the high level process that the two main operations follow.

### Graph folder structure

Assuming we had an `Entity` group called "BasicEntity" and an `Edge` group called "BasicEdge" then the folder structure would look like: 

```
parquet_data
`-- graphId
    `|-- <A long representing the time at which the data was written (as the number of milliseconds since epoch)>
        |-- graph
        |   |-- GROUP=BasicEdge
        |   |   |-- _index
        |   |   `-- part-00000.gz.parquet
        |   `-- GROUP=BasicEntity
        |       |-- _index
        |       `-- part-00000.gz.parquet
        `-- sortedBy=DESTINATION
            `-- GROUP=BasicEdge
                |-- _index
                `-- part-00000.gz.parquet
```

The root directory has two folders, one for the main graph which is what is returned when a `GetAllElements` operation is executed and the other is a sortedBy destination folder. The sortedBy destination folder is there to store all the Edge groups data again but this time the data is sorted by the destination, allowing for quick random access for seeds equal to the destination of an edge.

### High level operations process

The main two operations are the `AddElements` and the `GetElements`.

The `AddElements` operation is a six stage process:
1. Work out what the split points should be from the index or if this is the first time data is added to the graph then it will work it out from the input;
2. Write the input data split by split points, group into Parquet files in the temporary files directory using the `ParquetElementWriter`;
3. Using Spark, aggregate the data in each of the temporary files directories and the current store files on a per group, per split basis;
4. Using Spark, sort the data in each of the temporary files directories by source or vertex on a per group basis, unless the sortBySplits property is set to true in which case it will only sort within each split;
5. Using Spark, load in each of the Edge group's aggregated temporary files and sort them by destination to allow indexing by destination;
6. Generate an `GraphIndex` containing the range of vertices in each file and load that into memory.

The `GetElements` operation is a four stage process per group:
1. From the Gaffer view build up a corresponding Parquet filter;
2. For each seed, build a map from file path to Parquet filter. This is done by using the `GraphIndex` to determine which files will contain which seeds;
3. If the query has seeds then for each filter in the path to filter map add in the group filter built in the first stage;
4. Using the path to filter map build an `Iterable` that will iterate through the required files applying only the relevant filters for that file.
