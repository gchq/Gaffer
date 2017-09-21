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

The master copy of this page is the README in the hbase-store module.

HBase Store
===================

1. [Introduction](#introduction)
2. [Use cases](#use-cases)
3. [HBase Set Up](#hbase-set-up)
4. [Properties file](#properties-file)
5. [Schema](#schema)
6. [Inserting data](#inserting-data)
7. [Queries](#queries)
8. [Visibility](#visibility)
9. [Timestamp](#timestamp)
10. [Validation and age-off of data](#validation-and-age-off-of-data)
13. [Implementation details](#implementation-details)
14. [Migration](#migration)

Introduction
-----------------------------------------------

This is a Gaffer Store implementation using Apache HBase.

Please note that this HBase Store has not been extensively tested.

This store implements the following Gaffer features:
- Visibility - user-defined visibility settings to prevent authorised access to records which a user does not have permissions to see.
  Implemented using the HBase Cell Visibility Labels.
- Pre Aggregation Filtering - predicates are pushed down to each HBase region servers to distribute the computations.
- Post Aggregation Filtering - again the predicates are pushed down to each HBase region servers to distribute the computations.
- Post Transformation Filtering - predicates are executed on a single node in the cluster.
- Transformation - transformations are applied to elements on a single node in the cluster.
- Store Aggregation - similar elements are summarised in the background using HBase's coprocessors.
- Query Aggregation - similar elements are further summarised at query time using HBase's coprocessors,
- Store Validation - HBase's coprocessors are used to validate the elements in the background so ensure old/invalid data is deleted.


This Gaffer store implementation is very similar to the Accumulo Store. 
One main difference is that due to constraints with HBase's column families the Gaffer group is store at the beginning of the column qualifier - this means filtering on groups is not as efficient as in HBase.
Please note that currently this store does not implement some of the advanced operations provided in the Accumulo store.

- Scalability to large volumes of data;
- The ability to store any properties (subject to serialisers being provided if Gaffer is not aware of the objects);
- User-configured persistent aggregation of properties for the same vertices and edges;
- Flexible query-time filtering, aggregation and transformation;
- Multiple versions of Gaffer can be run on the same HBase cluster thanks to the the ability to reference the required Gaffer jar for the coprocessors on each table. 


Use cases
-----------------------------------------------

Gaffer's `HBaseStore` is particularly well-suited to graphs where the properties on vertices and edges are formed by aggregating interactions over time windows. For example, suppose that we want to produce daily summaries of the interactions between vertices, e.g. on January 1st 2016, 25 interactions between A and B were observed, on January 2nd, 10 interactions between A and B were observed. Gaffer allows this data to be continually updated (e.g. if a new interaction between A and B is observed on January 2nd then an edge for January 2nd between A and B with a count of 1 can be inserted and this will automatically be merged with the existing edge and the count updated). This ability to update the properties without having to perform a query, then an update, then a put, is important for scaling to large volumes.

HBase set up
-----------------------------------------------

HBase provides a standalone mode for running HBase on a single machine, see [https://hbase.apache.org](https://hbase.apache.org).

Once you have HBase running (either standalone or distributed) you will need to add the hbase-store-[version]-deploy.jar to the cluster (either local file system or hdfs) for HBase's coprocessors to use. You can download this file from [maven central](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22uk.gov.gchq.gaffer%22%20AND%20a%3A%22hbase-store%22).

In addition, if you are using custom serialisers, properties or functions then these will need to be bundled together with the hbase-store-[version]-deploy.jar before being added to the cluster.


The next step is to write a store.properties file.

Properties file
-----------------------------------------------

The next stage is to create a properties file that Gaffer will use to instantiate a connection to your HBase cluster. This requires the following properties:

```properties
gaffer.store.class=uk.gov.gchq.gaffer.hbasestore.HBaseStore
gaffer.store.properties.class=uk.gov.gchq.gaffer.hbasestore.HBaseProperties

# A comma separated list of zookeepers
# In AWS it will just be something like: ec2-xx-xx-xx-xx.location.compute.amazonaws.com
hbase.zookeepers=localhost:2181

# Add the hbase-store-[version]-deploy.jar to the cluster - either local file system or hdfs.
# e.g /user/hbase/gaffer/jars/hbase-store-0.7.0-deploy.jar
hbase.hdfs.jars.path=[path to jar folder]/hbase-store-[version]-deploy.jar
```

Schema
-----------------------------------------------

See [Getting started](https://gchq.github.io/gaffer-doc/getting-started/dev-guide.html#schemas) for details of how to write a schema that tells Gaffer what data will be stored, and how to aggregate it. Once the schema has been created, a `Graph` object can be created using:

```java
Graph graph = new Graph.Builder()
      .config(new GraphConfig.Builder()
          .graphId(uniqueNameOfYourGraph)
          .build())
      .addSchemas(schemas)
      .storeProperties(storeProperties)
      .build();
```


Inserting data
-----------------------------------------------

There are two ways of inserting data into a Gaffer `HBaseStore`: continuous load, and bulk import. To ingest large volumes of data, it is recommended to first set appropriate split points on the table, otherwise loading data may take significantly longer (but it will still work). The split points represent how HBase partitions the data into multiple tablets.

**Continuous load**

This is done by using the `AddElements` operation and is as simple as the following where `input` is a Java `Iterable` of Gaffer `Element`s that match the schema specified when the graph was created:

```java
AddElements addElements = new AddElements.Builder()
        .input(elements)
        .build();
graph.execute(addElements, new User());
```

Note that here `elements` could be a never-ending stream of `Element`s and the above command will continuously ingest the data until it is cancelled or the stream stops.

IMPORTANT - due to the way elements are inserted into HBase we need to aggregate elements within each batch before adding them to HBase to avoid them being skipped.
Therefore optimising the batch size could have a big impact on performance. Configure the batch size using store property: hbase.writeBufferSize
If your schema does not have aggregation then elements with the same key (group, vertex, source, destination, direction) in the same batch will require the batch to flushed multiple times to avoid losing elements and this will have a large impact on ingest rates. If this happens you will need to consider creating your own batches with distinct elements or using AddElementsFromHdfs.

**Bulk import**

To ingest data via bulk import, a MapReduce job is used to convert your data into files of HBase key-value pairs that are pre-sorted to match the distribution of data in HBase. Once these files are created, HBase moves them from their current location in HDFS to the correct directory within HBase's data directory. The data in them is then available for query immediately.

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
- `failureDir` is a string specifying the directory in HDFS which HBase will use to store files that were not successfully imported;
- `myMapperGeneratorClass` is a `Class` that implements the `MapperGenerator` interface. This is used to generate a `Mapper` class that is used to convert your data into `Element`s. Gaffer contains two built-in generators: `TextMapperGenerator` and `AvroMapperGenerator`. The former requires your data to be stored in text files in HDFS; the latter requires your data to be stored in Avro files;
- `jobInitialiser` is an instance of the `JobInitialiser` interface that is used to initialise the MapReduce job. If your data is in text files then you can use the built-in `TextJobInitialiser`. An `AvroJobInitialiser` is also provided.

The operation can then be executed as normal using:

```java
graph.execute(addElementsFromHdfs, new User());
```

However, note you will need to create a Java jar file with dependencies that contains a main method that executes the addElementsFromHdfs. This jar must then be executed using the `hadoop` command to ensure that the Hadoop configuration is available.


Queries
-----------------------------------------------

The HBase store supports all the standard queries. See [Getting Started](https://gchq.github.io/gaffer-doc/summaries/getting-started.html) for more details or the [Operation examples](https://gchq.github.io/gaffer-doc/getting-started/operation-examples.html).

Visibility
-----------------------------------------------

Gaffer can take advantage of HBase's built-in fine-grained security to ensure that users only see data that they have authorisation to. This is done by specifying a "visibilityProperty" in the schema. This is a string that tells HBase which key in the `Properties` on each `Element` should be used for the visibility. The HBase Cell Visibility will be set to this value for each element. This means that a user must have authorisations corresponding to that visibility in order to see the data.

If no "visibilityProperty" is specified then the column visibility is empty which means that anyone who has read access to the table can view it.

See [the aggregation example](https://gchq.github.io/gaffer-doc/getting-started/user-guide.html#aggregation) in the [user guide](https://gchq.github.io/gaffer-doc/getting-started/user-guide.html) for an example of how properties can be aggregated over different visibilities at query time.

Timestamp
-----------------------------------------------

HBase keys have a timestamp field. The user can specify which property is used for this by setting "timestampProperty" in the schema to the name of the property. If this is not specified then the time when the conversion to an HBase Cell happens is used.

Validation and age-off of data
-----------------------------------------------

In production systems where data is continually being ingested, it is necessary to periodically remove data so that the total size of the graph does not become too large. The `HBaseStore` allows the creator of the graph to specify custom logic to decide what data should be removed. This logic is applied during compactions, so that the data is permanently deleted. It is also applied during queries so that even if a compaction has not happened recently, the data that should be removed is still hidden from the user.

A common approach is simply to delete data that is older than a certain date. In this case, within the properties on each element there will be a time window specified. For example, the properties may contain a "day" property, and the store may be configured so that once the day is more than one year ago, it will be deleted. This can be implemented as follows:

- Each element has a property called, for example, "day", which is a `Long` which contains the start of the time window. Every time an element is observed this property is set to the previous midnight expressed in milliseconds since the epoch.
- In the schema the validation of this property is expressed as follows:
```sh
"long": {
    "class": "java.lang.Long",
    "validateFunctions": [
        {
            "class": "uk.gov.gchq.gaffer.types.function.function.simple.filter.AgeOff",
            "ageOffDays": "100"
        }
    ]
}
```
- Then data will be aged-off whenever it is more than 100 days old.


Implementation details
-----------------------------------------------

**Table design**

Gaffer`Entity`s are converted into a single HBase row and an `Edge` into 2 rows. The row ID of the `Entity` is the vertex serialised to a byte array, followed by a flag to indicate that this is an `Entity`. This allows the `Entity`s associated to a vertex to be quickly retrieved. It is necessary to store each `Edge` as two rows so that it can found from both the source vertex and the destination vertex: one row has a row ID consisting of the source vertex serialised to a byte array, followed by a delimiter, followed by the destination vertex serialised to a byte array; the other key-value has the opposite, with the destination vertex followed by the source vertex. A flag is also stored to indicate which of these two versions the key is so that the original `Edge` can be recreated.

An important feature of the row IDs created by both key-packages is that it is possible to create ranges of keys that either only contain the `Entity`s or only contain the `Edge`s or contain both. This means that if, for example, a user states that they only want to retrieve the `Entity`s for a particular vertex then only relevant key-value pairs need to be read. In the case of a high-degree vertex, this means that queries for just the `Entity`s will still be very quick.

An `Entity` is store in HBase as follows:

<table>
<tr>
    <th>Row ID</th>
    <th>Column Family</th>
    <th>Column Qualifier</th>
    <th>Cell Visibility</th>
    <th>Timestamp</th>
    <th>Value</th>
</tr>
<tr>
    <td>(serialised_vertex)01</td>
    <td>e</td>
    <td>group and group by properties</td>
    <td>visibility property</td>
    <td>timestamp</td>
    <td>the visibility property (again) and all other properties</td>
</tr>
</table>

In the row ID the 0 is a delimiter to split the serialised vertex from the 1. The 1 indicates that this is an `Entity`. By having this flag at the end of the row id it is easy to determine if the key relates to an `Entity` or an `Edge`.

The following HBase key-value pairs are created for an `Edge`:

<table>
<tr>
    <th>Row ID</th>
    <th>Column Family</th>
    <th>Column Qualifier</th>
    <th>Cell Visibility</th>
    <th>Timestamp</th>
    <th>Value</th>
</tr>
<tr>
    <td>(serialised_source_vertex)0x0(serialised_destination_vertex)0x</td>
    <td>e</td>
    <td>group and group by properties</td>
    <td>visibility property</td>
    <td>timestamp</td>
    <td>the visibility property (again) and all other properties</td>
</tr>
<tr>
    <td>(serialised_destination_vertex)0y0(serialised_source_vertex)0y</td>
    <td>e</td>
    <td>group and group by properties</td>
    <td>visibility property</td>
    <td>timestamp</td>
    <td>the visibility property (again) and all other properties</td>
</tr>
</table>

If the `Edge` is undirected then both `x` and `y` are 4. If the `Edge` is directed then `x` is 2 and `y` is 3.

The flag is repeated twice to allow filters that need to know whether the key corresponds to a `Entity` or an `Edge` to avoid having to fully deserialise the row ID. For a query such as find all out-going edges from this vertex, the flag that is directly after the source vertex can be used to restrict the range of row IDs queried for.

## Migration

The HBase Store also provides a utility [TableUtils](https://github.com/gchq/Gaffer/blob/master/store-implementation/hbase-store/src/main/java/uk/gov/gchq/gaffer/hbasestore/utils/TableUtils.java)
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
- reorder any of the properties. In the HBase store we don't use any property names, we just rely on the order the properties are defined in the schema.
- change the way properties or vertices are serialised - i.e don't change the serialisers.
- change which properties are groupBy

Please note, that the validation functions in the schema can be a bit dangerous. 
If an element is found to be invalid then the element will be permanently deleted from the table. 
So, be very careful when making changes to your schema that you don't accidentally make all your existing elements invalid as you will quickly realise all your data has been deleted. 
For example, if you add a new property 'prop1' and set the validateFunctions to be a single Exists predicate. 
Then when that Exists predicate is applied to all of your existing elements, those old elements will fail validation and be removed.

To carry out the migration you will need the following:

- your schema in 1 or more json files.
- store.properties file contain your HBase store properties
- a jar-with-dependencies containing the HBase Store classes and any of your custom classes. 
If you don't have any custom classes then you can just use the hbase-store-[version]-utility.jar. 
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


If you have existing data, then before doing any form of upgrade or change to your table we strongly recommend cloning the table so you have a backup so you can easily restore to that version if things go wrong. 
Even if you have an error like the one above where all your data is deleted in your table you will still be able to quickly revert back to your backup. 
Cloning a table in HBase is very simple and fast (it doesn't actually copy the data).


You will need to run the TableUtils utility providing it with your graphId, schema and store properties.
If you run it without any arguments it will tell you how to use it.

```bash
java -cp [path to your jar-with-dependencies].jar uk.gov.gchq.gaffer.hbasestore.utils.TableUtils
```