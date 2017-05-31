## Introduction 

Gaffer is built for very large graphs.

It's designed to be as flexible, scalable and extensible as possible, allowing for rapid prototyping and transition to production systems.

Gaffer does 

 - rapid query across very large numbers of entities and relationships,
 - versatile query-time summarisation, filtering and transformation of data,
 - in-database aggregation of rich statistical properties describing entities and relationships,
 - scalable ingest at very high data rates and volumes,
 - automated, rule-based data purge,
 - fine grained data access and query execution controls.

Gaffer can be run on various databases, including Accumulo and HBase. It is also integrated with Spark for fast and flexible data analysis.

Gaffer allows you to take data, convert it into a graph, store it in a database and then run queries and analytics on it.

To do this you need to do a few things:
 - Choose a database - called the Gaffer ${STORE_JAVADOC}. We've provided a few for you and in the following examples we'll be using the ${MOCK_ACCUMULO_STORE_JAVADOC}. The MockAccumuloStore behaves the same as the full ${ACCUMULO_STORE_JAVADOC} but means that you can run the code on your local machine in memory without having to have a full Accumulo cluster.
 - Write a ${SCHEMA_JAVADOC}. This is a JSON document that describes your graph and is made up of 3 parts:
   - DataSchema - the Elements (Edges and Entities) in your Graph; what classes represent your vertices (nodes), what ${PROPERTIES_JAVADOC} your Elements have and so on.
   - DataTypes - list of all the data types that are used in your data schema. For each type it defines the java class and a list of validation functions.
   - StoreTypes - describes how your data types are mapped into your Store, how they are aggregated and how they are serialised.
 - Write an ${ELEMENT_GENERATOR_JAVADOC} to convert your data into Gaffer Graph ${ELEMENT_JAVADOC}. We've provided some interfaces for you.
 - Write a Store Properties file. This contains information and settings about the specific instance of your store, for example hostnames, ports and so on.

When you've done these things you can write applications to load and query the data.

Gaffer is hosted on [Maven Central](https://mvnrepository.com/search?q=uk.gov.gchq.gaffer) and can easily be incorporated into your own projects.

To use Gaffer the only required dependencies are the Gaffer graph module and a store module which corresponds to the data storage framework to utilise:

```
<dependency>
    <groupId>uk.gov.gchq.gaffer.core</groupId>
    <artifactId>graph</artifactId>
    <version>[gaffer.version]</version>
</dependency>
<dependency>
    <groupId>uk.gov.gchq.gaffer</groupId>
    <artifactId>accumulo-store</artifactId>
    <version>[gaffer.version]</version>
</dependency>
```

This will include all other mandatory dependencies. Other (optional) components can be added to your project as required.

Alternatively, you can download the code and compile it yourself:

Start by cloning the gaffer GitHub project.

```bash
git clone https://github.com/gchq/Gaffer.git
```

Then from inside the Gaffer folder run the maven 'quick' profile. There are quite a few dependencies to download so it is not actually that quick.

```bash
mvn clean install -Pquick
```


We've written some ${EXAMPLES_LINK} to show you how to get started.

## Running the Examples

The section below provides an overview of a number of examples of working with a Gaffer graph, including a description of the input and output at each stage.

Each example is backed by an example java class containing a main method that can be invoked by your IDE or using the doc-jar-with-dependencies.jar.

You can download the doc-jar-with-dependencies.jar from [maven central](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22uk.gov.gchq.gaffer%22%20AND%20a%3A%22doc%22). Select the latest version and download the jar-with-dependencies.jar file.
Alternatively you can compile the code yourself by running a "mvn clean install -Pquick". The doc-jar-with-dependencies.jar file will be located here: doc/target/doc-jar-with-dependencies.jar.

```bash
# Replace <TheBasics> with your example name.
java -cp doc-jar-with-dependencies.jar uk.gov.gchq.gaffer.doc.user.walkthrough.TheBasics
```