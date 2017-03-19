## A *Very* Short Introduction to Gaffer

Gaffer allows you to take data, convert it into a graph, store it in a database and then run graph queries and analytics on it.

To do this you need to do a few things:
 - Choose a database - called the Gaffer ${STORE_JAVADOC} We've provided a couple for you and in the following examples we'll be using the ${MOCK_ACCUMULO_STORE_JAVADOC}. The MockAccumuloStore behaves exactly the same as the full ${ACCUMULO_STORE_JAVADOC} but means that you can run the code on your local machine without having to have a full Accumulo cluster.
 - Write an ${ELEMENT_GENERATOR_JAVADOC} to convert your data into Gaffer Graph ${ELEMENT_JAVADOC}. We've provided some interfaces for you.
 - Write a ${SCHEMA_JAVADOC}. This is a json document that describes your graph and is made up of 3 parts:
  - DataSchema - the Elements in your Graph; what classes represent your vertices, what ${PROPERTIES_JAVADOC} your Elements have and so on.
  - DataTypes - list of all the data types that are used in your data schema. For each type it defines the java class and a list of validation functions.
  - StoreTypes - describes how your data types are mapped into your Store, how they are aggregated and how they are serialised.
 - Write a Store Properties file. This contains information and settings about the specific instance of your store, for example hostnames, ports and so on.

When you've done these things you can write java applications to load and query the data.

Gaffer is hosted on [Maven Central](https://mvnrepository.com/search?q=uk.gov.gchq.gaffer) and can easily be incorporated into your own projects.

To use Gaffer the only required dependencies are the Gaffer graph module and a store module which corresponds to the data storage framework to utilise (currently limited to Apache Accumulo):

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

Alternatively, you can download the code and compile it yourself.

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

It is possible to run these examples yourself using one of two methods:

* The examples can be run directly from the IDE of your choice, or
* The examples JAR file can be compiled and the examples can be executed from the command line.

In order to run the examples from the command line, first the examples JAR must be compiled:

```bash
mvn clean install -Pquick -PexampleJar
```

The examples can then be run from the JAR, for instance

```bash
java -cp example/example-graph/target/example-jar-with-dependencies.jar uk.gov.gchq.gaffer.example.gettingstarted.analytic.LoadAndQuery1
```