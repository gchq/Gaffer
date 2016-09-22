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

But before that you will need to download the code and compile it - sorry we are not yet publishing our artifacts to maven central.

Start by cloning the gaffer GitHub project.

```bash
git clone https://github.com/GovernmentCommunicationsHeadquarters/Gaffer.git
```

Then from inside the Gaffer folder run the maven 'quick' build. There are quite a few dependencies to download so it is not actually that quick.

```bash
mvn clean install -Pquick
```

We've written some ${EXAMPLES_LINK} to show you how to get started.

