${HEADER}

${CODE_LINK}

We'll walk through it in some detail.

First, let’s do the most basic thing; take some data from a csv file, load it into a Gaffer graph and then run a very simple query to return some graph edges. 
We'll also look at specific examples of an ${ELEMENT_GENERATOR_JAVADOC} and ${SCHEMA_JAVADOC}.

We are going to base the following walkthroughs on Road Traffic data, a simplified version of the ${ROAD_TRAFFIC_EXAMPLE_LINK}. 
Throughout these walkthroughs we will gradually build up the graph, so as we learn about new features we will add them to our Graph schema. 

This is the data we will use, a simple csv in the format road,junctionA,junctionB.
${DATA}

The first thing to do when creating a Gaffer Graph is to model your data and write your Gaffer Schema. 
When modelling data for Gaffer you really need to work out what questions you want to ask on the data. 
Gaffer queries are seeded by the vertices, essentially the vertices are indexed. 
So in this example we want to be able to ask questions like, how many vehicles have travelled from junction 10 to junction 11. So junction needs to be a vertex in the Graph.

So the Graph will look something like this (sorry it is not particularly exciting at this point). The number on the edge is the edge count property:

```
    --3->
10         11
    <-1--
 

23  --2->  24
    
    
27  <-1--  28
```

The Schema file can be broken down into small parts, we encourage at least 3 files:

- ${DATA_SCHEMA_LINK}
- ${DATA_TYPES_LINK}
- ${STORE_TYPES_LINK}

Splitting the schema up into these 3 files helps to illustrate the different roles that the schemas fulfil.

##### The DataSchema

The DataSchema is a JSON document that describes the Elements (Edges and Entities) in the Graph. We will start by using this very basic schema:

${DATA_SCHEMA_JSON}

We have one Edge Group, `"RoadUse"`. The Group simply labels a particular type of Edge defined by its vertex types, directed flag and set of properties.

This edge is a directed edge representing vehicles moving from junction A to junction B.

You can see the `“RoadUse”` Edge has a source and a destination vertex of type `"junction"` and a single property called `"count"` of type `"count.long"`. 
These types are defined in the DataType file.

##### The DataTypes

The DataTypes is a JSON document that describes the types of objects used by Elements

${DATA_TYPES_JSON}

First we'll look at `"junction"`, a road junction represented by a String. You can see it just has 2 fields, a description and the java class of the type.

The property `"count"` on the `"RoadUse"` Edges is of type `"count.long"`. The definition here says that any object of type `"count.long"` is a an long that must be greater than or equal to 0. This time we have added a validator that mandates that the count object's value must be greater than or equal to 0. If we have a `"RoadUse"` Edge with a count that's not a Long or is an Long but has a value less than 0 it will fail validation and won't be added to the Graph. 
Gaffer validation is done using [Java Predicates](https://docs.oracle.com/javase/8/docs/api/java/util/function/Predicate.html)

##### The StoreTypes

The next file used to instantiate our Graph is StoreTypes. For this example it is quite simple:
${STORE_TYPES_JSON}

The StoreTypes file is specific to a particular Store. In our example we are using an in-memory 'Mock' Accumulo Store. In simple terms, data in Accumulo is stored in rows as keys and values where the key has multiple parts.
It describes how the data types are mapped into the database that backs the Gaffer Store you've chosen.

In our StoreTypes file we supply an ${SUM_JAVADOC} [BinaryOperator](https://docs.oracle.com/javase/8/docs/api/java/util/function/BinaryOperator.html) to aggregate the count.long type. 
Gaffer allows Edges of the same Group to be aggregated together. This means that when different vehicles travel from junction 10 to junction 11 the edges will be aggregated together and the count property will represent the total number of vehicles that have travelled between the 2 junctions. 


#### Generating Graph Elements

So now we have modelled our data we need to write an Element Generator, an implementation of a [Java Function](https://docs.oracle.com/javase/8/docs/api/java/util/function/Function.html) to convert each line of our csv file to a RoadUse edge.

Here is our simple Element Generator.

${ELEMENT_GENERATOR_JAVA}

The `_apply` method takes a line from the data file as a String and returns a Gaffer ${EDGE_JAVADOC}.

First we take a line from the file as a String and split on `","` to get 3 Strings: (Road,JunctionA,JunctionB).
Then we create a new Edge object with a group `"RoadUse"`. 
We set the source vertex of the Edge to be the first junction and the destination vertex to be the second junction.
Next we set the Edge's directed flag to true, indicating that in this case our Edges are directed.
Finally we add a property called 'count' to our Edge. This is an integer and is going to represent how many vehicles travel between junctionA and junctionB. We're initialising this to be 1, as a single line in our csv data represents 1 vehicle.

In a later dev.walkthrough you will see there is a quick way of adding data to Gaffer directly from CSVs but for now we will do it in 2 steps. First we convert the csv to Edges.

${GENERATE_SNIPPET}

This produces these edges:

```
${GENERATED_EDGES}
```

#### Creating a Graph Object

The next thing we do is create an instance of a Gaffer ${GRAPH_JAVADOC}, this is basically just a proxy for your chosen Gaffer Store.

${GRAPH_SNIPPET}

To do this we provide the schema files and a Store Properties file: ${STORE_PROPERTIES_LINK}.

##### The StoreProperties

Here is the StoreProperties file:

${STORE_PROPERTIES}

This contains information specific to the actual instance of the Store you are using. Refer to the documentation for your chosen store for the configurable properties, e.g ${ACCUMULO_USER_GUIDE}.
The important property is 'gaffer.store.class' this tells Gaffer the type of store you wish to use to store your data. 

#### Loading and Querying Data

Now we've generated some Graph Edges and created a Graph, let's put the Edges in the Graph.

Start by creating a user:

${USER_SNIPPET}

Then using that user we can add the elements:

${ADD_SNIPPET}

To do anything with the Elements in a Gaffer Graph we use an ${OPERATION_JAVADOC}. In this case our Operation is ${ADD_ELEMENTS_JAVADOC}.

Finally, we run a query to return all Edges in our Graph that contain the vertex "10". To do this we use a ${GET_ELEMENTS_JAVADOC} Operation.

${GET_SNIPPET}

#### Summary

In this example we've taken some simple pairs of integers in a file and, using a ElementGenerator, converted them into Gaffer Graph Edges with a `”count”` property.

Then we loaded the Edges into a Gaffer Graph backed by a MockAccumuloStore and returned only the Edges containing the Vertex `”10”`. In our DataSchema we specified that we should sum the "count" property on Edges of the same Group between the same pair of Vertices. We get the following Edges returned, with their "counts" summed:

```
${GET_ELEMENTS_RESULT}
```