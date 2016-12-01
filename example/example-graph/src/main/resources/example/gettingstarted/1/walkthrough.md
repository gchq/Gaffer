${HEADER}

${CODE_LINK}

We'll walk through it in some detail.

First, let’s do the most basic thing; take some data from a file, load it into a ${MOCK_ACCUMULO_STORE_JAVADOC} and then run a very simple query to return some graph edges that contain specific vertices. We'll also look at specific examples of the ${ELEMENT_GENERATOR_JAVADOC} and ${SCHEMA_JAVADOC} we introduced in the previous section.

${DATA}

#### Generating Graph Elements

We have written a simple data generator to convert a line in a the data file to a Gaffer Edge:

${DATA_GENERATOR_JAVA}

The `getElement` method takes a line from the data file as a String and returns a Gaffer ${EDGE_JAVADOC}.

We can do the following to take our integer pairs from csv and create Edges from them.

${GENERATE_SNIPPET}

First we take a line from the file as a String and split on `","` to get two Strings, one for each of the integers.
Then we create a new Edge object with a label `"data"`. This label is the Edge's `Group` (more on what the `Group` is a bit later in this section).

We set the source vertex of the Edge to be the first integer in the pair and the destination vertex to be the second integer in the pair.

Next we set the Edge's directed flag to false, indicating that in this case our Edges aren't directed.

Finally we add a property called 'count' to our Edge. This is an integer and is going to represent how many times we've seen a particular Edge between two vertices. We're initialising this to be 1.

Now if you run the example you'll see that the first thing that's printed out is the individual Edges generated from the data file.

```
${GENERATED_EDGES}
```

#### Creating a Graph Object

The next thing we do is create a Gaffer ${GRAPH_JAVADOC}.

${GRAPH_SNIPPET}

To do this we reference 4 files:

- ${DATA_SCHEMA_LINK}
- ${DATA_TYPES_LINK}
- ${STORE_TYPES_LINK}
- ${STORE_PROPERTIES_LINK}

You'll notice that the `DataSchema`, `DataTypes` and `StoreTypes` are added to the Graph using the same method, `addSchema`. They can all be combined into a single file or broken into multiple parts, each added to the Graph using the `addSchema` method. However in these examples we'll keep the logical separation into `DataSchema`, `DataTypes` and `StoreTypes` because this helps to illustrate the different roles that the schemas fulfil.

##### The DataSchema

The DataSchema is a json document that describes the Elements in the Graph.

The first section describes the Edges we just created in the DataGenerator:

${DATA_SCHEMA_JSON}

We have one Edge Group, `"data"`. The Group simply labels a particular type of Edge defined by its vertex types, directed flag and set of properties.

The `“data”` Edges have source and destination vertices of type `"vertex.string"` and a single property called `"count"` of type `"count.int"`. These types are defined in the DataType file.

##### The DataTypes

The DataTypes is a json document that describes the types of objects used by Elements

${DATA_TYPES_JSON}

First we'll look at `"vertex.string"`. This basically says that any object of type `"vertex.string"` is a Java String. The ${EXISTS_JAVADOC} says that the object must exist (i.e is not null). The validator ensures that when we load data into the Graph any `"data"` Edge where the source and destination vertices are not a non-null String will be rejected.

The property `"count"` on the `"data"` Edges is of type `"count.int"`. This says that any object of type `"count.int"` is a Java Integer. This time the validator mandates that the count object's value must be greater than or equal to 0. If we have a `"data"` Edge with a count that's not an Integer or is an Integer but has a value less than 0 it won't be loaded into the Graph.

##### The StoreTypes

The next file used to instantiate our Graph is StoreTypes. For this example it is quite simple:
${STORE_TYPES_JSON}

The StoreTypes file is specific to a particular Store. In our example we are using an in-memory 'Mock' Accumulo Store. In simple terms, data in Accumulo is stored in rows as keys and values where the key has multiple parts.
It describes how the data types are mapped into the database that backs the Gaffer Store you've chosen.

In our StoreTypes file we supply an ${AGGREGATE_FUNCTION} for the count.int type. Gaffer allows Edges of the same Group to be aggregated at query time.

If you run this example you'll see that the last thing that's printed to the console is:

```
${GET_RELATED_EDGES_RESULT}
```

You can see that the counts have been aggregated. If you look in the data file, the pair '1,2' appears 3 times. When we generated the Edges we added a count for each of these and set it to 1 so we had 3 Edges from 1 to 2, each with a count of 1.

We've used the ${SUM_JAVADOC} Aggregate Function in the DataSchema for the `”count”` property so when we query for them these three Edges with counts of 1 are returned as a single Edge with a count of 3.

##### The StoreProperties

${STORE_PROPERTIES}

Finally we have the StoreProperties. As we said previously, this contains information specific to the actual instance of the Store you are using. This is covered in more detail in the ${ACCUMULO_USER_GUIDE}.

#### Loading and Querying Data

Now we've generated some Graph Edges and created a Graph, let's put the Edges in the Graph.

Start by creating a user:

${USER_SNIPPET}

Then using that user we can add the elements:

${ADD_SNIPPET}

To do anything with the Elements in a Gaffer Graph we use an ${OPERATION_JAVADOC}. In this case our Operation is ${ADD_ELEMENTS_JAVADOC}.

Finally, we run a query to return all Edges in our Graph that contain the vertex "1". To do this we use a ${GET_RELATED_EDGES_JAVADOC} Operation.

${GET_SNIPPET}

#### Summary

In this example we've taken some simple pairs of integers in a file and, using a DataGenerator, converted them into Gaffer Graph Edges with a `”count”` property:

```
${GENERATED_EDGES}
```

Then we loaded the Edges into a Gaffer Graph backed by a MockAccumuloStore and returned only the Edges containing the Vertex `”1”`. In our DataSchema we specified that we should sum the "count" property on Edges of the same Group between the same pair of Vertices. We get the following Edges returned, with their "counts" summed:

```
${GET_RELATED_EDGES_RESULT}
```