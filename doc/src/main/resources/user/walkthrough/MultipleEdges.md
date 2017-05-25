${HEADER}

${CODE_LINK}

This time we'll see at what happens when we have more than one Edge.

#### Schemas

We first need to add an additional Edge to our Schema. We will add a RoadHasJunction Edge. This will be a simple Edge from a new vertex 'Road' to the existing 'Junction' vertex. 

In the data schema we need to define the new Road Edge:
${DATA_SCHEMA_JSON}

Here we have used a new 'road' type so we also have to define that in our data schema. It is defined to be a simple java String.

#### Element Generator

Like the previous example, we use a ElementGenerator to generate Gaffer Edges from each line of the CSV, but in now each line will generate 3 Edges. 1 RoadUse edge and 2 RoadHasJunction edges. The generator for this example is:
${ELEMENT_GENERATOR_JAVA}

Here are the generated edges:

```
${GENERATED_EDGES}
```

#### Loading and Querying the Data

We create a Graph and load the data using the ${ADD_ELEMENTS_JAVADOC} Operation exactly the same as in the previous example.

This time we'll run 2 queries using ${GET_ELEMENTS_JAVADOC}.

The first one is exactly the same as last time. We ask for all of the Edges containing the Vertex "10". The result is:

```
${GET_ELEMENTS_RESULT}
```

Our second query is to return all "RoadHasJunction" Edges. We still use the ${GET_ELEMENTS_JAVADOC} Operation like before but this time we add a ${VIEW_JAVADOC} to it:

${GET_SNIPPET}

and only get the "RoadHasJunction" Edges containing Vertex "10":

```
${GET_ROAD_HAS_JUNCTION_EDGES_RESULT}
```

We'll explore the ${VIEW_JAVADOC} in more detail over the next few examples. But for now think of it as you are requesting a particular View of you graph, filtering out unwanted elements.