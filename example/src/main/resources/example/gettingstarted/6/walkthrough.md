${HEADER}

${CODE_LINK}

In the previous examples we have had several steps to get data into Gaffer.
In this example we will show you a way of using an operation chain to both generate the elements from the data file and add them directly to the graph.

The schema is very similar to the previous examples except that the edges are now directed and the data now also contains is:

${DATA}

Operation chains are simply a list of operations in which the operations are executed sequentially, using the output from the first operation as the input to the next operation.
This allows us to simplifying the addition of elements from the data file. It can now be done as follows:

${ADD_SNIPPET}

This chain consists of 2 operations.
The first, GenerateElements, which takes the data and a data generator and generates the Gaffer edges.
The second, AddElements, simply takes the generated elements and adds them to the graph.
This operation chain can then be executed on the graph as before.


Another example of using an operation chain is when we are traversing the graph.

${GET_SNIPPET}

This operation chain takes starts with a seed (vertex) traverses down all outgoing edges using the ${GET_ADJACENT_ENTITY_SEEDS_JAVADOC} operation and then returns all the following connected edges using the ${GET_RELATED_EDGES_JAVADOC} operation. Before returning the results the edges are converted back into the original csv data format using the ${GENERATE_OBJECTS_JAVADOC} operation.
In order to convert the edges back into the initial csv format we have implemented the getObjects method in ${ELEMENT_GENERATOR_JAVADOC}. The generator adds the count property as a third variable in the csv output.

${DATA_GENERATOR_JAVA}

When we execute this query we get:

```csv
${RESULT}
```

You can see the data has been converted straight back into csv.

Operation chains work with any combination of operations where sequential operations have compatible output/input formats.

For more examples of different types of operations see ${OPERATION_EXAMPLES_LINK}.