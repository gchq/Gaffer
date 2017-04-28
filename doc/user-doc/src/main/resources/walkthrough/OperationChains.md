${HEADER}

${CODE_LINK}

In the previous examples we have had several steps to get data into Gaffer. As promised, we can now simplify this and use an operation chain.

We will show you a way of using an operation chain to both generate the elements from the data file and add them directly to the graph.
Operation chains are simply a list of operations in which the operations are executed sequentially, the output of one operation is passed in as the input to the next operation.

So adding elements from a CSV file can now be done as follows:

${ADD_SNIPPET}

This chain consists of 2 operations.
The first, GenerateElements, which takes the data and an element generator and generates the Gaffer Edges.
The second, AddElements, simply takes the generated edges and adds them to the graph.
This operation chain can then be executed on the graph as before.


Another example of using an operation chain is when we are traversing the graph.

${GET_SNIPPET}

This operation chain takes starts with a seed vertex traverses down all outgoing edges using the ${GET_ADJACENT_ENTITY_SEEDS_JAVADOC} operation and then returns all the following connected edges using the ${GET_ELEMENTS_JAVADOC} operation. 
Before returning the results the edges are converted into a csv format (junctionA, junctionB, count) using the ${GENERATE_OBJECTS_JAVADOC} operation.
In order to convert the edges back into the initial csv format we have implemented the ${OBJECT_GENERATOR_JAVADOC}. 

${CSV_GENERATOR_JAVA}

When we execute this query we get:

```csv
${RESULT}
```

You can see the data has been converted back into csv.

Operation chains work with any combination of operations where sequential operations have compatible output/input formats.

For more examples of different types of operations see ${OPERATION_EXAMPLES_LINK}.