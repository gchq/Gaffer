/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.gov.gchq.gaffer.doc.operation;

import org.apache.commons.io.IOUtils;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.doc.operation.generator.ElementGenerator;
import uk.gov.gchq.gaffer.doc.util.Example;
import uk.gov.gchq.gaffer.doc.util.JavaSourceUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public abstract class OperationExample extends Example {
    private final Graph graph = createExampleGraph();

    public OperationExample(final Class<? extends Operation> classForExample) {
        super(classForExample);
    }

    public OperationExample(final Class<? extends Operation> classForExample, final String description) {
        super(classForExample, description);
    }

    @Override
    protected void runExamples() {
        // not used - implement runExample(Graph) instead.
    }

    protected Graph getGraph() {
        return graph;
    }

    protected void runExampleNoResult(final Operation operation) {
        log("#### " + getMethodNameAsSentence(1) + "\n");
        printJava(JavaSourceUtil.getRawJavaSnippet(getClass(), "doc", " " + getMethodName(1) + "() {", String.format("---%n"), "// ----"));
        printAsJson(operation);

        try {
            getGraph().execute(operation, new User("user01"));
        } catch (final OperationException e) {
            throw new RuntimeException(e);
        }

        log(METHOD_DIVIDER);
    }

    protected <RESULT_TYPE> RESULT_TYPE runExample(final Output<RESULT_TYPE> operation) {
        log("#### " + getMethodNameAsSentence(1) + "\n");
        printGraph();
        printJava(JavaSourceUtil.getRawJavaSnippet(getClass(), "doc", " " + getMethodName(1) + "() {", String.format("---%n"), "// ----"));
        printAsJson(operation);

        final RESULT_TYPE results;
        try {
            results = getGraph().execute(
                    operation, new User("user01"));
        } catch (final OperationException e) {
            throw new RuntimeException(e);
        }

        logResult(results);

        log(METHOD_DIVIDER);
        return results;
    }

    protected <RESULT_TYPE> RESULT_TYPE runExample(final OperationChain<RESULT_TYPE> operationChain) {
        log("#### " + getMethodNameAsSentence(1) + "\n");
        printGraph();
        printJava(JavaSourceUtil.getRawJavaSnippet(getClass(), "doc", " " + getMethodName(1) + "() {", String.format("---%n"), "// ----"));
        printAsJson(operationChain);

        final RESULT_TYPE result;
        try {
            result = getGraph().execute(
                    operationChain, new User("user01"));
        } catch (final OperationException e) {
            throw new RuntimeException(e);
        }

        logResult(result);

        log(METHOD_DIVIDER);
        return result;
    }

    public <RESULT_TYPE> void logResult(final RESULT_TYPE result) {
        log("Result:");
        log("\n```");
        if (result instanceof Iterable) {
            for (final Object item : (Iterable) result) {
                log(item.toString());
            }
        } else if (result instanceof Map) {
            final Map<?, ?> resultMap = (Map) result;
            for (final Map.Entry<?, ?> entry : resultMap.entrySet()) {
                log(entry.getKey() + ":");
                if (entry.getValue() instanceof Iterable) {
                    for (final Object item : (Iterable) entry.getValue()) {
                        log("    " + item.toString());
                    }
                } else {
                    log("    " + entry.getValue().toString());
                }
            }
        } else if (result instanceof Stream) {
            final Stream stream = (Stream) result;
            stream.forEach(item -> log(item.toString()));
        } else if (result instanceof Object[]) {
            final Object[] array = (Object[]) result;
            for (int i = 0; i < array.length; i++) {
                log(array[i].toString());
            }
        } else {
            log(result.toString());
        }
        log("```");
    }

    protected Graph createExampleGraph() {
        final Graph graph = new Graph.Builder()
                .addSchemas(StreamUtil.openStreams(getClass(), "operation/schema"))
                .storeProperties(StreamUtil.openStream(getClass(), "mockaccumulostore.properties"))
                .build();

        // Create data generator
        final ElementGenerator dataGenerator = new ElementGenerator();

        // Load data into memory
        final List<String> data;
        try {
            data = IOUtils.readLines(StreamUtil.openStream(getClass(), "operation/data.txt"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //add the edges to the graph using an operation chain consisting of:
        //generateElements - generating edges from the data (note these are directed edges)
        //addElements - add the edges to the graph
        final OperationChain addOpChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .generator(dataGenerator)
                        .input(data)
                        .build())
                .then(new AddElements())
                .build();

        try {
            graph.execute(addOpChain, new User());
        } catch (final OperationException e) {
            throw new RuntimeException(e);
        }

        return graph;
    }

    protected void printGraph() {
        log("Using this simple directed graph:");
        log("\n```");
        log("");
        log("    --> 4 <--");
        log("  /     ^     \\");
        log(" /      |      \\");
        log("1  -->  2  -->  3");
        log("         \\");
        log("           -->  5");
        log("```\n");
    }
}
