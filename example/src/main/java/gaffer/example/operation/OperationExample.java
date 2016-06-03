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
package gaffer.example.operation;

import gaffer.commonutil.CommonConstants;
import gaffer.commonutil.StreamUtil;
import gaffer.example.gettingstarted.util.DataUtils;
import gaffer.example.operation.generator.DataGenerator;
import gaffer.exception.SerialisationException;
import gaffer.graph.Graph;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.operation.Operation;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.generate.GenerateElements;
import gaffer.user.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Locale;

public abstract class OperationExample {

    public static final String CAPITALS_AND_NUMBERS_REGEX = "((?=[A-Z])|(?<=[0-9])(?=[a-zA-Z])|(?<=[a-zA-Z])(?=[0-9]))";
    public static final JSONSerialiser JSON_SERIALISER = new JSONSerialiser();
    public static final String DIVIDER = "-----------------------------------------------";
    public static final String TITLE_DIVIDER = DIVIDER;
    public static final String METHOD_DIVIDER = DIVIDER + "\n";
    private static final String JAVA_DOC_URL_PREFIX = "http://governmentcommunicationsheadquarters.github.io/Gaffer/";
    private final Class<? extends Operation> operationClass;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public OperationExample(final Class<? extends Operation> operationClass) {
        this.operationClass = operationClass;
    }

    public void run() throws OperationException {
        log(operationClass.getSimpleName() + " example");
        log(TITLE_DIVIDER);
        log("See [javadoc](" + JAVA_DOC_URL_PREFIX + operationClass.getName().replace(".", "/") + ".html).\n");

        final Graph graph = createExampleGraph();
        runExamples(graph);
    }

    public Class<? extends Operation> getOperationClass() {
        return operationClass;
    }

    protected abstract void runExamples(final Graph graph) throws OperationException;

    public Graph createExampleGraph() {
        final Graph graph = new Graph.Builder()
                .addSchemas(StreamUtil.openStreams(getClass(), "/example/operation/schema"))
                .storeProperties(StreamUtil.openStream(getClass(), "/example/operation/mockaccumulostore.properties"))
                .build();

        // Create data generator
        final DataGenerator dataGenerator = new DataGenerator();

        // Load data into memory
        final List<String> data = DataUtils.loadData(StreamUtil.openStream(getClass(), "/example/operation/data.txt", true));

        //add the edges to the graph using an operation chain consisting of:
        //generateElements - generating edges from the data (note these are directed edges)
        //addElements - add the edges to the graph
        final OperationChain addOpChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .generator(dataGenerator)
                        .objects(data)
                        .build())
                .then(new AddElements())
                .build();

        try {
            graph.execute(addOpChain, new User());
        } catch (OperationException e) {
            throw new RuntimeException(e);
        }

        log("Using this simple directed graph:");
        log("\n```");
        log("    --> 4 <--");
        log("  /     ^     \\");
        log(" /      |      \\");
        log("1  -->  2  -->  3");
        log("         \\");
        log("           -->  5");
        log("```");

        log(METHOD_DIVIDER);

        return graph;
    }

    protected String getMethodNameAsSentence(final int parentMethod) {
        final String[] words = Thread.currentThread().getStackTrace()[parentMethod + 2].getMethodName().split(CAPITALS_AND_NUMBERS_REGEX);
        final StringBuilder sentence = new StringBuilder();
        for (String word : words) {
            sentence.append(word.toLowerCase(Locale.getDefault()))
                    .append(" ");
        }
        sentence.replace(0, 1, sentence.substring(0, 1).toUpperCase(Locale.getDefault()));
        sentence.replace(sentence.length() - 1, sentence.length(), "");
        return sentence.toString();
    }

    protected String getMethodNameAsSentence() {
        return getMethodNameAsSentence(1);
    }

    protected String getOperationJson(final Operation operation) {
        try {
            return new String(JSON_SERIALISER.serialise(operation, true), CommonConstants.UTF_8);
        } catch (final SerialisationException | UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    protected <RESULT_TYPE extends Iterable<?>> RESULT_TYPE runAndPrintOperation(final Operation<?, RESULT_TYPE> operation, final Graph graph, final String operationJava) throws OperationException {
        log("#### " + getMethodNameAsSentence(1) + "\n");
        final String operationJson = getOperationJson(operation);
        printOperationJava(operationJava);

        final RESULT_TYPE results = graph.execute(
                operation, new User("user01"));

        log("Results:");
        log("\n```");
        for (Object e : results) {
            log(e.toString());
        }
        log("```");

        printOperationJson(operationJson);

        log(METHOD_DIVIDER);
        return results;
    }

    protected void printOperationJava(final String java) {
        log("\n```java");
        log(java);
        log("```\n\n");
    }

    protected void printOperationJson(final String operationJson) {
        log("\nThis operation can also be written in JSON:");
        log("\n```json");
        log(operationJson);
        log("```\n");
    }

    protected void log(final String message) {
        logger.info(message);
    }
}
