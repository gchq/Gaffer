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
package uk.gov.gchq.gaffer.spark.examples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.doc.operation.OperationExample;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.SparkConstants;
import uk.gov.gchq.gaffer.spark.operation.javardd.GetJavaRDDOfAllElements;
import uk.gov.gchq.gaffer.user.User;

import java.util.List;

/**
 * An example showing how the {@link GetJavaRDDOfAllElements} operation is used from Java.
 */
public class GetJavaRDDOfAllElementsExample extends OperationExample {
    private static final Logger ROOT_LOGGER = Logger.getRootLogger();

    public static void main(final String[] args) throws OperationException {
        new GetJavaRDDOfAllElementsExample().run();
    }

    public GetJavaRDDOfAllElementsExample() {
        super(GetJavaRDDOfAllElements.class, getDescription());
    }

    @Override
    public void runExamples() {
        // Need to actively turn logging on and off as needed as Spark produces some logs
        // even when the log level is set to off.
        ROOT_LOGGER.setLevel(Level.OFF);
        final SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("GetJavaRDDOfAllElementsExample")
                .set(SparkConstants.SERIALIZER, SparkConstants.DEFAULT_SERIALIZER)
                .set(SparkConstants.KRYO_REGISTRATOR, SparkConstants.DEFAULT_KRYO_REGISTRATOR)
                .set(SparkConstants.DRIVER_ALLOW_MULTIPLE_CONTEXTS, "true");
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("OFF");
        final Graph graph = getGraph();
        try {
            getJavaRddOfAllElements(sc, graph);
            getJavaRddOfAllElementsReturningEdgesOnly(sc, graph);
        } catch (final OperationException e) {
            throw new RuntimeException(e);
        }
        sc.stop();
        ROOT_LOGGER.setLevel(Level.INFO);
    }

    private static String getDescription() {
        final String description = "All the elements in a graph can be returned "
                + "as a JavaRDD by using the operation GetJavaRDDOfAllElements. "
                + "Some examples follow. Note that there is an option to "
                + "read the Rfiles directly rather than the usual approach of "
                + "obtaining them from Accumulo's tablet servers. This requires "
                + "the Hadoop user running the Spark job to have read access to "
                + "the RFiles in the Accumulo tablet. Note that data that has "
                + "not been minor compacted will not be read if this option "
                + "is used. This option is enabled using the option "
                + "gaffer.accumulo.spark.directrdd.use_rfile_reader=true";
        return description;
    }

    public void getJavaRddOfAllElements(final JavaSparkContext sc, final Graph graph) throws OperationException {
        ROOT_LOGGER.setLevel(Level.INFO);
        // Avoid using getMethodNameAsSentence as it messes up the formatting of the "RDD" part
        log("#### get Java RDD of elements\n");
        printGraph();
        ROOT_LOGGER.setLevel(Level.OFF);
        final GetJavaRDDOfAllElements operation = new GetJavaRDDOfAllElements.Builder()
                .build();
        final JavaRDD<Element> rdd = graph.execute(operation, new User("user01"));
        final List<Element> elements = rdd.collect();
        ROOT_LOGGER.setLevel(Level.INFO);
        printJava("GetJavaRDDOfAllElements<ElementId> operation = new GetJavaRDDOfAllElements.Builder<>()\n" +
                "                .build();\n"
                + "JavaRDD<Element> rdd = graph.execute(operation, new User(\"user01\"));\n"
                + "List<Element> elements = rdd.collect();");
        log("The results are:\n");
        log("```");
        for (final Element e : elements) {
            log(e.toString());
        }
        log("```");
        ROOT_LOGGER.setLevel(Level.OFF);
    }

    public void getJavaRddOfAllElementsReturningEdgesOnly(final JavaSparkContext sc, final Graph graph) throws OperationException {
        ROOT_LOGGER.setLevel(Level.INFO);
        log("#### get Java RDD of elements returning edges only\n");
        printGraph();
        ROOT_LOGGER.setLevel(Level.OFF);
        final GetJavaRDDOfAllElements operation = new GetJavaRDDOfAllElements.Builder()
                .view(new View.Builder()
                        .edge("edge")
                        .build())
                .build();
        final JavaRDD<Element> rdd = graph.execute(operation, new User("user01"));
        final List<Element> elements = rdd.collect();
        ROOT_LOGGER.setLevel(Level.INFO);
        printJava("GetJavaRDDOfAllElements<ElementId> operation = new GetJavaRDDOfAllElements.Builder<>()\n"
                + "                .view(new View.Builder()\n" +
                "                        .edge(\"edge\")\n" +
                "                        .build())\n" +
                "                .build();\n"
                + "JavaRDD<Element> rdd = graph.execute(operation, new User(\"user01\"));\n"
                + "List<Element> elements = rdd.collect();");
        log("The results are:\n");
        log("```");
        for (final Element e : elements) {
            log(e.toString());
        }
        log("```");
        ROOT_LOGGER.setLevel(Level.OFF);
    }

}
