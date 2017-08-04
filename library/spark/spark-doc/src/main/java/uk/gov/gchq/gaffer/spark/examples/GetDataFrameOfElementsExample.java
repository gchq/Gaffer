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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.doc.operation.OperationExample;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.SparkConstants;
import uk.gov.gchq.gaffer.spark.operation.dataframe.GetDataFrameOfElements;
import uk.gov.gchq.gaffer.user.User;

/**
 * An example showing how the {@link GetDataFrameOfElements} operation is used from Java.
 */
public class GetDataFrameOfElementsExample extends OperationExample {
    private static final Logger ROOT_LOGGER = Logger.getRootLogger();

    public static void main(final String[] args) throws OperationException {
        new GetDataFrameOfElementsExample().run();
    }

    public GetDataFrameOfElementsExample() {
        super(GetDataFrameOfElements.class);
    }

    @Override
    public void runExamples() {
        // Need to actively turn logging on and off as needed as Spark produces some logs
        // even when the log level is set to off.
        ROOT_LOGGER.setLevel(Level.OFF);
        final SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("getDataFrameOfElementsWithEntityGroup")
                .set(SparkConstants.SERIALIZER, SparkConstants.DEFAULT_SERIALIZER)
                .set(SparkConstants.KRYO_REGISTRATOR, SparkConstants.DEFAULT_KRYO_REGISTRATOR)
                .set(SparkConstants.DRIVER_ALLOW_MULTIPLE_CONTEXTS, "true");
        final SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        sparkSession.sparkContext().setLogLevel("OFF");
        final Graph graph = getGraph();
        try {
            getDataFrameOfElementsWithEntityGroup(sparkSession, graph);
            getDataFrameOfElementsWithEdgeGroup(sparkSession, graph);
        } catch (final OperationException e) {
            throw new RuntimeException(e);
        }
        sparkSession.stop();
        ROOT_LOGGER.setLevel(Level.INFO);
    }

    public void getDataFrameOfElementsWithEntityGroup(final SparkSession sparkSession, final Graph graph) throws OperationException {
        ROOT_LOGGER.setLevel(Level.INFO);
        log("#### " + getMethodNameAsSentence(0) + "\n");
        printGraph();
        ROOT_LOGGER.setLevel(Level.OFF);
        final GetDataFrameOfElements operation = new GetDataFrameOfElements.Builder()
                .view(new View.Builder()
                        .entity("entity")
                        .build())
                .sparkSession(sparkSession)
                .build();
        final Dataset<Row> df = graph.execute(operation, new User("user01"));

        // Show
        String result = df.showString(100, 20);
        ROOT_LOGGER.setLevel(Level.INFO);
        printJava("GetDataFrameOfElements operation = new GetDataFrameOfElements.Builder()\n"
                + "                .view(new View.Builder()\n"
                + "                        .entity(\"entity\")\n"
                + "                        .build()).\n"
                + "                .sqlContext(sqlc)\n"
                + "                .build();\n"
                + "Dataset<Row> df = getGraph().execute(operation, new User(\"user01\"));\n"
                + "df.show();");
        log("The results are:\n");
        log("```");
        log(result.substring(0, result.length() - 2));
        log("```");
        ROOT_LOGGER.setLevel(Level.OFF);

        // Restrict to entities involving certain vertices
        final Dataset<Row> seeded = df.filter("vertex = 1 OR vertex = 2");
        result = seeded.showString(100, 20);
        ROOT_LOGGER.setLevel(Level.INFO);
        printJava("df.filter(\"vertex = 1 OR vertex = 2\").show();");
        log("The results are:\n");
        log("```");
        log(result.substring(0, result.length() - 2));
        log("```");
        ROOT_LOGGER.setLevel(Level.OFF);

        // Filter by property
        final Dataset<Row> filtered = df.filter("count > 1");
        result = filtered.showString(100, 20);
        ROOT_LOGGER.setLevel(Level.INFO);
        printJava("df.filter(\"count > 1\").show();");
        log("The results are:\n");
        log("```");
        log(result.substring(0, result.length() - 2));
        log("```");
        ROOT_LOGGER.setLevel(Level.OFF);
    }

    public void getDataFrameOfElementsWithEdgeGroup(final SparkSession sparkSession, final Graph graph) throws OperationException {
        ROOT_LOGGER.setLevel(Level.INFO);
        log("#### " + getMethodNameAsSentence(0) + "\n");
        printGraph();
        ROOT_LOGGER.setLevel(Level.OFF);
        final GetDataFrameOfElements operation = new GetDataFrameOfElements.Builder()
                .view(new View.Builder()
                        .edge("edge")
                        .build())
                .sparkSession(sparkSession)
                .build();
        final Dataset<Row> df = graph.execute(operation, new User("user01"));

        // Show
        String result = df.showString(100, 20);
        ROOT_LOGGER.setLevel(Level.INFO);
        printJava("GetDataFrameOfElements operation = new GetDataFrameOfElements.Builder()\n"
                + "                .view(new View.Builder()\n"
                + "                        .entity(\"edge\")\n"
                + "                        .build()).\n"
                + "                .sqlContext(sqlc)\n"
                + "                .build();\n"
                + "Dataset<Row> df = getGraph().execute(operation, new User(\"user01\"));\n"
                + "df.show();");
        log("The results are:\n");
        log("```");
        log(result.substring(0, result.length() - 2));
        log("```");
        ROOT_LOGGER.setLevel(Level.OFF);

        // Restrict to edges involving given vertices
        final Dataset<Row> seeded = df.filter("src = 1 OR src = 3");
        result = seeded.showString(100, 20);
        ROOT_LOGGER.setLevel(Level.INFO);
        printJava("df.filter(\"src = 1 OR src = 3\").show();");
        log("The results are:\n");
        log("```");
        log(result.substring(0, result.length() - 2));
        log("```");
        ROOT_LOGGER.setLevel(Level.OFF);

        // Filter by property
        final Dataset<Row> filtered = df.filter("count > 1");
        result = filtered.showString(100, 20);
        ROOT_LOGGER.setLevel(Level.INFO);
        printJava("df.filter(\"count > 1\").show();");
        log("The results are:\n");
        log("```");
        log(result.substring(0, result.length() - 2));
        log("```");
        ROOT_LOGGER.setLevel(Level.OFF);
    }

}
