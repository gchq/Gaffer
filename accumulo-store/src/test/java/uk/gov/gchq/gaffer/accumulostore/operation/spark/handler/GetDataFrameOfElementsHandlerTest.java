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
package uk.gov.gchq.gaffer.accumulostore.operation.spark.handler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.simple.spark.GetDataFrameOfElements;
import uk.gov.gchq.gaffer.user.User;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Row$;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;

import java.util.*;

public class GetDataFrameOfElementsHandlerTest {

    private final static String ENTITY_GROUP = "BasicEntity";
    private final static String EDGE_GROUP = "BasicEdge";
    private final static int NUM_ELEMENTS = 1;

    @Test
    public void checkGetCorrectElementsInDataFrame() throws OperationException {
        final Graph graph1 = new Graph.Builder()
                .addSchema(getClass().getResourceAsStream("/schema-DataFrame/dataSchema.json"))
                .addSchema(getClass().getResourceAsStream("/schema-DataFrame/dataTypes.json"))
                .addSchema(getClass().getResourceAsStream("/schema-DataFrame/storeTypes.json"))
                .storeProperties(getClass().getResourceAsStream("/store.properties"))
                .build();

        final User user = new User();
        graph1.execute(new AddElements(getElements()), user);

        final SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("checkGetCorrectElementsInDataFrame")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "uk.gov.gchq.gaffer.serialisation.kryo.Registrator")
                .set("spark.driver.allowMultipleContexts", "true");
        final SparkContext sparkContext = new SparkContext(sparkConf);
        final SQLContext sqlContext = new SQLContext(sparkContext);

        // Edges group - check get correct edges
        GetDataFrameOfElements dfOperation = new GetDataFrameOfElements.Builder()
                .sqlContext(sqlContext)
                .group(EDGE_GROUP)
                .build();
        Dataset<Row> dataFrame = graph1.execute(dfOperation, user);
        if (dataFrame == null) {
            fail("No DataFrame returned");
        }
        Set<Row> results = new HashSet<>(dataFrame.collectAsList());
        final Set<Row> expectedRows = new HashSet<>();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            final scala.collection.mutable.MutableList<Object> fields1 = new scala.collection.mutable.MutableList<>();
            fields1.appendElem(EDGE_GROUP);
            fields1.appendElem("" + i);
            fields1.appendElem("B");
            fields1.appendElem(1);
            fields1.appendElem(2);
            fields1.appendElem(3.0F);
            fields1.appendElem(4.0D);
            fields1.appendElem(5L);
            fields1.appendElem(100L);
            expectedRows.add(Row$.MODULE$.fromSeq(fields1));
            final scala.collection.mutable.MutableList<Object> fields2 = new scala.collection.mutable.MutableList<>();
            fields2.appendElem(EDGE_GROUP);
            fields2.appendElem("" + i);
            fields2.appendElem("C");
            fields2.appendElem(6);
            fields2.appendElem(7);
            fields2.appendElem(8.0F);
            fields2.appendElem(9.0D);
            fields2.appendElem(10L);
            fields2.appendElem(200L);
            expectedRows.add(Row$.MODULE$.fromSeq(fields2));
        }
        assertEquals(expectedRows, results);

        // Entities group - check get correct entities
        dfOperation = new GetDataFrameOfElements.Builder()
                .sqlContext(sqlContext)
                .group(ENTITY_GROUP)
                .build();
        dataFrame = graph1.execute(dfOperation, user);
        if (dataFrame == null) {
            fail("No DataFrame returned");
        }
        results.clear();
        results.addAll(dataFrame.collectAsList());
        expectedRows.clear();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            final scala.collection.mutable.MutableList<Object> fields1 = new scala.collection.mutable.MutableList<>();
            fields1.clear();
            fields1.appendElem(ENTITY_GROUP);
            fields1.appendElem("" + i);
            fields1.appendElem(1);
            fields1.appendElem(2);
            fields1.appendElem(3.0F);
            fields1.appendElem(4.0D);
            fields1.appendElem(5L);
            fields1.appendElem(6);
            expectedRows.add(Row$.MODULE$.fromSeq(fields1));
        }
        assertEquals(expectedRows, results);

        sparkContext.stop();
    }

    @Test
    public void checkGetCorrectElementsInDataFrameMultipleGroups() throws OperationException {
        final Graph graph1 = new Graph.Builder()
                .addSchema(getClass().getResourceAsStream("/schema-DataFrame/dataSchema.json"))
                .addSchema(getClass().getResourceAsStream("/schema-DataFrame/dataTypes.json"))
                .addSchema(getClass().getResourceAsStream("/schema-DataFrame/storeTypes.json"))
                .storeProperties(getClass().getResourceAsStream("/store.properties"))
                .build();

        final User user = new User();
        graph1.execute(new AddElements(getElements()), user);

        final SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("checkGetCorrectElementsInDataFrameMultipleGroups")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "uk.gov.gchq.gaffer.serialisation.kryo.Registrator")
                .set("spark.driver.allowMultipleContexts", "true");
        final SparkContext sparkContext = new SparkContext(sparkConf);
        final SQLContext sqlContext = new SQLContext(sparkContext);

        // Use entity and edges group - check get correct data
        final LinkedHashSet<String> groups = new LinkedHashSet<>();
        groups.add(ENTITY_GROUP);
        groups.add(EDGE_GROUP);
        GetDataFrameOfElements dfOperation = new GetDataFrameOfElements.Builder()
                .sqlContext(sqlContext)
                .groups(groups)
                .build();
        Dataset<Row> dataFrame = graph1.execute(dfOperation, user);
        if (dataFrame == null) {
            fail("No DataFrame returned");
        }
        final Set<Row> results = new HashSet<>(dataFrame.collectAsList());
        final Set<Row> expectedRows = new HashSet<>();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            final scala.collection.mutable.MutableList<Object> fields1 = new scala.collection.mutable.MutableList<>();
            fields1.appendElem(EDGE_GROUP);
            fields1.appendElem(null);
            fields1.appendElem(1);
            fields1.appendElem(2);
            fields1.appendElem(3.0F);
            fields1.appendElem(4.0D);
            fields1.appendElem(5L);
            fields1.appendElem(100L);
            fields1.appendElem("" + i);
            fields1.appendElem("B");
            expectedRows.add(Row$.MODULE$.fromSeq(fields1));
            final scala.collection.mutable.MutableList<Object> fields2 = new scala.collection.mutable.MutableList<>();
            fields2.appendElem(EDGE_GROUP);
            fields2.appendElem(null);
            fields2.appendElem(6);
            fields2.appendElem(7);
            fields2.appendElem(8.0F);
            fields2.appendElem(9.0D);
            fields2.appendElem(10L);
            fields2.appendElem(200L);
            fields2.appendElem("" + i);
            fields2.appendElem("C");
            expectedRows.add(Row$.MODULE$.fromSeq(fields2));
            final scala.collection.mutable.MutableList<Object> fields3 = new scala.collection.mutable.MutableList<>();
            fields3.appendElem(ENTITY_GROUP);
            fields3.appendElem("" + i);
            fields3.appendElem(1);
            fields3.appendElem(2);
            fields3.appendElem(3.0F);
            fields3.appendElem(4.0D);
            fields3.appendElem(5L);
            fields3.appendElem(6);
            fields3.appendElem(null);
            fields3.appendElem(null);
            expectedRows.add(Row$.MODULE$.fromSeq(fields3));
        }
        assertEquals(expectedRows, results);

        // Entities group - check get correct entities
        dfOperation = new GetDataFrameOfElements.Builder()
                .sqlContext(sqlContext)
                .group(ENTITY_GROUP)
                .build();
        dataFrame = graph1.execute(dfOperation, user);
        if (dataFrame == null) {
            fail("No DataFrame returned");
        }
        results.clear();
        results.addAll(dataFrame.collectAsList());
        expectedRows.clear();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            final scala.collection.mutable.MutableList<Object> fields1 = new scala.collection.mutable.MutableList<>();
            fields1.clear();
            fields1.appendElem(ENTITY_GROUP);
            fields1.appendElem("" + i);
            fields1.appendElem(1);
            fields1.appendElem(2);
            fields1.appendElem(3.0F);
            fields1.appendElem(4.0D);
            fields1.appendElem(5L);
            fields1.appendElem(6);
            expectedRows.add(Row$.MODULE$.fromSeq(fields1));
        }
        assertEquals(expectedRows, results);

        sparkContext.stop();
    }

    @Test
    public void checkGetCorrectElementsInDataFrameWithProjection() throws OperationException {
        final Graph graph1 = new Graph.Builder()
                .addSchema(getClass().getResourceAsStream("/schema-DataFrame/dataSchema.json"))
                .addSchema(getClass().getResourceAsStream("/schema-DataFrame/dataTypes.json"))
                .addSchema(getClass().getResourceAsStream("/schema-DataFrame/storeTypes.json"))
                .storeProperties(getClass().getResourceAsStream("/store.properties"))
                .build();

        final User user = new User();
        graph1.execute(new AddElements(getElements()), user);

        final SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("checkGetCorrectElementsInDataFrameWithProjection")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "uk.gov.gchq.gaffer.serialisation.kryo.Registrator")
                .set("spark.driver.allowMultipleContexts", "true");
        final SparkContext sparkContext = new SparkContext(sparkConf);
        final SQLContext sqlContext = new SQLContext(sparkContext);

        // Get all edges
        final GetDataFrameOfElements dfOperation = new GetDataFrameOfElements.Builder()
                .sqlContext(sqlContext)
                .group(EDGE_GROUP)
                .build();
        final Dataset<Row> dataFrame = graph1.execute(dfOperation, user);
        if (dataFrame == null) {
            fail("No DataFrame returned");
        }

        // Check get correct rows when ask for src, dst and property2 columns
        Set<Row> results = new HashSet<>(dataFrame.select("src", "dst", "property2").collectAsList());
        final Set<Row> expectedRows = new HashSet<>();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            scala.collection.mutable.MutableList<Object> fields1 = new scala.collection.mutable.MutableList<>();
            fields1.appendElem("" + i);
            fields1.appendElem("B");
            fields1.appendElem(3.0F);
            expectedRows.add(Row$.MODULE$.fromSeq(fields1));
            scala.collection.mutable.MutableList<Object> fields2 = new scala.collection.mutable.MutableList<>();
            fields2.appendElem("" + i);
            fields2.appendElem("C");
            fields2.appendElem(8.0F);
            expectedRows.add(Row$.MODULE$.fromSeq(fields2));
        }
        assertEquals(expectedRows, results);

        // Check get correct rows when ask for just property2 column
        results = new HashSet<>(dataFrame.select("property2").collectAsList());
        expectedRows.clear();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            scala.collection.mutable.MutableList<Object> fields1 = new scala.collection.mutable.MutableList<>();
            fields1 = new scala.collection.mutable.MutableList<>();
            fields1.appendElem(3.0F);
            expectedRows.add(Row$.MODULE$.fromSeq(fields1));
            scala.collection.mutable.MutableList<Object> fields2 = new scala.collection.mutable.MutableList<>();
            fields2 = new scala.collection.mutable.MutableList<>();
            fields2.appendElem(8.0F);
            expectedRows.add(Row$.MODULE$.fromSeq(fields2));
        }
        assertEquals(expectedRows, results);

        sparkContext.stop();
    }

    @Test
    public void checkGetCorrectElementsInDataFrameWithProjectionAndFiltering() throws OperationException {
        final Graph graph1 = new Graph.Builder()
                .addSchema(getClass().getResourceAsStream("/schema-DataFrame/dataSchema.json"))
                .addSchema(getClass().getResourceAsStream("/schema-DataFrame/dataTypes.json"))
                .addSchema(getClass().getResourceAsStream("/schema-DataFrame/storeTypes.json"))
                .storeProperties(getClass().getResourceAsStream("/store.properties"))
                .build();

        final User user = new User();
        graph1.execute(new AddElements(getElements()), user);

        final SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("checkGetCorrectElementsInDataFrameWithProjectionAndFiltering")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "uk.gov.gchq.gaffer.serialisation.kryo.Registrator")
                .set("spark.driver.allowMultipleContexts", "true");
        final SparkContext sparkContext = new SparkContext(sparkConf);
        final SQLContext sqlContext = new SQLContext(sparkContext);

        // Get DataFrame
        final GetDataFrameOfElements dfOperation = new GetDataFrameOfElements.Builder()
                .sqlContext(sqlContext)
                .group(EDGE_GROUP)
                .build();
        final Dataset<Row> dataFrame = graph1.execute(dfOperation, user);
        if (dataFrame == null) {
            fail("No DataFrame returned");
        }

        // Check get correct rows when ask for all columns but only rows where property2 > 4.0
        Set<Row> results = new HashSet<>(dataFrame.filter("property2 > 4.0").collectAsList());
        final Set<Row> expectedRows = new HashSet<>();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            final scala.collection.mutable.MutableList<Object> fields = new scala.collection.mutable.MutableList<>();
            fields.appendElem(EDGE_GROUP);
            fields.appendElem("" + i);
            fields.appendElem("C");
            fields.appendElem(6);
            fields.appendElem(7);
            fields.appendElem(8.0F);
            fields.appendElem(9.0D);
            fields.appendElem(10L);
            fields.appendElem(200L);
            expectedRows.add(Row$.MODULE$.fromSeq(fields));
        }
        assertEquals(expectedRows, results);

        // Check get correct rows when ask for columns property2 and property3 but only rows where property2 > 4.0
        results = new HashSet<>(dataFrame.select("property2", "property3").filter("property2 > 4.0").collectAsList());
        expectedRows.clear();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            final scala.collection.mutable.MutableList<Object> fields = new scala.collection.mutable.MutableList<>();
            fields.appendElem(8.0F);
            fields.appendElem(9.0D);
            expectedRows.add(Row$.MODULE$.fromSeq(fields));
        }
        assertEquals(expectedRows, results);

        sparkContext.stop();
    }

    @Test
    public void checkGetExceptionIfIncompatibleSchemas() throws OperationException {
        final Graph graph1 = new Graph.Builder()
                .addSchema(getClass().getResourceAsStream("/schema-DataFrame/dataSchemaIncompatible.json"))
                .addSchema(getClass().getResourceAsStream("/schema-DataFrame/dataTypes.json"))
                .addSchema(getClass().getResourceAsStream("/schema-DataFrame/storeTypes.json"))
                .storeProperties(getClass().getResourceAsStream("/store.properties"))
                .build();

        final User user = new User();

        final SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("checkGetExceptionIfIncompatibleSchemas")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "uk.gov.gchq.gaffer.serialisation.kryo.Registrator")
                .set("spark.driver.allowMultipleContexts", "true");
        final SparkContext sparkContext = new SparkContext(sparkConf);
        final SQLContext sqlContext = new SQLContext(sparkContext);

        // Use entity and edges group - check get correct data
        final LinkedHashSet<String> groups = new LinkedHashSet<>();
        groups.add(ENTITY_GROUP);
        groups.add(EDGE_GROUP);
        final GetDataFrameOfElements dfOperation = new GetDataFrameOfElements.Builder()
                .sqlContext(sqlContext)
                .groups(groups)
                .build();
        // NB Catch the exception rather than using expected annotation on test to ensure SparkContext
        // is shut down.
        try {
            graph1.execute(dfOperation, user);
            fail("IllegalArgumentException should have been thrown");
        } catch (final IllegalArgumentException e) {
            // Expected
        }
        sparkContext.stop();
    }

    private static List<Element> getElements() {
        final List<Element> elements = new ArrayList<>();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            final Entity entity = new Entity(ENTITY_GROUP);
            entity.setVertex("" + i);
            entity.putProperty("columnQualifier", 1);
            entity.putProperty("property1", 2);
            entity.putProperty("property2", 3.0F);
            entity.putProperty("property3", 4.0D);
            entity.putProperty("property4", 5L);
            entity.putProperty("count", 6L);

            final Edge edge1 = new Edge(EDGE_GROUP);
            edge1.setSource("" + i);
            edge1.setDestination("B");
            edge1.setDirected(true);
            edge1.putProperty("columnQualifier", 1);
            edge1.putProperty("property1", 2);
            edge1.putProperty("property2", 3.0F);
            edge1.putProperty("property3", 4.0D);
            edge1.putProperty("property4", 5L);
            edge1.putProperty("count", 100L);

            final Edge edge2 = new Edge(EDGE_GROUP);
            edge2.setSource("" + i);
            edge2.setDestination("C");
            edge2.setDirected(true);
            edge2.putProperty("columnQualifier", 6);
            edge2.putProperty("property1", 7);
            edge2.putProperty("property2", 8.0F);
            edge2.putProperty("property3", 9.0D);
            edge2.putProperty("property4", 10L);
            edge2.putProperty("count", 200L);

            elements.add(edge1);
            elements.add(edge2);
            elements.add(entity);
        }
        return elements;
    }

}
