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
package gaffer.accumulostore.operation.spark.handler;

import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.graph.Graph;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.simple.spark.GetDataFrameOfElementsOperation;
import gaffer.user.User;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Row$;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
                .set("spark.kryo.registrator", "gaffer.serialisation.kryo.Registrator")
                .set("spark.driver.allowMultipleContexts", "true");
        final SparkContext sparkContext = new SparkContext(sparkConf);
        final SQLContext sqlContext = new SQLContext(sparkContext);

        // Edges group - check get correct edges
        GetDataFrameOfElementsOperation dfOperation = new GetDataFrameOfElementsOperation(sqlContext, EDGE_GROUP);
        Iterable<DataFrame> dataFrames = graph1.execute(dfOperation, user);
        if (dataFrames == null || !dataFrames.iterator().hasNext()) {
            fail("No DataFrame returned");
        }
        DataFrame dataFrame = dataFrames.iterator().next();
        Set<Row> results = new HashSet<>(dataFrame.collectAsList());
        final Set<Row> expectedRows = new HashSet<>();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            final scala.collection.mutable.MutableList<Object> fields1 = new scala.collection.mutable.MutableList<>();
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
        dfOperation = new GetDataFrameOfElementsOperation(sqlContext, ENTITY_GROUP);
        dataFrames = graph1.execute(dfOperation, user);
        if (dataFrames == null || !dataFrames.iterator().hasNext()) {
            fail("No DataFrame returned");
        }
        dataFrame = dataFrames.iterator().next();
        results.clear();
        results.addAll(dataFrame.collectAsList());
        expectedRows.clear();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            final scala.collection.mutable.MutableList<Object> fields1 = new scala.collection.mutable.MutableList<>();
            fields1.clear();
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
                .set("spark.kryo.registrator", "gaffer.serialisation.kryo.Registrator")
                .set("spark.driver.allowMultipleContexts", "true");
        final SparkContext sparkContext = new SparkContext(sparkConf);
        final SQLContext sqlContext = new SQLContext(sparkContext);

        // Get all edges
        final GetDataFrameOfElementsOperation dfOperation = new GetDataFrameOfElementsOperation(sqlContext, EDGE_GROUP);
        final Iterable<DataFrame> dataFrames = graph1.execute(dfOperation, user);
        if (dataFrames == null || !dataFrames.iterator().hasNext()) {
            fail("No DataFrame returned");
        }
        final DataFrame dataFrame = dataFrames.iterator().next();

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
                .set("spark.kryo.registrator", "gaffer.serialisation.kryo.Registrator")
                .set("spark.driver.allowMultipleContexts", "true");
        final SparkContext sparkContext = new SparkContext(sparkConf);
        sparkContext.setLogLevel("DEBUG");
        final SQLContext sqlContext = new SQLContext(sparkContext);

        // Get DataFrame
        final GetDataFrameOfElementsOperation dfOperation = new GetDataFrameOfElementsOperation(sqlContext, EDGE_GROUP);
        final Iterable<DataFrame> dataFrames = graph1.execute(dfOperation, user);
        if (dataFrames == null || !dataFrames.iterator().hasNext()) {
            fail("No DataFrame returned");
        }
        final DataFrame dataFrame = dataFrames.iterator().next();

        // Check get correct rows when ask for all columns but only rows where property2 > 4.0
        Set<Row> results = new HashSet<>(dataFrame.filter("property2 > 4.0").collectAsList());
        final Set<Row> expectedRows = new HashSet<>();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            final scala.collection.mutable.MutableList<Object> fields = new scala.collection.mutable.MutableList<>();
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
