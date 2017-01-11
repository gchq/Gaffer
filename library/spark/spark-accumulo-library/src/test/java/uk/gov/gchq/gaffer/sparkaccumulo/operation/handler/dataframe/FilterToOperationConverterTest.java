/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.Or;
import org.junit.Test;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.spark.operation.scalardd.AbstractGetRDD;
import uk.gov.gchq.gaffer.user.User;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Contains unit tests for {@link FiltersToOperationConverter}. The operations that are returned are checked
 * directly (i.e. by consuming the RDD), rather than being tested through the <code>GetDataFrameOfElements</code>
 * operation as that will filter out invalid results itself in the Spark executors. Here we want to test that the
 * underlying RDD from which the Dataframe is derived has already has the correct filtering applied to it.
 */
public class FilterToOperationConverterTest {

    @Test
    public void testIncompatibleGroups() throws OperationException {
        final Graph graph = getGraph();
        final SQLContext sqlContext = getSqlContext("testIncompatibleGroups");

        final Filter[] filters = new Filter[2];
        filters[0] = new EqualTo(AccumuloStoreRelation.GROUP, "A");
        filters[1] = new EqualTo(AccumuloStoreRelation.GROUP, "B");
        final FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext,
                new View.Builder().build(), graph.getSchema(), filters);

        final AbstractGetRDD<?> operation = converter.getOperation();
        assertNull(operation);

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testSingleGroup() throws OperationException {
        final Graph graph = getGraph();
        final SQLContext sqlContext = getSqlContext("testSingleGroup");

        final Filter[] filters = new Filter[1];
        filters[0] = new EqualTo(AccumuloStoreRelation.GROUP, GetDataFrameOfElementsHandlerTest.ENTITY_GROUP);
        final FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext, graph.getView(),
                graph.getSchema(), filters);

        final AbstractGetRDD<?> operation = converter.getOperation();
        final RDD<Element> rdd = graph.execute(operation, new User());
        final Set<Element> results = new HashSet<>();
        final Element[] returnedElements = (Element[]) rdd.collect();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        final Set<Element> expectedResults = new HashSet<>();
        for (final Element element : getElements()) {
            if (element.getGroup().equals(GetDataFrameOfElementsHandlerTest.ENTITY_GROUP)) {
                expectedResults.add(element);
            }
        }
        assertEquals(expectedResults, results);

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testSingleGroupNotInSchema() throws OperationException {
        final Graph graph = getGraph();
        final SQLContext sqlContext = getSqlContext("testSingleGroupNotInSchema");

        final Filter[] filters = new Filter[1];
        filters[0] = new EqualTo(AccumuloStoreRelation.GROUP, "random");
        final FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext, graph.getView(),
                graph.getSchema(), filters);

        final AbstractGetRDD<?> operation = converter.getOperation();
        assertNull(operation);

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testTwoGroups() throws OperationException {
        final Graph graph = getGraph();
        final SQLContext sqlContext = getSqlContext("testTwoGroups");

        final Filter[] filters = new Filter[1];
        final Filter left = new EqualTo(AccumuloStoreRelation.GROUP, GetDataFrameOfElementsHandlerTest.ENTITY_GROUP);
        final Filter right = new EqualTo(AccumuloStoreRelation.GROUP, GetDataFrameOfElementsHandlerTest.EDGE_GROUP2);
        filters[0] = new Or(left, right);
        final FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext, graph.getView(),
                graph.getSchema(), filters);

        final AbstractGetRDD<?> operation = converter.getOperation();
        final RDD<Element> rdd = graph.execute(operation, new User());
        final Set<Element> results = new HashSet<>();
        final Element[] returnedElements = (Element[]) rdd.collect();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        final Set<Element> expectedResults = new HashSet<>();
        for (final Element element : getElements()) {
            if (element.getGroup().equals(GetDataFrameOfElementsHandlerTest.ENTITY_GROUP)
                    || element.getGroup().equals(GetDataFrameOfElementsHandlerTest.EDGE_GROUP2)) {
                expectedResults.add(element);
            }
        }
        assertEquals(expectedResults, results);

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testSpecifyVertex() throws OperationException {
        final Graph graph = getGraph();
        final SQLContext sqlContext = getSqlContext("testSpecifyVertex");

        final Filter[] filters = new Filter[1];
        filters[0] = new EqualTo(AccumuloStoreRelation.VERTEX_COL_NAME, "0");
        final FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext, graph.getView(),
                graph.getSchema(), filters);

        final AbstractGetRDD<?> operation = converter.getOperation();
        final RDD<Element> rdd = graph.execute(operation, new User());
        final Set<Element> results = new HashSet<>();
        final Element[] returnedElements = (Element[]) rdd.collect();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        final Set<Element> expectedResults = new HashSet<>();
        for (final Element element : getElements()) {
            if (element instanceof Entity && ((Entity) element).getVertex().equals("0")) {
                expectedResults.add(element);
            }
        }
        assertEquals(expectedResults, results);

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testSpecifySourceOrDestination() throws OperationException {
        final Graph graph = getGraph();
        final SQLContext sqlContext = getSqlContext("testSpecifySourceOrDestination");

        final Filter[] filters = new Filter[1];
        filters[0] = new EqualTo(AccumuloStoreRelation.SRC_COL_NAME, "0");
        FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext, graph.getView(),
                graph.getSchema(), filters);

        AbstractGetRDD<?> operation = converter.getOperation();
        RDD<Element> rdd = graph.execute(operation, new User());
        final Set<Element> results = new HashSet<>();
        Element[] returnedElements = (Element[]) rdd.collect();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        final Set<Element> expectedResults = new HashSet<>();
        for (final Element element : getElements()) {
            if (element instanceof Edge
                    && (((Edge) element).getSource().equals("0")
                    || ((Edge) element).getDestination().equals("0"))) {
                expectedResults.add(element);
            }
        }
        assertEquals(expectedResults, results);

        filters[0] = new EqualTo(AccumuloStoreRelation.SRC_COL_NAME, "0");
        converter = new FiltersToOperationConverter(sqlContext, graph.getView(), graph.getSchema(), filters);

        operation = converter.getOperation();
        rdd = graph.execute(operation, new User());
        results.clear();
        returnedElements = (Element[]) rdd.collect();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        expectedResults.clear();
        for (final Element element : getElements()) {
            if (element instanceof Edge
                    && (((Edge) element).getSource().equals("0")
                    || ((Edge) element).getDestination().equals("0"))) {
                expectedResults.add(element);
            }
        }
        assertEquals(expectedResults, results);

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testSpecifyPropertyFilters() throws OperationException {
        final Graph graph = getGraph();
        final SQLContext sqlContext = getSqlContext("testSpecifyPropertyFilters");
        final Filter[] filters = new Filter[1];

        // GreaterThan
        filters[0] = new GreaterThan("property1", 5);
        FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext, graph.getView(),
                graph.getSchema(), filters);
        AbstractGetRDD<?> operation = converter.getOperation();
        RDD<Element> rdd = graph.execute(operation, new User());
        final Set<Element> results = new HashSet<>();
        Element[] returnedElements = (Element[]) rdd.collect();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        final Set<Element> expectedResults = new HashSet<>();
        for (final Element element : getElements()) {
            if ((Integer) element.getProperty("property1") > 5) {
                expectedResults.add(element);
            }
        }
        assertEquals(expectedResults, results);

        // LessThan
        filters[0] = new LessThan("property4", 8L);
        converter = new FiltersToOperationConverter(sqlContext, graph.getView(), graph.getSchema(), filters);
        operation = converter.getOperation();
        rdd = graph.execute(operation, new User());
        results.clear();
        returnedElements = (Element[]) rdd.collect();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        expectedResults.clear();
        for (final Element element : getElements()) {
            if (element.getProperties().containsKey("property4") && (Long) element.getProperty("property4") < 8L) {
                expectedResults.add(element);
            }
        }
        assertEquals(expectedResults, results);

        // And
        final Filter left = new GreaterThan("property1", 5);
        final Filter right = new GreaterThan("property4", 8L);
        filters[0] = new And(left, right);
        converter = new FiltersToOperationConverter(sqlContext, graph.getView(), graph.getSchema(), filters);
        operation = converter.getOperation();
        rdd = graph.execute(operation, new User());
        results.clear();
        returnedElements = (Element[]) rdd.collect();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        expectedResults.clear();
        for (final Element element : getElements()) {
            if (element.getProperties().containsKey("property1") && element.getProperties().containsKey("property4")) {
                if ((Integer) element.getProperty("property1") > 5 && (Long) element.getProperty("property4") > 8L) {
                    expectedResults.add(element);
                }
            }
        }
        assertEquals(expectedResults, results);

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testSpecifyMultiplePropertyFilters() throws OperationException {
        final Graph graph = getGraph();
        final SQLContext sqlContext = getSqlContext("testSpecifyMultiplePropertyFilters");

        final Filter[] filters = new Filter[2];
        filters[0] = new GreaterThan("property1", 5);
        filters[1] = new LessThan("property4", 8L);
        FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext, graph.getView(),
                graph.getSchema(), filters);
        AbstractGetRDD<?> operation = converter.getOperation();
        RDD<Element> rdd = graph.execute(operation, new User());
        final Set<Element> results = new HashSet<>();
        Element[] returnedElements = (Element[]) rdd.collect();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        final Set<Element> expectedResults = new HashSet<>();
        for (final Element element : getElements()) {
            if (element.getProperties().containsKey("property1") && element.getProperties().containsKey("property4")) {
                if ((Integer) element.getProperty("property1") > 5 && (Long) element.getProperty("property4") < 8L) {
                    expectedResults.add(element);
                }
            }
        }
        assertEquals(expectedResults, results);

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testSpecifyVertexAndPropertyFilter() throws OperationException {
        final Graph graph = getGraph();
        final SQLContext sqlContext = getSqlContext("testSpecifyVertexAndPropertyFilter");

        Filter[] filters = new Filter[2];
        filters[0] = new GreaterThan("property1", 5);
        filters[1] = new EqualTo(AccumuloStoreRelation.VERTEX_COL_NAME, "0");
        FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext, graph.getView(),
                graph.getSchema(), filters);
        AbstractGetRDD<?> operation = converter.getOperation();
        RDD<Element> rdd = graph.execute(operation, new User());
        final Set<Element> results = new HashSet<>();
        Element[] returnedElements = (Element[]) rdd.collect();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        final Set<Element> expectedResults = new HashSet<>();
        for (final Element element : getElements()) {
            if (element instanceof Entity
                    && ((Entity) element).getVertex().equals("0")
                    && (Integer) element.getProperty("property1") > 5) {
                expectedResults.add(element);
            }
        }
        assertEquals(expectedResults, results);

        filters = new Filter[3];
        filters[0] = new GreaterThan("property1", 5);
        filters[1] = new EqualTo(AccumuloStoreRelation.VERTEX_COL_NAME, "0");
        filters[2] = new LessThan("property4", 8);
        converter = new FiltersToOperationConverter(sqlContext, graph.getView(), graph.getSchema(), filters);
        operation = converter.getOperation();
        rdd = graph.execute(operation, new User());
        results.clear();
        returnedElements = (Element[]) rdd.collect();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        expectedResults.clear();
        for (final Element element : getElements()) {
            if (element.getProperties().containsKey("property1") && element.getProperties().containsKey("property4")) {
                if ((Integer) element.getProperty("property1") > 5 && (Long) element.getProperty("property4") < 8) {
                    if (element instanceof Entity && ((Entity) element).getVertex().equals("0")) {
                        expectedResults.add(element);
                    }
                }
            }
        }
        assertEquals(expectedResults, results);

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testSpecifySourceOrDestinationAndPropertyFilter() throws OperationException {
        final Graph graph = getGraph();
        final SQLContext sqlContext = getSqlContext("testSpecifyVertexAndPropertyFilter");

        Filter[] filters = new Filter[2];
        filters[0] = new GreaterThan("property1", 5);
        filters[1] = new EqualTo(AccumuloStoreRelation.SRC_COL_NAME, "0");
        FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext, graph.getView(),
                graph.getSchema(), filters);
        AbstractGetRDD<?> operation = converter.getOperation();
        RDD<Element> rdd = graph.execute(operation, new User());
        final Set<Element> results = new HashSet<>();
        Element[] returnedElements = (Element[]) rdd.collect();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        final Set<Element> expectedResults = new HashSet<>();
        for (final Element element : getElements()) {
            if (element instanceof Edge
                    && ((Edge) element).getSource().equals("0")
                    && (Integer) element.getProperty("property1") > 5) {
                expectedResults.add(element);
            }
        }
        assertEquals(expectedResults, results);

        filters = new Filter[3];
        filters[0] = new GreaterThan("property1", 5);
        filters[1] = new EqualTo(AccumuloStoreRelation.SRC_COL_NAME, "0");
        filters[2] = new LessThan("property4", 8);
        converter = new FiltersToOperationConverter(sqlContext, graph.getView(), graph.getSchema(), filters);
        operation = converter.getOperation();
        rdd = graph.execute(operation, new User());
        results.clear();
        returnedElements = (Element[]) rdd.collect();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        expectedResults.clear();
        for (final Element element : getElements()) {
            if (element.getProperties().containsKey("property1") && element.getProperties().containsKey("property4")) {
                if ((Integer) element.getProperty("property1") > 5 && (Long) element.getProperty("property4") < 8) {
                    if (element instanceof Edge && ((Edge) element).getSource().equals("0")) {
                        expectedResults.add(element);
                    }
                }
            }
        }
        assertEquals(expectedResults, results);

        sqlContext.sparkContext().stop();
    }

    private Graph getGraph() throws OperationException {
        final Graph graph = new Graph.Builder()
                .addSchema(getClass().getResourceAsStream("/schema-DataFrame/dataSchema.json"))
                .addSchema(getClass().getResourceAsStream("/schema-DataFrame/dataTypes.json"))
                .addSchema(getClass().getResourceAsStream("/schema-DataFrame/storeTypes.json"))
                .storeProperties(getClass().getResourceAsStream("/store.properties"))
                .build();
        graph.execute(new AddElements(getElements()), new User());
        return graph;
    }

    private SQLContext getSqlContext(final String appName) {
        final SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName(appName)
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "uk.gov.gchq.gaffer.spark.serialisation.kryo.Registrator")
                .set("spark.driver.allowMultipleContexts", "true");
        return new SQLContext(new SparkContext(sparkConf));
    }

    static List<Element> getElements() {
        final List<Element> elements = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final Entity entity = new Entity(GetDataFrameOfElementsHandlerTest.ENTITY_GROUP);
            entity.setVertex("" + i);
            entity.putProperty("columnQualifier", 1);
            entity.putProperty("property1", i);
            entity.putProperty("property2", 3.0F);
            entity.putProperty("property3", 4.0D);
            entity.putProperty("property4", i * 2L);
            entity.putProperty("count", 6L);

            final Edge edge1 = new Edge(GetDataFrameOfElementsHandlerTest.EDGE_GROUP);
            edge1.setSource("" + i);
            edge1.setDestination("B");
            edge1.setDirected(true);
            edge1.putProperty("columnQualifier", 1);
            edge1.putProperty("property1", 2);
            edge1.putProperty("property2", 3.0F);
            edge1.putProperty("property3", 4.0D);
            edge1.putProperty("property4", 5L);
            edge1.putProperty("count", 100L);

            final Edge edge2 = new Edge(GetDataFrameOfElementsHandlerTest.EDGE_GROUP);
            edge2.setSource("" + i);
            edge2.setDestination("C");
            edge2.setDirected(true);
            edge2.putProperty("columnQualifier", 6);
            edge2.putProperty("property1", 7);
            edge2.putProperty("property2", 8.0F);
            edge2.putProperty("property3", 9.0D);
            edge2.putProperty("property4", 10L);
            edge2.putProperty("count", i * 200L);

            final Edge edge3 = new Edge(GetDataFrameOfElementsHandlerTest.EDGE_GROUP2);
            edge3.setSource("" + i);
            edge3.setDestination("D");
            edge3.setDirected(false);
            edge3.putProperty("property1", 1000);

            elements.add(edge1);
            elements.add(edge2);
            elements.add(edge3);
            elements.add(entity);
        }
        return elements;
    }
}
