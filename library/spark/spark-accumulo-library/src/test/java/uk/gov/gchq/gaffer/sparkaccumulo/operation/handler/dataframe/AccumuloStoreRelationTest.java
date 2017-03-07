/*
 * Copyright 2016-2017 Crown Copyright
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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.function.FilterFunction;
import uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext;
import uk.gov.gchq.gaffer.function.filter.IsMoreThan;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.spark.operation.dataframe.ConvertElementToRow;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.schema.SchemaToStructTypeConverter;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;

/**
 * Contains unit tests for {@link AccumuloStoreRelation}.
 */
public class AccumuloStoreRelationTest {

    @Test
    public void testBuildScanFullView() throws OperationException, StoreException {
        final Schema schema = getSchema();
        final View view = getViewFromSchema(schema);

        testBuildScanWithView("testBuildScanFullView", view, e -> true);
    }

    @Test
    public void testBuildScanRestrictViewToOneGroup() throws OperationException, StoreException {
        final View view = new View.Builder()
                .edge(GetDataFrameOfElementsHandlerTest.EDGE_GROUP)
                .build();

        final Predicate<Element> returnElement = (Element element) ->
                element.getGroup().equals(GetDataFrameOfElementsHandlerTest.EDGE_GROUP);
        testBuildScanWithView("testBuildScanRestrictViewToOneGroup", view, returnElement);
    }

    @Test
    public void testBuildScanRestrictViewByProperty() throws OperationException, StoreException {
        final List<ConsumerFunctionContext<String, FilterFunction>> filters = new ArrayList<>();
        filters.add(new ConsumerFunctionContext<>(new IsMoreThan(5, false), new ArrayList<>(Arrays.asList("property1"))));
        final View view = new View.Builder()
                .edge(GetDataFrameOfElementsHandlerTest.EDGE_GROUP, new ViewElementDefinition.Builder()
                        .postAggregationFilterFunctions(filters)
                        .build())
                .build();

        final Predicate<Element> returnElement = (Element element) ->
                element.getGroup().equals(GetDataFrameOfElementsHandlerTest.EDGE_GROUP)
                && ((Integer) element.getProperty("property1")) > 5;
        testBuildScanWithView("testBuildScanRestrictViewByProperty", view, returnElement);
    }

    private void testBuildScanWithView(final String name, final View view, final Predicate<Element> returnElement)
            throws OperationException, StoreException {
        // Given
        final SQLContext sqlContext = getSqlContext(name);
        final Schema schema = getSchema();
        final AccumuloProperties properties = AccumuloProperties
                .loadStoreProperties(AccumuloStoreRelationTest.class.getResourceAsStream("/store.properties"));
        final SingleUseMockAccumuloStore store = new SingleUseMockAccumuloStore();
        store.initialise(schema, properties);
        addElements(store);

        // When
        final AccumuloStoreRelation relation = new AccumuloStoreRelation(sqlContext, Collections.emptyList(), view,
                store, new User());
        final RDD<Row> rdd = relation.buildScan();
        final Row[] returnedElements = (Row[]) rdd.collect();

        // Then
        //  - Actual results are:
        final Set<Row> results = new HashSet<>();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        //  - Expected results are:
        final SchemaToStructTypeConverter schemaConverter = new SchemaToStructTypeConverter(schema, view,
                new ArrayList<>());
        final ConvertElementToRow elementConverter = new ConvertElementToRow(schemaConverter.getUsedProperties(),
                schemaConverter.getPropertyNeedsConversion(), schemaConverter.getConverterByProperty());
        final Set<Row> expectedRows = new HashSet<>();
        StreamSupport.stream(getElements().spliterator(), false)
                .filter(returnElement)
                .map(elementConverter::apply)
                .forEach(expectedRows::add);
        assertEquals(expectedRows, results);

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testBuildScanSpecifyColumnsFullView() throws OperationException, StoreException {
        final Schema schema = getSchema();
        final View view = getViewFromSchema(schema);

        final String[] requiredColumns = new String[]{"property1"};
        testBuildScanSpecifyColumnsWithView("testBuildScanSpecifyColumnsFullView", view, requiredColumns, e -> true);
    }

    private void testBuildScanSpecifyColumnsWithView(final String name, final View view, final String[] requiredColumns,
                                                     final Predicate<Element> returnElement)
            throws OperationException, StoreException {
        // Given
        final SQLContext sqlContext = getSqlContext(name);
        final Schema schema = getSchema();
        final AccumuloProperties properties = AccumuloProperties
                .loadStoreProperties(getClass().getResourceAsStream("/store.properties"));
        final SingleUseMockAccumuloStore store = new SingleUseMockAccumuloStore();
        store.initialise(schema, properties);
        addElements(store);

        // When
        final AccumuloStoreRelation relation = new AccumuloStoreRelation(sqlContext, Collections.emptyList(), view,
                store, new User());
        final RDD<Row> rdd = relation.buildScan(requiredColumns);
        final Row[] returnedElements = (Row[]) rdd.collect();

        // Then
        //  - Actual results are:
        final Set<Row> results = new HashSet<>();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        //  - Expected results are:
        final SchemaToStructTypeConverter schemaConverter = new SchemaToStructTypeConverter(schema, view,
                new ArrayList<>());
        final ConvertElementToRow elementConverter = new ConvertElementToRow(new LinkedHashSet<>(Arrays.asList(requiredColumns)),
                schemaConverter.getPropertyNeedsConversion(), schemaConverter.getConverterByProperty());
        final Set<Row> expectedRows = new HashSet<>();
        StreamSupport.stream(getElements().spliterator(), false)
                .filter(returnElement)
                .map(elementConverter::apply)
                .forEach(expectedRows::add);
        assertEquals(expectedRows, results);

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testBuildScanSpecifyColumnsAndFiltersFullView() throws OperationException, StoreException {
        final Schema schema = getSchema();
        final View view = getViewFromSchema(schema);

        final String[] requiredColumns = new String[1];
        requiredColumns[0] = "property1";
        final Filter[] filters = new Filter[1];
        filters[0] = new GreaterThan("property1", 4);
        final Predicate<Element> returnElement = (Element element) -> ((Integer) element.getProperty("property1")) > 4;
        testBuildScanSpecifyColumnsAndFiltersWithView("testBuildScanSpecifyColumnsAndFiltersFullView", view,
                requiredColumns, filters, returnElement);
    }

    private void testBuildScanSpecifyColumnsAndFiltersWithView(final String name, final View view,
                                                               final String[] requiredColumns, final Filter[] filters,
                                                               final Predicate<Element> returnElement)
            throws OperationException, StoreException {
        // Given
        final SQLContext sqlContext = getSqlContext(name);
        final Schema schema = getSchema();
        final AccumuloProperties properties = AccumuloProperties
                .loadStoreProperties(getClass().getResourceAsStream("/store.properties"));
        final SingleUseMockAccumuloStore store = new SingleUseMockAccumuloStore();
        store.initialise(schema, properties);
        addElements(store);

        // When
        final AccumuloStoreRelation relation = new AccumuloStoreRelation(sqlContext, Collections.emptyList(), view,
                store, new User());
        final RDD<Row> rdd = relation.buildScan(requiredColumns, filters);
        final Row[] returnedElements = (Row[]) rdd.collect();

        // Then
        //  - Actual results are:
        final Set<Row> results = new HashSet<>();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        //  - Expected results are:
        final SchemaToStructTypeConverter schemaConverter = new SchemaToStructTypeConverter(schema, view,
                new ArrayList<>());
        final ConvertElementToRow elementConverter = new ConvertElementToRow(new LinkedHashSet<>(Arrays.asList(requiredColumns)),
                schemaConverter.getPropertyNeedsConversion(), schemaConverter.getConverterByProperty());
        final Set<Row> expectedRows = new HashSet<>();
        StreamSupport.stream(getElements().spliterator(), false)
                .filter(returnElement)
                .map(elementConverter::apply)
                .forEach(expectedRows::add);
        assertEquals(expectedRows, results);

        sqlContext.sparkContext().stop();
    }

    private static Schema getSchema() {
        return Schema.fromJson(AccumuloStoreRelationTest.class.getResourceAsStream("/schema-DataFrame/dataSchema.json"),
                AccumuloStoreRelationTest.class.getResourceAsStream("/schema-DataFrame/dataTypes.json"),
                AccumuloStoreRelationTest.class.getResourceAsStream("/schema-DataFrame/storeTypes.json"));
    }

    private static View getViewFromSchema(final Schema schema) {
        return new View.Builder()
                .entities(schema.getEntityGroups())
                .edges(schema.getEdgeGroups())
                .build();
    }

    private static void addElements(final Store store) throws OperationException {
        store.execute(new AddElements(getElements()), new User());
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

    private static List<Element> getElements() {
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
            edge3.setDirected(true);
            edge3.putProperty("property1", 1000);

            elements.add(edge1);
            elements.add(edge2);
            elements.add(edge3);
            elements.add(entity);
        }
        return elements;
    }
}
