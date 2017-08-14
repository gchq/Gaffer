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
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.spark.SparkConstants;
import uk.gov.gchq.gaffer.spark.operation.dataframe.ConvertElementToRow;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.schema.SchemaToStructTypeConverter;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

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
        final List<TupleAdaptedPredicate<String, ?>> filters = new ArrayList<>();
        filters.add(new TupleAdaptedPredicate<>(new IsMoreThan(5, false), new String[]{"property1"}));
        final View view = new View.Builder()
                .edge(GetDataFrameOfElementsHandlerTest.EDGE_GROUP, new ViewElementDefinition.Builder()
                        .postAggregationFilterFunctions(filters)
                        .build())
                .build();

        final Predicate<Element> returnElement = (Element element) ->
                element.getGroup().equals(GetDataFrameOfElementsHandlerTest.EDGE_GROUP)
                        && (Integer) element.getProperty("property1") > 5;
        testBuildScanWithView("testBuildScanRestrictViewByProperty", view, returnElement);
    }

    private void testBuildScanWithView(final String name, final View view, final Predicate<Element> returnElement)
            throws OperationException, StoreException {
        // Given
        final SparkSession sparkSession = SparkSession.builder().config(getSparkConf(name)).getOrCreate();
        final Schema schema = getSchema();
        final AccumuloProperties properties = AccumuloProperties
                .loadStoreProperties(AccumuloStoreRelationTest.class.getResourceAsStream("/store.properties"));
        final SingleUseMockAccumuloStore store = new SingleUseMockAccumuloStore();
        store.initialise("graphId", schema, properties);
        addElements(store);

        // When
        final AccumuloStoreRelation relation = new AccumuloStoreRelation(sparkSession, Collections.emptyList(), view,
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
        Streams.toStream(getElements())
                .filter(returnElement)
                .map(elementConverter::apply)
                .forEach(expectedRows::add);
        assertEquals(expectedRows, results);

        sparkSession.sparkContext().stop();
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
        final SparkSession sparkSession = SparkSession.builder().config(getSparkConf(name)).getOrCreate();
        final Schema schema = getSchema();
        final AccumuloProperties properties = AccumuloProperties
                .loadStoreProperties(getClass().getResourceAsStream("/store.properties"));
        final SingleUseMockAccumuloStore store = new SingleUseMockAccumuloStore();
        store.initialise("graphId", schema, properties);
        addElements(store);

        // When
        final AccumuloStoreRelation relation = new AccumuloStoreRelation(sparkSession, Collections.emptyList(), view,
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
        Streams.toStream(getElements())
                .filter(returnElement)
                .map(elementConverter::apply)
                .forEach(expectedRows::add);
        assertEquals(expectedRows, results);

        sparkSession.sparkContext().stop();
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
        final SparkSession sparkSession = SparkSession.builder().config(getSparkConf(name)).getOrCreate();
        final Schema schema = getSchema();
        final AccumuloProperties properties = AccumuloProperties
                .loadStoreProperties(getClass().getResourceAsStream("/store.properties"));
        final SingleUseMockAccumuloStore store = new SingleUseMockAccumuloStore();
        store.initialise("graphId", schema, properties);
        addElements(store);

        // When
        final AccumuloStoreRelation relation = new AccumuloStoreRelation(sparkSession, Collections.emptyList(), view,
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
        Streams.toStream(getElements())
                .filter(returnElement)
                .map(elementConverter::apply)
                .forEach(expectedRows::add);
        assertEquals(expectedRows, results);

        sparkSession.sparkContext().stop();
    }

    private static Schema getSchema() {
        return Schema.fromJson(
                AccumuloStoreRelationTest.class.getResourceAsStream("/schema-DataFrame/elements.json"),
                AccumuloStoreRelationTest.class.getResourceAsStream("/schema-DataFrame/types.json"),
                AccumuloStoreRelationTest.class.getResourceAsStream("/schema-DataFrame/serialisation.json"));
    }

    private static View getViewFromSchema(final Schema schema) {
        return new View.Builder()
                .entities(schema.getEntityGroups())
                .edges(schema.getEdgeGroups())
                .build();
    }

    private static void addElements(final Store store) throws OperationException {
        store.execute(new AddElements.Builder().input(getElements()).build(), new User());
    }

    private SparkConf getSparkConf(final String appName) {
        return new SparkConf()
                .setMaster("local")
                .setAppName(appName)
                .set(SparkConstants.SERIALIZER, SparkConstants.DEFAULT_SERIALIZER)
                .set(SparkConstants.KRYO_REGISTRATOR, SparkConstants.DEFAULT_KRYO_REGISTRATOR)
                .set(SparkConstants.DRIVER_ALLOW_MULTIPLE_CONTEXTS, "true");
    }

    private static List<Element> getElements() {
        final List<Element> elements = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final Entity entity = new Entity.Builder()
                    .group(GetDataFrameOfElementsHandlerTest.ENTITY_GROUP)
                    .vertex("" + i)
                    .property("columnQualifier", 1)
                    .property("property1", i)
                    .property("property2", 3.0F)
                    .property("property3", 4.0D)
                    .property("property4", i * 2L)
                    .property("count", 6L)
                    .build();

            final Edge edge1 = new Edge.Builder()
                    .group(GetDataFrameOfElementsHandlerTest.EDGE_GROUP)
                    .source("" + i)
                    .dest("B")
                    .directed(true)
                    .property("columnQualifier", 1)
                    .property("property1", 2)
                    .property("property2", 3.0F)
                    .property("property3", 4.0D)
                    .property("property4", 5L)
                    .property("count", 100L)
                    .build();

            final Edge edge2 = new Edge.Builder()
                    .group(GetDataFrameOfElementsHandlerTest.EDGE_GROUP)
                    .source("" + i)
                    .dest("C")
                    .directed(true)
                    .property("columnQualifier", 6)
                    .property("property1", 7)
                    .property("property2", 8.0F)
                    .property("property3", 9.0D)
                    .property("property4", 10L)
                    .property("count", i * 200L)
                    .build();

            final Edge edge3 = new Edge.Builder()
                    .group(GetDataFrameOfElementsHandlerTest.EDGE_GROUP2)
                    .source("" + i)
                    .dest("D")
                    .directed(true)
                    .property("property1", 1000)
                    .build();

            elements.add(edge1);
            elements.add(edge2);
            elements.add(edge3);
            elements.add(entity);
        }
        return elements;
    }
}
