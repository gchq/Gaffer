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
package uk.gov.gchq.gaffer.spark.operation.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.Or;
import org.junit.Test;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.function.FilterFunction;
import uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext;
import uk.gov.gchq.gaffer.function.filter.IsLessThan;
import uk.gov.gchq.gaffer.function.filter.IsMoreThan;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.schema.SchemaToStructTypeConverter;
import uk.gov.gchq.gaffer.spark.operation.scalardd.AbstractGetRDD;
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfAllElements;
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfElements;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FilterToOperationConverterTest {
    private static final String ENTITY_GROUP = "BasicEntity";
    private static final String EDGE_GROUP = "BasicEdge";
    private static final String EDGE_GROUP2 = "BasicEdge2";
    private static final Set<String> EDGE_GROUPS = new HashSet<>(Arrays.asList(EDGE_GROUP, EDGE_GROUP2));

    @Test
    public void testIncompatibleGroups() throws OperationException {
        final Schema schema = getSchema();
        final SQLContext sqlContext = getSqlContext("testIncompatibleGroups");

        final Filter[] filters = new Filter[2];
        filters[0] = new EqualTo(SchemaToStructTypeConverter.GROUP, "A");
        filters[1] = new EqualTo(SchemaToStructTypeConverter.GROUP, "B");
        final FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext,
                getViewFromSchema(schema), schema, filters);

        final AbstractGetRDD<?> operation = converter.getOperation();
        assertNull(operation);

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testSingleGroup() throws OperationException {
        final Schema schema = getSchema();
        final SQLContext sqlContext = getSqlContext("testSingleGroup");

        final Filter[] filters = new Filter[1];
        filters[0] = new EqualTo(SchemaToStructTypeConverter.GROUP, ENTITY_GROUP);

        final FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext,
                getViewFromSchema(schema), schema, filters);

        final AbstractGetRDD<?> operation = converter.getOperation();
        assertTrue(operation instanceof GetRDDOfAllElements);
        assertEquals(Collections.singleton(ENTITY_GROUP), operation.getView().getEntityGroups());
        assertEquals(0, operation.getView().getEdgeGroups().size());

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testSingleGroupNotInSchema() throws OperationException {
        final Schema schema = getSchema();
        final SQLContext sqlContext = getSqlContext("testSingleGroupNotInSchema");

        final Filter[] filters = new Filter[1];
        filters[0] = new EqualTo(SchemaToStructTypeConverter.GROUP, "random");
        final FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext,
                getViewFromSchema(schema), schema, filters);

        final AbstractGetRDD<?> operation = converter.getOperation();
        assertNull(operation);

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testTwoGroups() throws OperationException {
        final Schema schema = getSchema();
        final SQLContext sqlContext = getSqlContext("testTwoGroups");

        final Filter[] filters = new Filter[1];
        final Filter left = new EqualTo(SchemaToStructTypeConverter.GROUP, ENTITY_GROUP);
        final Filter right = new EqualTo(SchemaToStructTypeConverter.GROUP, EDGE_GROUP2);
        filters[0] = new Or(left, right);
        final FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext,
                getViewFromSchema(schema), schema, filters);

        final AbstractGetRDD<?> operation = converter.getOperation();
        assertTrue(operation instanceof GetRDDOfAllElements);
        assertEquals(Collections.singleton(ENTITY_GROUP), operation.getView().getEntityGroups());
        assertEquals(Collections.singleton(EDGE_GROUP2), operation.getView().getEdgeGroups());

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testSpecifyVertex() throws OperationException {
        final Schema schema = getSchema();
        final SQLContext sqlContext = getSqlContext("testSpecifyVertex");

        final Filter[] filters = new Filter[1];
        filters[0] = new EqualTo(SchemaToStructTypeConverter.VERTEX_COL_NAME, "0");
        final FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext,
                getViewFromSchema(schema), schema, filters);

        final AbstractGetRDD<?> operation = converter.getOperation();
        assertTrue(operation instanceof GetRDDOfElements);
        assertEquals(Collections.singleton(ENTITY_GROUP), operation.getView().getEntityGroups());
        assertEquals(0, operation.getView().getEdgeGroups().size());
        final Set<EntitySeed> seeds = new HashSet<>();
        for (final Object seed : ((GetRDDOfElements) operation).getSeeds()) {
            seeds.add((EntitySeed) seed);
        }
        assertEquals(Collections.singleton(new EntitySeed("0")), seeds);

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testSpecifySource() throws OperationException {
        final Schema schema = getSchema();
        final SQLContext sqlContext = getSqlContext("testSpecifySource");

        final Filter[] filters = new Filter[1];
        filters[0] = new EqualTo(SchemaToStructTypeConverter.SRC_COL_NAME, "0");
        FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext, getViewFromSchema(schema),
                schema, filters);

        AbstractGetRDD<?> operation = converter.getOperation();
        assertTrue(operation instanceof GetRDDOfElements);
        assertEquals(0, operation.getView().getEntityGroups().size());
        assertEquals(EDGE_GROUPS, operation.getView().getEdgeGroups());
        final Set<EntitySeed> seeds = new HashSet<>();
        for (final Object seed : ((GetRDDOfElements) operation).getSeeds()) {
            seeds.add((EntitySeed) seed);
        }
        assertEquals(Collections.singleton(new EntitySeed("0")), seeds);

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testSpecifyDestination() throws OperationException {
        final Schema schema = getSchema();
        final SQLContext sqlContext = getSqlContext("testSpecifyDestination");

        final Filter[] filters = new Filter[1];
        filters[0] = new EqualTo(SchemaToStructTypeConverter.DST_COL_NAME, "0");
        final FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext,
                getViewFromSchema(schema), schema, filters);

        final AbstractGetRDD<?> operation = converter.getOperation();
        assertTrue(operation instanceof GetRDDOfElements);
        assertEquals(0, operation.getView().getEntityGroups().size());
        assertEquals(EDGE_GROUPS, operation.getView().getEdgeGroups());
        final Set<EntitySeed> seeds = new HashSet<>();
        for (final Object seed : ((GetRDDOfElements) operation).getSeeds()) {
            seeds.add((EntitySeed) seed);
        }
        assertEquals(Collections.singleton(new EntitySeed("0")), seeds);

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testSpecifyPropertyFilters() throws OperationException {
        final Schema schema = getSchema();
        final SQLContext sqlContext = getSqlContext("testSpecifyPropertyFilters");
        final Filter[] filters = new Filter[1];

        // GreaterThan
        filters[0] = new GreaterThan("property1", 5);
        FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext, getViewFromSchema(schema),
                schema, filters);
        AbstractGetRDD<?> operation = converter.getOperation();

        assertTrue(operation instanceof GetRDDOfAllElements);
        View opView = operation.getView();
        List<ConsumerFunctionContext<String, FilterFunction>> entityPostAggFilters = opView
                .getEntity(ENTITY_GROUP).getPostAggregationFilterFunctions();
        assertEquals(1, entityPostAggFilters.size());
        assertEquals(new ArrayList<>(Collections.singleton("property1")), entityPostAggFilters.get(0).getSelection());
        assertEquals(new IsMoreThan(5, false), entityPostAggFilters.get(0).getFunction());
        for (final String edgeGroup : EDGE_GROUPS) {
            final List<ConsumerFunctionContext<String, FilterFunction>> edgePostAggFilters = opView
                    .getEdge(edgeGroup).getPostAggregationFilterFunctions();
            assertEquals(1, edgePostAggFilters.size());
            assertEquals(new ArrayList<>(Collections.singleton("property1")), edgePostAggFilters.get(0).getSelection());
            assertEquals(new IsMoreThan(5, false), edgePostAggFilters.get(0).getFunction());
        }

        // LessThan
        filters[0] = new LessThan("property4", 8L);
        converter = new FiltersToOperationConverter(sqlContext, getViewFromSchema(schema), schema, filters);
        operation = converter.getOperation();

        assertTrue(operation instanceof GetRDDOfAllElements);
        // Only groups ENTITY_GROUP and EDGE_GROUP should be in the view as only they have property4
        opView = operation.getView();
        entityPostAggFilters = opView
                .getEntity(ENTITY_GROUP).getPostAggregationFilterFunctions();
        assertEquals(1, entityPostAggFilters.size());
        assertEquals(new ArrayList<>(Collections.singleton("property4")), entityPostAggFilters.get(0).getSelection());
        assertEquals(new IsLessThan(8L, false), entityPostAggFilters.get(0).getFunction());
        List<ConsumerFunctionContext<String, FilterFunction>> edgePostAggFilters = opView.getEdge(EDGE_GROUP)
                .getPostAggregationFilterFunctions();
        assertEquals(1, edgePostAggFilters.size());
        assertEquals(new ArrayList<>(Collections.singleton("property4")), edgePostAggFilters.get(0).getSelection());
        assertEquals(new IsLessThan(8L, false), edgePostAggFilters.get(0).getFunction());

        // And
        final Filter left = new GreaterThan("property1", 5);
        final Filter right = new GreaterThan("property4", 8L);
        filters[0] = new And(left, right);
        converter = new FiltersToOperationConverter(sqlContext, getViewFromSchema(schema), schema, filters);
        operation = converter.getOperation();

        assertTrue(operation instanceof GetRDDOfAllElements);
        // Only groups ENTITY_GROUP and EDGE_GROUP should be in the view as only they have property1 and property4
        opView = operation.getView();
        entityPostAggFilters = opView.getEntity(ENTITY_GROUP).getPostAggregationFilterFunctions();
        assertEquals(2, entityPostAggFilters.size());
        final ArrayList<String> expectedProperties = new ArrayList<>();
        expectedProperties.add("property1");
        expectedProperties.add("property4");
        assertEquals(1, entityPostAggFilters.get(0).getSelection().size());
        assertEquals(expectedProperties.get(0), entityPostAggFilters.get(0).getSelection().get(0));
        assertEquals(1, entityPostAggFilters.get(1).getSelection().size());
        assertEquals(expectedProperties.get(1), entityPostAggFilters.get(1).getSelection().get(0));
        final ArrayList<FilterFunction> expectedFunctions = new ArrayList<>();
        expectedFunctions.add(new IsMoreThan(5, false));
        expectedFunctions.add(new IsMoreThan(8L, false));
        assertEquals(expectedFunctions.get(0), entityPostAggFilters.get(0).getFunction());
        assertEquals(expectedFunctions.get(1), entityPostAggFilters.get(1).getFunction());
        edgePostAggFilters = opView.getEdge(EDGE_GROUP).getPostAggregationFilterFunctions();
        assertEquals(2, edgePostAggFilters.size());
        assertEquals(1, edgePostAggFilters.get(0).getSelection().size());
        assertEquals(expectedProperties.get(0), edgePostAggFilters.get(0).getSelection().get(0));
        assertEquals(1, edgePostAggFilters.get(1).getSelection().size());
        assertEquals(expectedProperties.get(1), edgePostAggFilters.get(1).getSelection().get(0));

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testSpecifyMultiplePropertyFilters() throws OperationException {
        final Schema schema = getSchema();
        final SQLContext sqlContext = getSqlContext("testSpecifyMultiplePropertyFilters");

        final Filter[] filters = new Filter[2];
        filters[0] = new GreaterThan("property1", 5);
        filters[1] = new LessThan("property4", 8L);
        FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext, getViewFromSchema(schema),
                schema, filters);
        AbstractGetRDD<?> operation = converter.getOperation();

        assertTrue(operation instanceof GetRDDOfAllElements);
        // Only groups ENTITY_GROUP and EDGE_GROUP should be in the view as only they have property1 and property4
        View opView = operation.getView();
        List<ConsumerFunctionContext<String, FilterFunction>> entityPostAggFilters = opView.getEntity(ENTITY_GROUP)
                .getPostAggregationFilterFunctions();
        assertEquals(2, entityPostAggFilters.size());
        final ArrayList<String> expectedProperties = new ArrayList<>();
        expectedProperties.add("property1");
        expectedProperties.add("property4");
        assertEquals(1, entityPostAggFilters.get(0).getSelection().size());
        assertEquals(expectedProperties.get(0), entityPostAggFilters.get(0).getSelection().get(0));
        assertEquals(1, entityPostAggFilters.get(1).getSelection().size());
        assertEquals(expectedProperties.get(1), entityPostAggFilters.get(1).getSelection().get(0));
        final ArrayList<FilterFunction> expectedFunctions = new ArrayList<>();
        expectedFunctions.add(new IsMoreThan(5, false));
        expectedFunctions.add(new IsLessThan(8L, false));
        assertEquals(expectedFunctions.get(0), entityPostAggFilters.get(0).getFunction());
        assertEquals(expectedFunctions.get(1), entityPostAggFilters.get(1).getFunction());
        final List<ConsumerFunctionContext<String, FilterFunction>> edgePostAggFilters = opView.getEdge(EDGE_GROUP)
                .getPostAggregationFilterFunctions();
        assertEquals(2, edgePostAggFilters.size());
        assertEquals(1, edgePostAggFilters.get(0).getSelection().size());
        assertEquals(expectedProperties.get(0), edgePostAggFilters.get(0).getSelection().get(0));
        assertEquals(1, edgePostAggFilters.get(1).getSelection().size());
        assertEquals(expectedProperties.get(1), edgePostAggFilters.get(1).getSelection().get(0));

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testSpecifyVertexAndPropertyFilter() throws OperationException {
        final Schema schema = getSchema();
        final SQLContext sqlContext = getSqlContext("testSpecifyVertexAndPropertyFilter");

        // Specify vertex and a filter on property1
        Filter[] filters = new Filter[2];
        filters[0] = new GreaterThan("property1", 5);
        filters[1] = new EqualTo(SchemaToStructTypeConverter.VERTEX_COL_NAME, "0");
        FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext, getViewFromSchema(schema),
                schema, filters);
        AbstractGetRDD<?> operation = converter.getOperation();

        assertTrue(operation instanceof GetRDDOfElements);
        assertEquals(1, operation.getView().getEntityGroups().size());
        assertEquals(0, operation.getView().getEdgeGroups().size());
        final Set<EntitySeed> seeds = new HashSet<>();
        for (final Object seed : ((GetRDDOfElements) operation).getSeeds()) {
            seeds.add((EntitySeed) seed);
        }
        assertEquals(Collections.singleton(new EntitySeed("0")), seeds);
        View opView = operation.getView();
        List<ConsumerFunctionContext<String, FilterFunction>> entityPostAggFilters = opView
                .getEntity(ENTITY_GROUP).getPostAggregationFilterFunctions();
        assertEquals(1, entityPostAggFilters.size());
        final ArrayList<String> expectedProperties = new ArrayList<>();
        expectedProperties.add("property1");
        assertEquals(1, entityPostAggFilters.get(0).getSelection().size());
        assertEquals(expectedProperties.get(0), entityPostAggFilters.get(0).getSelection().get(0));
        final ArrayList<FilterFunction> expectedFunctions = new ArrayList<>();
        expectedFunctions.add(new IsMoreThan(5, false));
        assertEquals(expectedFunctions.get(0), entityPostAggFilters.get(0).getFunction());

        // Specify vertex and filters on properties property1 and property4
        filters = new Filter[3];
        filters[0] = new GreaterThan("property1", 5);
        filters[1] = new EqualTo(SchemaToStructTypeConverter.VERTEX_COL_NAME, "0");
        filters[2] = new LessThan("property4", 8);
        converter = new FiltersToOperationConverter(sqlContext, getViewFromSchema(schema), schema, filters);
        operation = converter.getOperation();
        assertTrue(operation instanceof GetRDDOfElements);
        assertEquals(1, operation.getView().getEntityGroups().size());
        assertEquals(0, operation.getView().getEdgeGroups().size());
        seeds.clear();
        for (final Object seed : ((GetRDDOfElements) operation).getSeeds()) {
            seeds.add((EntitySeed) seed);
        }
        assertEquals(Collections.singleton(new EntitySeed("0")), seeds);
        opView = operation.getView();
        entityPostAggFilters = opView.getEntity(ENTITY_GROUP)
                .getPostAggregationFilterFunctions();
        assertEquals(2, entityPostAggFilters.size());
        expectedProperties.clear();
        expectedProperties.add("property1");
        expectedProperties.add("property4");
        assertEquals(1, entityPostAggFilters.get(0).getSelection().size());
        assertEquals(expectedProperties.get(0), entityPostAggFilters.get(0).getSelection().get(0));
        assertEquals(1, entityPostAggFilters.get(1).getSelection().size());
        assertEquals(expectedProperties.get(1), entityPostAggFilters.get(1).getSelection().get(0));

        expectedFunctions.clear();
        expectedFunctions.add(new IsMoreThan(5, false));
        expectedFunctions.add(new IsLessThan(8, false));
        assertEquals(expectedFunctions.get(0), entityPostAggFilters.get(0).getFunction());
        assertEquals(expectedFunctions.get(1), entityPostAggFilters.get(1).getFunction());

        sqlContext.sparkContext().stop();
    }

    @Test
    public void testSpecifySourceOrDestinationAndPropertyFilter() throws OperationException {
        final Schema schema = getSchema();
        final SQLContext sqlContext = getSqlContext("testSpecifyVertexAndPropertyFilter");

        // Specify src and a filter on property1
        Filter[] filters = new Filter[2];
        filters[0] = new GreaterThan("property1", 5);
        filters[1] = new EqualTo(SchemaToStructTypeConverter.SRC_COL_NAME, "0");
        FiltersToOperationConverter converter = new FiltersToOperationConverter(sqlContext, getViewFromSchema(schema),
                schema, filters);
        AbstractGetRDD<?> operation = converter.getOperation();

        assertTrue(operation instanceof GetRDDOfElements);
        assertEquals(0, operation.getView().getEntityGroups().size());
        assertEquals(2, operation.getView().getEdgeGroups().size());
        final Set<EntitySeed> seeds = new HashSet<>();
        for (final Object seed : ((GetRDDOfElements) operation).getSeeds()) {
            seeds.add((EntitySeed) seed);
        }
        assertEquals(Collections.singleton(new EntitySeed("0")), seeds);
        View opView = operation.getView();
        for (final String edgeGroup : EDGE_GROUPS) {
            final List<ConsumerFunctionContext<String, FilterFunction>> edgePostAggFilters = opView
                    .getEdge(edgeGroup).getPostAggregationFilterFunctions();
            assertEquals(1, edgePostAggFilters.size());
            assertEquals(new ArrayList<>(Collections.singleton("property1")), edgePostAggFilters.get(0).getSelection());
            assertEquals(new IsMoreThan(5, false), edgePostAggFilters.get(0).getFunction());
        }

        // Specify src and filters on property1 and property4
        filters = new Filter[3];
        filters[0] = new GreaterThan("property1", 5);
        filters[1] = new EqualTo(SchemaToStructTypeConverter.SRC_COL_NAME, "0");
        filters[2] = new LessThan("property4", 8);
        converter = new FiltersToOperationConverter(sqlContext, getViewFromSchema(schema), schema, filters);
        operation = converter.getOperation();

        assertTrue(operation instanceof GetRDDOfElements);
        assertEquals(0, operation.getView().getEntityGroups().size());
        assertEquals(1, operation.getView().getEdgeGroups().size());
        seeds.clear();
        for (final Object seed : ((GetRDDOfElements) operation).getSeeds()) {
            seeds.add((EntitySeed) seed);
        }
        assertEquals(Collections.singleton(new EntitySeed("0")), seeds);
        opView = operation.getView();
        final List<ConsumerFunctionContext<String, FilterFunction>> entityPostAggFilters = opView
                .getEdge(EDGE_GROUP).getPostAggregationFilterFunctions();
        assertEquals(2, entityPostAggFilters.size());
        final List<String> expectedProperties = new ArrayList<>();
        expectedProperties.add("property1");
        expectedProperties.add("property4");
        assertEquals(1, entityPostAggFilters.get(0).getSelection().size());
        assertEquals(expectedProperties.get(0), entityPostAggFilters.get(0).getSelection().get(0));
        assertEquals(new IsMoreThan(5, false), entityPostAggFilters.get(0).getFunction());
        assertEquals(1, entityPostAggFilters.get(1).getSelection().size());
        assertEquals(expectedProperties.get(1), entityPostAggFilters.get(1).getSelection().get(0));
        assertEquals(new IsLessThan(8, false), entityPostAggFilters.get(1).getFunction());

        sqlContext.sparkContext().stop();
    }

    private Schema getSchema() {
        return Schema.fromJson(
                getClass().getResourceAsStream("/schema-DataFrame/dataSchema.json"),
                getClass().getResourceAsStream("/schema-DataFrame/dataTypes.json"),
                getClass().getResourceAsStream("/schema-DataFrame/storeTypes.json"));
    }

    private View getViewFromSchema(final Schema schema) {
        return new View.Builder()
                .entities(schema.getEntityGroups())
                .edges(schema.getEdgeGroups())
                .build();
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
}
