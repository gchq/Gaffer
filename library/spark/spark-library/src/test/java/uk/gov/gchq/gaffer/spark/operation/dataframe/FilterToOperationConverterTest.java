/*
 * Copyright 2016-2019 Crown Copyright
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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.Or;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.spark.SparkSessionProvider;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.schema.SchemaToStructTypeConverter;
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfAllElements;
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfElements;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FilterToOperationConverterTest {
    private static final String ENTITY_GROUP = "BasicEntity";
    private static final String EDGE_GROUP = "BasicEdge";
    private static final String EDGE_GROUP2 = "BasicEdge2";
    private static final Set<String> EDGE_GROUPS = new HashSet<>(Arrays.asList(EDGE_GROUP, EDGE_GROUP2));

    @Test
    public void testIncompatibleGroups() {
        final Schema schema = getSchema();
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final Filter[] filters = new Filter[2];
        filters[0] = new EqualTo(SchemaToStructTypeConverter.GROUP, "A");
        filters[1] = new EqualTo(SchemaToStructTypeConverter.GROUP, "B");
        final FiltersToOperationConverter converter = new FiltersToOperationConverter(
                getViewFromSchema(schema), schema, filters);

        final Operation operation = converter.getOperation();
        assertNull(operation);

        sparkSession.sparkContext().stop();
    }

    @Test
    public void testSingleGroup() {
        final Schema schema = getSchema();
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final Filter[] filters = new Filter[1];
        filters[0] = new EqualTo(SchemaToStructTypeConverter.GROUP, ENTITY_GROUP);

        final FiltersToOperationConverter converter = new FiltersToOperationConverter(
                getViewFromSchema(schema), schema, filters);

        final Operation operation = converter.getOperation();
        assertTrue(operation instanceof GetRDDOfAllElements);
        assertEquals(Collections.singleton(ENTITY_GROUP), ((GraphFilters) operation).getView().getEntityGroups());
        assertEquals(0, ((GraphFilters) operation).getView().getEdgeGroups().size());

        sparkSession.sparkContext().stop();
    }

    @Test
    public void testSingleGroupNotInSchema() {
        final Schema schema = getSchema();
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final Filter[] filters = new Filter[1];
        filters[0] = new EqualTo(SchemaToStructTypeConverter.GROUP, "random");
        final FiltersToOperationConverter converter = new FiltersToOperationConverter(
                getViewFromSchema(schema), schema, filters);

        final Operation operation = converter.getOperation();
        assertNull(operation);

        sparkSession.sparkContext().stop();
    }

    @Test
    public void testTwoGroups() {
        final Schema schema = getSchema();
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final Filter[] filters = new Filter[1];
        final Filter left = new EqualTo(SchemaToStructTypeConverter.GROUP, ENTITY_GROUP);
        final Filter right = new EqualTo(SchemaToStructTypeConverter.GROUP, EDGE_GROUP2);
        filters[0] = new Or(left, right);
        final FiltersToOperationConverter converter = new FiltersToOperationConverter(
                getViewFromSchema(schema), schema, filters);

        final Operation operation = converter.getOperation();
        assertTrue(operation instanceof GetRDDOfAllElements);
        assertEquals(Collections.singleton(ENTITY_GROUP), ((GraphFilters) operation).getView().getEntityGroups());
        assertEquals(Collections.singleton(EDGE_GROUP2), ((GraphFilters) operation).getView().getEdgeGroups());

        sparkSession.sparkContext().stop();
    }

    @Test
    public void testSpecifyVertex() {
        final Schema schema = getSchema();
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final Filter[] filters = new Filter[1];
        filters[0] = new EqualTo(SchemaToStructTypeConverter.VERTEX_COL_NAME, "0");
        final FiltersToOperationConverter converter = new FiltersToOperationConverter(
                getViewFromSchema(schema), schema, filters);

        final Operation operation = converter.getOperation();
        assertTrue(operation instanceof GetRDDOfElements);
        assertEquals(Collections.singleton(ENTITY_GROUP), ((GraphFilters) operation).getView().getEntityGroups());
        assertEquals(0, ((GraphFilters) operation).getView().getEdgeGroups().size());
        final Set<EntityId> seeds = new HashSet<>();
        for (final Object seed : ((GetRDDOfElements) operation).getInput()) {
            seeds.add((EntitySeed) seed);
        }
        assertEquals(Collections.singleton(new EntitySeed("0")), seeds);

        sparkSession.sparkContext().stop();
    }

    @Test
    public void testSpecifySource() {
        final Schema schema = getSchema();
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final Filter[] filters = new Filter[1];
        filters[0] = new EqualTo(SchemaToStructTypeConverter.SRC_COL_NAME, "0");
        FiltersToOperationConverter converter = new FiltersToOperationConverter(getViewFromSchema(schema),
                schema, filters);

        Operation operation = converter.getOperation();
        assertTrue(operation instanceof GetRDDOfElements);
        assertEquals(0, ((GraphFilters) operation).getView().getEntityGroups().size());
        assertEquals(EDGE_GROUPS, ((GraphFilters) operation).getView().getEdgeGroups());
        final Set<EntityId> seeds = new HashSet<>();
        for (final Object seed : ((GetRDDOfElements) operation).getInput()) {
            seeds.add((EntitySeed) seed);
        }
        assertEquals(Collections.singleton(new EntitySeed("0")), seeds);

        sparkSession.sparkContext().stop();
    }

    @Test
    public void testSpecifyDestination() {
        final Schema schema = getSchema();
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final Filter[] filters = new Filter[1];
        filters[0] = new EqualTo(SchemaToStructTypeConverter.DST_COL_NAME, "0");
        final FiltersToOperationConverter converter = new FiltersToOperationConverter(
                getViewFromSchema(schema), schema, filters);

        final Operation operation = converter.getOperation();
        assertTrue(operation instanceof GetRDDOfElements);
        assertEquals(0, ((GraphFilters) operation).getView().getEntityGroups().size());
        assertEquals(EDGE_GROUPS, ((GraphFilters) operation).getView().getEdgeGroups());
        final Set<EntityId> seeds = new HashSet<>();
        for (final Object seed : ((GetRDDOfElements) operation).getInput()) {
            seeds.add((EntitySeed) seed);
        }
        assertEquals(Collections.singleton(new EntitySeed("0")), seeds);

        sparkSession.sparkContext().stop();
    }

    @Test
    public void testSpecifyPropertyFilters() {
        final Schema schema = getSchema();
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();
        final Filter[] filters = new Filter[1];

        // GreaterThan
        filters[0] = new GreaterThan("property1", 5);
        FiltersToOperationConverter converter = new FiltersToOperationConverter(getViewFromSchema(schema),
                schema, filters);
        Operation operation = converter.getOperation();

        assertTrue(operation instanceof GetRDDOfAllElements);
        View opView = ((GraphFilters) operation).getView();
        List<TupleAdaptedPredicate<String, ?>> entityPostAggFilters = opView
                .getEntity(ENTITY_GROUP).getPostAggregationFilterFunctions();
        assertEquals(1, entityPostAggFilters.size());
        assertArrayEquals(new String[]{"property1"}, entityPostAggFilters.get(0).getSelection());
        assertEquals(new IsMoreThan(5, false), entityPostAggFilters.get(0).getPredicate());
        for (final String edgeGroup : EDGE_GROUPS) {
            final List<TupleAdaptedPredicate<String, ?>> edgePostAggFilters = opView
                    .getEdge(edgeGroup).getPostAggregationFilterFunctions();
            assertEquals(1, edgePostAggFilters.size());
            assertArrayEquals(new String[]{"property1"}, edgePostAggFilters.get(0).getSelection());
            assertEquals(new IsMoreThan(5, false), edgePostAggFilters.get(0).getPredicate());
        }

        // LessThan
        filters[0] = new LessThan("property4", 8L);
        converter = new FiltersToOperationConverter(getViewFromSchema(schema), schema, filters);
        operation = converter.getOperation();

        assertTrue(operation instanceof GetRDDOfAllElements);
        // Only groups ENTITY_GROUP and EDGE_GROUP should be in the view as only they have property4
        opView = ((GraphFilters) operation).getView();
        entityPostAggFilters = opView
                .getEntity(ENTITY_GROUP).getPostAggregationFilterFunctions();
        assertEquals(1, entityPostAggFilters.size());
        assertArrayEquals(new String[]{"property4"}, entityPostAggFilters.get(0).getSelection());
        assertEquals(new IsLessThan(8L, false), entityPostAggFilters.get(0).getPredicate());
        List<TupleAdaptedPredicate<String, ?>> edgePostAggFilters = opView.getEdge(EDGE_GROUP)
                .getPostAggregationFilterFunctions();
        assertEquals(1, edgePostAggFilters.size());
        assertArrayEquals(new String[]{"property4"}, edgePostAggFilters.get(0).getSelection());
        assertEquals(new IsLessThan(8L, false), edgePostAggFilters.get(0).getPredicate());

        // And
        final Filter left = new GreaterThan("property1", 5);
        final Filter right = new GreaterThan("property4", 8L);
        filters[0] = new And(left, right);
        converter = new FiltersToOperationConverter(getViewFromSchema(schema), schema, filters);
        operation = converter.getOperation();

        assertTrue(operation instanceof GetRDDOfAllElements);
        // Only groups ENTITY_GROUP and EDGE_GROUP should be in the view as only they have property1 and property4
        opView = ((GraphFilters) operation).getView();
        entityPostAggFilters = opView.getEntity(ENTITY_GROUP).getPostAggregationFilterFunctions();
        assertEquals(2, entityPostAggFilters.size());
        final ArrayList<String> expectedProperties = new ArrayList<>();
        expectedProperties.add("property1");
        expectedProperties.add("property4");
        assertEquals(1, entityPostAggFilters.get(0).getSelection().length);
        assertEquals(expectedProperties.get(0), entityPostAggFilters.get(0).getSelection()[0]);
        assertEquals(1, entityPostAggFilters.get(1).getSelection().length);
        assertEquals(expectedProperties.get(1), entityPostAggFilters.get(1).getSelection()[0]);
        final ArrayList<Predicate> expectedFunctions = new ArrayList<>();
        expectedFunctions.add(new IsMoreThan(5, false));
        expectedFunctions.add(new IsMoreThan(8L, false));
        assertEquals(expectedFunctions.get(0), entityPostAggFilters.get(0).getPredicate());
        assertEquals(expectedFunctions.get(1), entityPostAggFilters.get(1).getPredicate());
        edgePostAggFilters = opView.getEdge(EDGE_GROUP).getPostAggregationFilterFunctions();
        assertEquals(2, edgePostAggFilters.size());
        assertEquals(1, edgePostAggFilters.get(0).getSelection().length);
        assertEquals(expectedProperties.get(0), edgePostAggFilters.get(0).getSelection()[0]);
        assertEquals(1, edgePostAggFilters.get(1).getSelection().length);
        assertEquals(expectedProperties.get(1), edgePostAggFilters.get(1).getSelection()[0]);

        sparkSession.sparkContext().stop();
    }

    @Test
    public void testSpecifyMultiplePropertyFilters() {
        final Schema schema = getSchema();
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final Filter[] filters = new Filter[2];
        filters[0] = new GreaterThan("property1", 5);
        filters[1] = new LessThan("property4", 8L);
        FiltersToOperationConverter converter = new FiltersToOperationConverter(getViewFromSchema(schema),
                schema, filters);
        Operation operation = converter.getOperation();

        assertTrue(operation instanceof GetRDDOfAllElements);
        // Only groups ENTITY_GROUP and EDGE_GROUP should be in the view as only they have property1 and property4
        View opView = ((GraphFilters) operation).getView();
        List<TupleAdaptedPredicate<String, ?>> entityPostAggFilters = opView.getEntity(ENTITY_GROUP)
                .getPostAggregationFilterFunctions();
        assertEquals(2, entityPostAggFilters.size());
        final ArrayList<String> expectedProperties = new ArrayList<>();
        expectedProperties.add("property1");
        expectedProperties.add("property4");
        assertEquals(1, entityPostAggFilters.get(0).getSelection().length);
        assertEquals(expectedProperties.get(0), entityPostAggFilters.get(0).getSelection()[0]);
        assertEquals(1, entityPostAggFilters.get(1).getSelection().length);
        assertEquals(expectedProperties.get(1), entityPostAggFilters.get(1).getSelection()[0]);
        final ArrayList<Predicate> expectedFunctions = new ArrayList<>();
        expectedFunctions.add(new IsMoreThan(5, false));
        expectedFunctions.add(new IsLessThan(8L, false));
        assertEquals(expectedFunctions.get(0), entityPostAggFilters.get(0).getPredicate());
        assertEquals(expectedFunctions.get(1), entityPostAggFilters.get(1).getPredicate());
        final List<TupleAdaptedPredicate<String, ?>> edgePostAggFilters = opView.getEdge(EDGE_GROUP)
                .getPostAggregationFilterFunctions();
        assertEquals(2, edgePostAggFilters.size());
        assertEquals(1, edgePostAggFilters.get(0).getSelection().length);
        assertEquals(expectedProperties.get(0), edgePostAggFilters.get(0).getSelection()[0]);
        assertEquals(1, edgePostAggFilters.get(1).getSelection().length);
        assertEquals(expectedProperties.get(1), edgePostAggFilters.get(1).getSelection()[0]);

        sparkSession.sparkContext().stop();
    }

    @Test
    public void testSpecifyVertexAndPropertyFilter() {
        final Schema schema = getSchema();
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        // Specify vertex and a filter on property1
        Filter[] filters = new Filter[2];
        filters[0] = new GreaterThan("property1", 5);
        filters[1] = new EqualTo(SchemaToStructTypeConverter.VERTEX_COL_NAME, "0");
        FiltersToOperationConverter converter = new FiltersToOperationConverter(getViewFromSchema(schema),
                schema, filters);
        Operation operation = converter.getOperation();

        assertTrue(operation instanceof GetRDDOfElements);
        assertEquals(1, ((GraphFilters) operation).getView().getEntityGroups().size());
        assertEquals(0, ((GraphFilters) operation).getView().getEdgeGroups().size());
        final Set<EntityId> seeds = new HashSet<>();
        for (final Object seed : ((GetRDDOfElements) operation).getInput()) {
            seeds.add((EntitySeed) seed);
        }
        assertEquals(Collections.singleton(new EntitySeed("0")), seeds);
        View opView = ((GraphFilters) operation).getView();
        List<TupleAdaptedPredicate<String, ?>> entityPostAggFilters = opView
                .getEntity(ENTITY_GROUP).getPostAggregationFilterFunctions();
        assertEquals(1, entityPostAggFilters.size());
        final ArrayList<String> expectedProperties = new ArrayList<>();
        expectedProperties.add("property1");
        assertEquals(1, entityPostAggFilters.get(0).getSelection().length);
        assertEquals(expectedProperties.get(0), entityPostAggFilters.get(0).getSelection()[0]);
        final ArrayList<Predicate> expectedFunctions = new ArrayList<>();
        expectedFunctions.add(new IsMoreThan(5, false));
        assertEquals(expectedFunctions.get(0), entityPostAggFilters.get(0).getPredicate());

        // Specify vertex and filters on properties property1 and property4
        filters = new Filter[3];
        filters[0] = new GreaterThan("property1", 5);
        filters[1] = new EqualTo(SchemaToStructTypeConverter.VERTEX_COL_NAME, "0");
        filters[2] = new LessThan("property4", 8);
        converter = new FiltersToOperationConverter(getViewFromSchema(schema), schema, filters);
        operation = converter.getOperation();
        assertTrue(operation instanceof GetRDDOfElements);
        assertEquals(1, ((GraphFilters) operation).getView().getEntityGroups().size());
        assertEquals(0, ((GraphFilters) operation).getView().getEdgeGroups().size());
        seeds.clear();
        for (final Object seed : ((GetRDDOfElements) operation).getInput()) {
            seeds.add((EntitySeed) seed);
        }
        assertEquals(Collections.singleton(new EntitySeed("0")), seeds);
        opView = ((GraphFilters) operation).getView();
        entityPostAggFilters = opView.getEntity(ENTITY_GROUP)
                .getPostAggregationFilterFunctions();
        assertEquals(2, entityPostAggFilters.size());
        expectedProperties.clear();
        expectedProperties.add("property1");
        expectedProperties.add("property4");
        assertEquals(1, entityPostAggFilters.get(0).getSelection().length);
        assertEquals(expectedProperties.get(0), entityPostAggFilters.get(0).getSelection()[0]);
        assertEquals(1, entityPostAggFilters.get(1).getSelection().length);
        assertEquals(expectedProperties.get(1), entityPostAggFilters.get(1).getSelection()[0]);

        expectedFunctions.clear();
        expectedFunctions.add(new IsMoreThan(5, false));
        expectedFunctions.add(new IsLessThan(8, false));
        assertEquals(expectedFunctions.get(0), entityPostAggFilters.get(0).getPredicate());
        assertEquals(expectedFunctions.get(1), entityPostAggFilters.get(1).getPredicate());

        sparkSession.sparkContext().stop();
    }

    @Test
    public void testSpecifySourceOrDestinationAndPropertyFilter() {
        final Schema schema = getSchema();
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        // Specify src and a filter on property1
        Filter[] filters = new Filter[2];
        filters[0] = new GreaterThan("property1", 5);
        filters[1] = new EqualTo(SchemaToStructTypeConverter.SRC_COL_NAME, "0");
        FiltersToOperationConverter converter = new FiltersToOperationConverter(getViewFromSchema(schema),
                schema, filters);
        Operation operation = converter.getOperation();

        assertTrue(operation instanceof GetRDDOfElements);
        assertEquals(0, ((GraphFilters) operation).getView().getEntityGroups().size());
        assertEquals(2, ((GraphFilters) operation).getView().getEdgeGroups().size());
        final Set<EntityId> seeds = new HashSet<>();
        for (final Object seed : ((GetRDDOfElements) operation).getInput()) {
            seeds.add((EntitySeed) seed);
        }
        assertEquals(Collections.singleton(new EntitySeed("0")), seeds);
        View opView = ((GraphFilters) operation).getView();
        for (final String edgeGroup : EDGE_GROUPS) {
            final List<TupleAdaptedPredicate<String, ?>> edgePostAggFilters = opView
                    .getEdge(edgeGroup).getPostAggregationFilterFunctions();
            assertEquals(1, edgePostAggFilters.size());
            assertArrayEquals(new String[]{"property1"}, edgePostAggFilters.get(0).getSelection());
            assertEquals(new IsMoreThan(5, false), edgePostAggFilters.get(0).getPredicate());
        }

        // Specify src and filters on property1 and property4
        filters = new Filter[3];
        filters[0] = new GreaterThan("property1", 5);
        filters[1] = new EqualTo(SchemaToStructTypeConverter.SRC_COL_NAME, "0");
        filters[2] = new LessThan("property4", 8);
        converter = new FiltersToOperationConverter(getViewFromSchema(schema), schema, filters);
        operation = converter.getOperation();

        assertTrue(operation instanceof GetRDDOfElements);
        assertEquals(0, ((GraphFilters) operation).getView().getEntityGroups().size());
        assertEquals(1, ((GraphFilters) operation).getView().getEdgeGroups().size());
        seeds.clear();
        for (final Object seed : ((GetRDDOfElements) operation).getInput()) {
            seeds.add((EntitySeed) seed);
        }
        assertEquals(Collections.singleton(new EntitySeed("0")), seeds);
        opView = ((GraphFilters) operation).getView();
        final List<TupleAdaptedPredicate<String, ?>> entityPostAggFilters = opView
                .getEdge(EDGE_GROUP).getPostAggregationFilterFunctions();
        assertEquals(2, entityPostAggFilters.size());
        final List<String> expectedProperties = new ArrayList<>();
        expectedProperties.add("property1");
        expectedProperties.add("property4");
        assertEquals(1, entityPostAggFilters.get(0).getSelection().length);
        assertEquals(expectedProperties.get(0), entityPostAggFilters.get(0).getSelection()[0]);
        assertEquals(new IsMoreThan(5, false), entityPostAggFilters.get(0).getPredicate());
        assertEquals(1, entityPostAggFilters.get(1).getSelection().length);
        assertEquals(expectedProperties.get(1), entityPostAggFilters.get(1).getSelection()[0]);
        assertEquals(new IsLessThan(8, false), entityPostAggFilters.get(1).getPredicate());

        sparkSession.sparkContext().stop();
    }

    private Schema getSchema() {
        return Schema.fromJson(StreamUtil.schemas(getClass()));
    }

    private View getViewFromSchema(final Schema schema) {
        return new View.Builder()
                .entities(schema.getEntityGroups())
                .edges(schema.getEdgeGroups())
                .build();
    }
}
