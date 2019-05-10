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
package uk.gov.gchq.gaffer.accumulostore.integration;

import com.google.common.collect.Lists;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloPropertyNames;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.Graph.Builder;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.integration.StandaloneIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import java.io.UnsupportedEncodingException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class AccumuloAggregationIT extends StandaloneIT {
    private static final String VERTEX = "vertex";
    private static final String PUBLIC_VISIBILITY = "publicVisibility";
    private static final String PRIVATE_VISIBILITY = "privateVisibility";

    private final User user = getUser();

    @Test
    public void shouldOnlyAggregateVisibilityWhenGroupByIsNull() throws Exception {
        final Graph graph = createGraph();
        final Entity entity1 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "value 3a")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "value 4")
                .property(AccumuloPropertyNames.VISIBILITY, PUBLIC_VISIBILITY)
                .build();
        final Entity entity2 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "value 3a")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "value 4")
                .property(AccumuloPropertyNames.VISIBILITY, PRIVATE_VISIBILITY)
                .build();
        final Entity entity3 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "value 3b")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "value 4")
                .property(AccumuloPropertyNames.VISIBILITY, PRIVATE_VISIBILITY)
                .build();

        graph.execute(new AddElements.Builder()
                        .input(entity1, entity2, entity3)
                        .build(),
                user);

        // Given
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed(VERTEX))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements, user));

        // Then
        assertNotNull(results);
        assertEquals(2, results.size());

        final Entity expectedSummarisedEntity = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, "")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "value 3a")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "value 4")
                .property(AccumuloPropertyNames.VISIBILITY, PRIVATE_VISIBILITY + "," + PUBLIC_VISIBILITY)
                .build();

        final Entity expectedEntity = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, "")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "value 3b")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "value 4")
                .property(AccumuloPropertyNames.VISIBILITY, PRIVATE_VISIBILITY)
                .build();

        assertThat(results, IsCollectionContaining.hasItems(
                expectedSummarisedEntity, expectedEntity
        ));
    }

    @Test
    public void shouldAggregateOverAllPropertiesExceptForGroupByProperties() throws Exception {
        final Graph graph = createGraph();
        final Entity entity1 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "some value")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, "some value 2")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "some value 3")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "some value 4")
                .property(AccumuloPropertyNames.VISIBILITY, PUBLIC_VISIBILITY)
                .build();
        final Entity entity2 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "some value")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, "some value 2b")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "some value 3b")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "some value 4b")
                .property(AccumuloPropertyNames.VISIBILITY, PRIVATE_VISIBILITY)
                .build();
        final Entity entity3 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "some value c")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, "some value 2c")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "some value 3c")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "some value 4c")
                .property(AccumuloPropertyNames.VISIBILITY, PRIVATE_VISIBILITY)
                .build();

        graph.execute(new AddElements.Builder().input(entity1, entity2, entity3).build(), user);

        // Given
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed(VERTEX))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                .groupBy(AccumuloPropertyNames.COLUMN_QUALIFIER)
                                .build())
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements, user));

        // Then
        assertNotNull(results);
        assertEquals(2, results.size());

        final Entity expectedEntity = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "some value")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, "some value 2,some value 2b")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "some value 3,some value 3b")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "some value 4,some value 4b")
                .property(AccumuloPropertyNames.VISIBILITY, PUBLIC_VISIBILITY + "," + PRIVATE_VISIBILITY)
                .build();

        assertThat(results, IsCollectionContaining.hasItems(
                expectedEntity,
                entity3
        ));
    }

    @Test
    public void shouldHandleAggregationWhenGroupByPropertiesAreNull() throws OperationException, UnsupportedEncodingException {
        final Graph graph = createGraphNoVisibility();
        final Entity entity1 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, null)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, null)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, null)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, null)
                .build();
        final Entity entity2 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "test 3")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "test 4")
                .build();

        graph.execute(new AddElements.Builder().input(entity1, entity2).build(), user);

        // Given
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed(VERTEX))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                .groupBy()
                                .build())
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements, user));

        // Then
        assertNotNull(results);
        assertEquals(1, results.size());

        final Entity expectedEntity = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, ",") //String Aggregation is combining two empty strings -> "",""
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, ",") //String Aggregation is combining two empty strings -> "",""
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, ",test 3") //String Aggregation is combining one empty strings -> "","test 3"
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, ",test 4") //String Aggregation is combining one empty strings -> "","test 4"
                .build();
        assertEquals(expectedEntity, results.get(0));
    }

    @Test
    public void shouldHandleAggregationWhenAllColumnQualifierPropertiesAreGroupByProperties() throws OperationException, UnsupportedEncodingException {
        final Graph graph = createGraphNoVisibility();
        final Entity entity1 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "test 3")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, "test 4")
                .build();
        final Entity entity2 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "test 3")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, "test 4")
                .build();

        graph.execute(new AddElements.Builder().input(entity1, entity2).build(), user);

        // Given
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed(VERTEX))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                .groupBy(AccumuloPropertyNames.COLUMN_QUALIFIER, AccumuloPropertyNames.COLUMN_QUALIFIER_2)
                                .build())
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements, user));

        // Then
        assertNotNull(results);
        assertEquals(1, results.size());

        final Entity expectedEntity = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "test 3")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, "test 4")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "")
                .build();
        assertEquals(expectedEntity, results.get(0));
    }

    @Test
    public void shouldHandleAggregationWhenGroupByPropertiesAreNotSet() throws OperationException, UnsupportedEncodingException {
        final Graph graph = createGraphNoVisibility();
        final Entity entity1 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "test 3")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "test 4")
                .build();
        final Entity entity2 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "test 3")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "test 4")
                .build();

        graph.execute(new AddElements.Builder().input(entity1, entity2).build(), user);

        // Given
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed(VERTEX))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                .groupBy(AccumuloPropertyNames.COLUMN_QUALIFIER, AccumuloPropertyNames.COLUMN_QUALIFIER_2)
                                .build())
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements, user));

        // Then
        assertNotNull(results);
        assertEquals(1, results.size());

        final Entity expectedEntity = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, "")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "test 3")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "test 4")
                .build();
        assertEquals(expectedEntity, results.get(0));
    }

    @Test
    public void shouldHandleAggregationWithMultipleCombinations() throws OperationException, UnsupportedEncodingException {
        final Graph graph = createGraphNoVisibility();
        final Entity entity1 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "test 3")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "test 4")
                .build();
        final Entity entity2 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, null)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "test 3")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "test 4")
                .build();
        final Entity entity3 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "test1a")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "test 3")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "test 4")
                .build();
        final Entity entity4 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "test1b")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "test 3")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "test 4")
                .build();
        final Entity entity5 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "test1a")
                .build();
        final Entity entity6 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "test1b")
                .build();
        final Entity entity7 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, "test2a")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "test 3")
                .build();

        graph.execute(new AddElements.Builder().input(
                entity1,
                entity2,
                entity3,
                entity4,
                entity5,
                entity6,
                entity7
        ).build(), user);

        // Duplicate the entities to check they are aggregated properly
        graph.execute(new AddElements.Builder().input(
                entity1,
                entity2,
                entity3,
                entity4,
                entity5,
                entity6,
                entity7
        ).build(), user);

        // Given
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed(VERTEX))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                .groupBy(AccumuloPropertyNames.COLUMN_QUALIFIER, AccumuloPropertyNames.COLUMN_QUALIFIER_2)
                                .build())
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements, user));

        // Then
        assertNotNull(results);
        assertEquals(4, results.size());

        final Entity expectedEntity1 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, "")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "test 3")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "test 4")
                .build();

        final Entity expectedEntity2 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "test1a")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, "")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, ",test 3")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, ",test 4")
                .build();

        final Entity expectedEntity3 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "test1b")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, "")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, ",test 3")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, ",test 4")
                .build();

        final Entity expectedEntity4 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, "test2a")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "test 3")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "")
                .build();

        assertThat(results, IsCollectionContaining.hasItems(
                expectedEntity1,
                expectedEntity2,
                expectedEntity3,
                expectedEntity4
        ));
    }

    @Test
    public void shouldHandleAggregationWhenNoAggregatorsAreProvided() throws OperationException {

        final Graph graph = createGraphNoAggregators();
        final Entity entity1 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_3, "test 3")
                .property(TestPropertyNames.PROP_4, "test 4")
                .build();
        final Entity entity2 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1, null)
                .property(TestPropertyNames.PROP_3, "test 3")
                .property(TestPropertyNames.PROP_4, "test 4")
                .build();
        final Entity entity3 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1, "test1a")
                .property(TestPropertyNames.PROP_3, "test 3")
                .property(TestPropertyNames.PROP_4, "test 4")
                .build();
        final Entity entity4 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1, "test1b")
                .property(TestPropertyNames.PROP_3, "test 3")
                .property(TestPropertyNames.PROP_4, "test 4")
                .build();
        final Entity entity5 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1, "test1a")
                .build();
        final Entity entity6 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1, "test1b")
                .build();
        final Entity entity7 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_2, "test2a")
                .property(TestPropertyNames.PROP_3, "test 3")
                .build();

        graph.execute(new AddElements.Builder().input(
                entity1,
                entity2,
                entity3,
                entity4,
                entity5,
                entity6,
                entity7
        ).build(), user);

        // Duplicate the entities to check they are not aggregated
        graph.execute(new AddElements.Builder()
                        .input(entity1,
                                entity2,
                                entity3,
                                entity4,
                                entity5,
                                entity6,
                                entity7)
                        .build(),
                user);

        // Given
        final GetAllElements getAllEntities = new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getAllEntities, user));

        // Then
        assertNotNull(results);
        assertEquals(14, results.size());

        final Entity expectedEntity1 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1, "")
                .property(TestPropertyNames.PROP_2, "")
                .property(TestPropertyNames.PROP_3, "test 3")
                .property(TestPropertyNames.PROP_4, "test 4")
                .build();

        final Entity expectedEntity2 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1, "test1a")
                .property(TestPropertyNames.PROP_2, "")
                .property(TestPropertyNames.PROP_3, "test 3")
                .property(TestPropertyNames.PROP_4, "test 4")
                .build();

        final Entity expectedEntity3 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1, "test1b")
                .property(TestPropertyNames.PROP_2, "")
                .property(TestPropertyNames.PROP_3, "test 3")
                .property(TestPropertyNames.PROP_4, "test 4")
                .build();

        final Entity expectedEntity4 = new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1, "")
                .property(TestPropertyNames.PROP_2, "test2a")
                .property(TestPropertyNames.PROP_3, "test 3")
                .property(TestPropertyNames.PROP_4, "")
                .build();

        assertThat(results, IsCollectionContaining.hasItems(
                expectedEntity1,
                expectedEntity2,
                expectedEntity3,
                expectedEntity4
        ));
    }

    protected Graph createGraphNoVisibility() {
        return new Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphWithNoVisibility")
                        .build())
                .storeProperties(createStoreProperties())
                .addSchema(new Schema.Builder()
                        .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                                .clazz(String.class)
                                .build())
                        .type("colQual", new TypeDefinition.Builder()
                                .clazz(String.class)
                                .aggregateFunction(new StringConcat())
                                .serialiser(new StringSerialiser())
                                .build())
                        .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                                .vertex(TestTypes.ID_STRING)
                                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "colQual")
                                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, "colQual")
                                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "colQual")
                                .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "colQual")
                                .groupBy(AccumuloPropertyNames.COLUMN_QUALIFIER,
                                        AccumuloPropertyNames.COLUMN_QUALIFIER_2,
                                        AccumuloPropertyNames.COLUMN_QUALIFIER_3,
                                        AccumuloPropertyNames.COLUMN_QUALIFIER_4)
                                .build())
                        .build())
                .build();
    }

    protected Graph createGraphNoAggregators() {
        return new Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphWithNoAggregators")
                        .build())
                .storeProperties(createStoreProperties())
                .addSchema(new Schema.Builder()
                        .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                                .clazz(String.class)
                                .build())
                        .type("prop", new TypeDefinition.Builder()
                                .clazz(String.class)
                                .serialiser(new StringSerialiser())
                                .build())
                        .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                                .vertex(TestTypes.ID_STRING)
                                .property(TestPropertyNames.PROP_1, "prop")
                                .property(TestPropertyNames.PROP_2, "prop")
                                .property(TestPropertyNames.PROP_3, "prop")
                                .property(TestPropertyNames.PROP_4, "prop")
                                .aggregate(false)
                                .build())
                        .build())
                .build();
    }

    @Override
    protected Schema createSchema() {
        return new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type("colQual", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(new StringConcat())
                        .serialiser(new StringSerialiser())
                        .build())
                .type("visibility", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(new StringConcat())
                        .serialiser(new StringSerialiser())
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(AccumuloPropertyNames.COLUMN_QUALIFIER, "colQual")
                        .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, "colQual")
                        .property(AccumuloPropertyNames.COLUMN_QUALIFIER_3, "colQual")
                        .property(AccumuloPropertyNames.COLUMN_QUALIFIER_4, "colQual")
                        .property(AccumuloPropertyNames.VISIBILITY, "visibility")
                        .groupBy(AccumuloPropertyNames.COLUMN_QUALIFIER,
                                AccumuloPropertyNames.COLUMN_QUALIFIER_2,
                                AccumuloPropertyNames.COLUMN_QUALIFIER_3,
                                AccumuloPropertyNames.COLUMN_QUALIFIER_4)
                        .build())
                .visibilityProperty(AccumuloPropertyNames.VISIBILITY)
                .build();
    }

    @Override
    protected User getUser() {
        return new User.Builder()
                .dataAuth(PUBLIC_VISIBILITY)
                .dataAuth(PRIVATE_VISIBILITY)
                .build();
    }

    @Override
    public StoreProperties createStoreProperties() {
        return AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloStoreITs.class));
    }
}
