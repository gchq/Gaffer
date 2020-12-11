/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.integration.provider.GraphBuilderProvider;
import uk.gov.gchq.gaffer.integration.provider.StorePropertiesProvider;
import uk.gov.gchq.gaffer.integration.util.TestUtil;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;

public class VisibilityIT extends AbstractStoreIT {

    private static final User USER_VIS_1 = new User.Builder().dataAuth("vis1")
            .build();
    private static final User USER_VIS_2 = new User.Builder().dataAuth("vis2")
            .build();

    @ParameterizedTest
    @ArgumentsSource(StorePropertiesProvider.class)
    @TraitRequirement(StoreTrait.VISIBILITY)
    public void shouldAccessMissingVisibilityGroups(final StoreProperties storeProperties) throws OperationException {
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig("test"))
                .storeProperties(storeProperties)
                .addSchema(createVisibilitySchema())
                .build();
        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "A");

        // Do NOT add an explicit visibility property
        // entity1.putProperty(AccumuloPropertyNames.VISIBILITY, "");
        elements.add(entity1);

        final AddElements addElements = new AddElements.Builder()
                .input(elements)
                .build();
        graph.execute(addElements, getUser());

        final GetElements get = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .build();

        final CloseableIterable<? extends Element> iterable = graph.execute(get, getUser());

        final List<Element> results = Lists.newArrayList(iterable);

        // Check for all entities which should be visible
        assertEquals(1, results.size(), "Results do not contain all expected entities.");

        for (final Element e : results) {

            // Check that all visible entities contain the visibility property
            assertTrue(e.getProperties().containsKey(TestTypes.VISIBILITY), "Visibility property should be visible.");

            assertEquals("", e.getProperties().get(TestTypes.VISIBILITY).toString(), "Visibility property should contain an empty String.");
        }
        iterable.close();
    }

    @ParameterizedTest
    @ArgumentsSource(StorePropertiesProvider.class)
    @TraitRequirement(StoreTrait.VISIBILITY)
    public void shouldAccessMissingVisibilityGroupsWithNoVisibilityPropertyInSchema(final StoreProperties storeProperties) throws OperationException {
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig("test"))
                .storeProperties(storeProperties)
                .addSchema(createSchemaNoVisibility())
                .build();

        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "A");
        elements.add(entity1);

        final AddElements addElements = new AddElements.Builder()
                .input(elements)
                .build();
        graph.execute(addElements, getUser());

        final GetElements get = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .build();

        final CloseableIterable<? extends Element> iterable = graph.execute(get, getUser());

        final List<Element> results = Lists.newArrayList(iterable);

        // Check for all entities which should be visible
        assertEquals(1, results.size(), "Results do not contain all expected entities.");

        for (final Element e : results) {

            // Check that all visible entities do not contain the visibility property
            assertFalse(e.getProperties().containsKey(TestTypes.VISIBILITY), "Visibility property should not be visible.");
        }

        iterable.close();
    }

    @ParameterizedTest
    @ArgumentsSource(StorePropertiesProvider.class)
    @TraitRequirement(StoreTrait.VISIBILITY)
    public void shouldAccessEmptyVisibilityGroups(final StoreProperties storeProperties) throws OperationException {
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig("test"))
                .storeProperties(storeProperties)
                .addSchema(createVisibilitySchema())
                .build();

        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "A");
        entity1.putProperty(TestTypes.VISIBILITY, "");
        elements.add(entity1);

        final AddElements addElements = new AddElements.Builder()
                .input(elements)
                .build();
        graph.execute(addElements, getUser());

        final GetElements get = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .build();

        final CloseableIterable<? extends Element> iterable = graph.execute(get, getUser());

        final List<Element> results = Lists.newArrayList(iterable);

        // Check for all entities which should be visible
        assertEquals(1, results.size(), "Results do not contain all expected entities.");

        for (final Element e : results) {

            // Check that all visible entities contain the visibility property
            assertTrue(e.getProperties().containsKey(TestTypes.VISIBILITY), "Visibility property should be visible.");

            assertEquals("", e.getProperties().get(TestTypes.VISIBILITY).toString(),
                    "Visibility property should contain an empty String.");
        }

        iterable.close();
    }

    @ParameterizedTest
    @ArgumentsSource(StorePropertiesProvider.class)
    @TraitRequirement(StoreTrait.VISIBILITY)
    public void shouldAccessNullVisibilityGroups(final StoreProperties storeProperties) throws OperationException {
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig("test"))
                .storeProperties(storeProperties)
                .addSchema(createVisibilitySchema())
                .build();

        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "A");
        entity1.putProperty(TestTypes.VISIBILITY, null);
        elements.add(entity1);

        final AddElements addElements = new AddElements.Builder()
                .input(elements)
                .build();
        graph.execute(addElements, getUser());

        final GetElements get = new GetElements.Builder()
                .input(new EntitySeed("A"), new EntitySeed("B"))
                .build();

        final CloseableIterable<? extends Element> iterable = graph.execute(get, getUser());

        final List<Element> results = Lists.newArrayList(iterable);

        // Check for all entities which should be visible
        assertEquals(1, results.size(), "Results do not contain all expected entities.");

        for (final Element e : results) {

            // Check that all visible entities contain the visibility property
            assertTrue(e.getProperties().containsKey(TestTypes.VISIBILITY), "Visibility property should be visible.");

            assertEquals("", e.getProperties().get(TestTypes.VISIBILITY).toString(),
                    "Visibility property should contain an empty String.");
        }

        iterable.close();
    }

    @ParameterizedTest
    @ArgumentsSource(StorePropertiesProvider.class)
    @TraitRequirement(StoreTrait.VISIBILITY)
    public void shouldAccessSingleVisibilityGroup(final StoreProperties storeProperties) throws OperationException {
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig("test"))
                .storeProperties(storeProperties)
                .addSchema(createVisibilitySchema())
                .build();

        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "A");
        entity1.putProperty(TestTypes.VISIBILITY, "vis1");
        final Entity entity2 = new Entity(TestGroups.ENTITY, "B");
        entity2.putProperty(TestTypes.VISIBILITY, "vis1");

        elements.add(entity1);
        elements.add(entity2);

        final AddElements addElements = new AddElements.Builder()
                .input(elements)
                .build();
        graph.execute(addElements, USER_VIS_1);

        final GetElements get = new GetElements.Builder()
                .input(new EntitySeed("A"), new EntitySeed("B"))
                .build();

        final CloseableIterable<? extends Element> userVis1Iterable = graph.execute(get, USER_VIS_1);
        final CloseableIterable<? extends Element> userVis2Iterable = graph.execute(get, USER_VIS_2);

        final List<Element> userVis1Results = Lists.newArrayList(userVis1Iterable);
        final List<Element> userVis2Results = Lists.newArrayList(userVis2Iterable);

        assertEquals(2, userVis1Results.size());
        assertEquals(0, userVis2Results.size());

        for (final Element e : userVis1Results) {
            // Check that all visible entities contain the visibility property
            assertTrue(e.getProperties().containsKey(TestTypes.VISIBILITY), "Missing visibility property.");

            // Check that the visibility key contains the correct value
            assertEquals("vis1",
                    e.getProperties()
                            .get(TestTypes.VISIBILITY)
                            .toString(), "Visibility property should be \"vis1\"");
        }

        userVis1Iterable.close();
        userVis2Iterable.close();
    }

    @ParameterizedTest
    @ArgumentsSource(StorePropertiesProvider.class)
    @TraitRequirement(StoreTrait.VISIBILITY)
    public void shouldAccessMultipleVisibilityGroups_and(final StoreProperties storeProperties) throws OperationException {
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig("test"))
                .storeProperties(storeProperties)
                .addSchema(createVisibilitySchema())
                .build();

        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "B");
        entity1.putProperty(TestTypes.VISIBILITY, "vis1&vis2");
        elements.add(entity1);

        final AddElements addElements = new AddElements.Builder()
                .input(elements)
                .build();
        graph.execute(addElements, new User());

        final GetElements get = new GetElements.Builder()
                .input(new EntitySeed("B"))
                .build();

        final CloseableIterable<? extends Element> iterable = graph.execute(get, new User(User.UNKNOWN_USER_ID, Sets
                .newHashSet("vis1", "vis2")));

        final List<Element> results = Lists.newArrayList(iterable);

        assertEquals(1, results.size(), "Results do not contain all expected Elements.");

        for (final Element e : iterable) {
            assertTrue(e.getProperties()
                    .containsKey(TestTypes.VISIBILITY));
        }

        iterable.close();
    }

    @ParameterizedTest
    @ArgumentsSource(StorePropertiesProvider.class)
    @TraitRequirement(StoreTrait.VISIBILITY)
    public void shouldAccessMultipleVisibilityGroups_or(final StoreProperties storeProperties) throws OperationException {
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig("test"))
                .storeProperties(storeProperties)
                .addSchema(createVisibilitySchema())
                .build();

        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "B");
        entity1.putProperty(TestTypes.VISIBILITY, "vis1|vis2");
        elements.add(entity1);

        final AddElements addElements = new AddElements.Builder()
                .input(elements)
                .build();
        graph.execute(addElements, new User());

        final GetElements get = new GetElements.Builder()
                .input(new EntitySeed("B"))
                .build();
        final CloseableIterable<? extends Element> iterable = graph.execute(get, new User(User.UNKNOWN_USER_ID, Sets
                .newHashSet("vis1")));

        final List<Element> results = Lists.newArrayList(iterable);

        assertEquals(1, results.size(), "Results do not contain all expected Elements.");

        for (final Element e : results) {
            assertTrue(e.getProperties()
                    .containsKey(TestTypes.VISIBILITY));
        }

        iterable.close();
    }


    private Schema createVisibilitySchema() {
        return new Schema.Builder()
                .merge(TestUtil.createDefaultSchema())
                .type(TestTypes.VISIBILITY, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(new StringConcat())
                        .build())
                .type(TestGroups.ENTITY, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.SET, TestTypes.PROP_SET_STRING)
                        .property(TestTypes.VISIBILITY, TestTypes.VISIBILITY)
                        .groupBy(TestPropertyNames.INT)
                        .build())
                .visibilityProperty(TestTypes.VISIBILITY)
                .build();
    }

    private Schema createSchemaNoVisibility() {
        return new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type(DIRECTED_EITHER, Boolean.class)
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(DIRECTED_EITHER)
                        .build())
                .build();
    }

}
