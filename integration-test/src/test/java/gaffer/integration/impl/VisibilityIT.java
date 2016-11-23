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

package gaffer.integration.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import gaffer.commonutil.CommonConstants;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestTypes;
import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.exception.SerialisationException;
import gaffer.function.simple.aggregate.StringConcat;
import gaffer.graph.Graph;
import gaffer.integration.AbstractStoreIT;
import gaffer.integration.TraitRequirement;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.serialisation.AbstractSerialisation;
import gaffer.store.StoreTrait;
import gaffer.store.schema.Schema;
import gaffer.store.schema.SchemaEntityDefinition;
import gaffer.store.schema.TypeDefinition;
import gaffer.user.User;
import org.junit.Before;
import org.junit.Test;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class VisibilityIT extends AbstractStoreIT {

    private static final User USER_DEFAULT = new User();
    private static final User USER_VIS_1 = new User.Builder().dataAuth("vis1")
                                                             .build();
    private static final User USER_VIS_2 = new User.Builder().dataAuth("vis2")
                                                             .build();

    protected static Graph graphNoVisibility;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
        graphNoVisibility = new Graph.Builder()
                .storeProperties(getStoreProperties())
                .addSchema(createSchemaNoVisibility())
                .addSchema(getStoreSchema())
                .build();
    }

    @Test
    @TraitRequirement(StoreTrait.VISIBILITY)
    public void shouldAccessMissingVisibilityGroups() throws OperationException, JsonProcessingException {

        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "A");

        // Do NOT add an explicit visibility property
        // entity1.putProperty(AccumuloPropertyNames.VISIBILITY, "");
        elements.add(entity1);

        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();
        graph.execute(addElements, USER_DEFAULT);

        final GetRelatedElements<EntitySeed, Element> get = new GetRelatedElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .build();

        final CloseableIterable<Element> iterable = graph.execute(get, USER_DEFAULT);

        final List<Element> results = Lists.newArrayList(iterable);

        // Check for all entities which should be visible
        assertThat("Results do not contain all expected entities.", results, hasSize(1));

        for (final Element e : results) {

            // Check that all visible entities contain the visibility property
            assertTrue("Visibility property should be visible.", e.getProperties()
                                                                  .containsKey(TestTypes.VISIBILITY));

            assertThat("Visibility property should contain an empty String.", e.getProperties()
                                                                               .get(TestTypes.VISIBILITY)
                                                                               .toString(), isEmptyString());
        }

        iterable.close();
    }

    @Test
    @TraitRequirement(StoreTrait.VISIBILITY)
    public void shouldAccessMissingVisibilityGroupsWithNoVisibilityPropertyInSchema() throws OperationException, JsonProcessingException {

        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "A");
        elements.add(entity1);

        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();
        graphNoVisibility.execute(addElements, USER_DEFAULT);

        final GetRelatedElements<EntitySeed, Element> get = new GetRelatedElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .build();

        final CloseableIterable<Element> iterable = graphNoVisibility.execute(get, USER_DEFAULT);

        final List<Element> results = Lists.newArrayList(iterable);

        // Check for all entities which should be visible
        assertThat("Results do not contain all expected entities.", results, hasSize(1));

        for (final Element e : results) {

            // Check that all visible entities do not contain the visibility property
            assertFalse("Visibility property should not be visible.", e.getProperties()
                                                                       .containsKey(TestTypes.VISIBILITY));
        }

        iterable.close();
    }

    @Test
    @TraitRequirement(StoreTrait.VISIBILITY)
    public void shouldAccessEmptyVisibilityGroups() throws OperationException, JsonProcessingException {

        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "A");
        entity1.putProperty(TestTypes.VISIBILITY, "");
        elements.add(entity1);

        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();
        graph.execute(addElements, USER_DEFAULT);

        final GetRelatedElements<EntitySeed, Element> get = new GetRelatedElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .build();

        final CloseableIterable<Element> iterable = graph.execute(get, USER_DEFAULT);

        final List<Element> results = Lists.newArrayList(iterable);

        // Check for all entities which should be visible
        assertThat("Results do not contain all expected entities.", results, hasSize(1));

        for (final Element e : results) {

            // Check that all visible entities contain the visibility property
            assertTrue("Visibility property should be visible.", e.getProperties()
                                                                  .containsKey(TestTypes.VISIBILITY));

            assertThat("Visibility property should contain an empty String.", e.getProperties()
                                                                               .get(TestTypes.VISIBILITY)
                                                                               .toString(), isEmptyString());
        }

        iterable.close();
    }

    @Test
    @TraitRequirement(StoreTrait.VISIBILITY)
    public void shouldAccessNullVisibilityGroups() throws OperationException, JsonProcessingException {
        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "A");
        entity1.putProperty(TestTypes.VISIBILITY, null);
        elements.add(entity1);

        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();
        graph.execute(addElements, USER_DEFAULT);

        final GetRelatedElements<EntitySeed, Element> get = new GetRelatedElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("B"))
                .build();

        final CloseableIterable<Element> iterable = graph.execute(get, USER_DEFAULT);

        final List<Element> results = Lists.newArrayList(iterable);

        // Check for all entities which should be visible
        assertThat("Results do not contain all expected entities.", results, hasSize(1));

        for (final Element e : results) {

            // Check that all visible entities contain the visibility property
            assertTrue("Visibility property should be visible.", e.getProperties()
                                                                  .containsKey(TestTypes.VISIBILITY));

            assertThat("Visibility property should contain an empty String.", e.getProperties()
                                                                               .get(TestTypes.VISIBILITY)
                                                                               .toString(), isEmptyString());
        }

        iterable.close();
    }

    @Test
    @TraitRequirement(StoreTrait.VISIBILITY)
    public void shouldAccessSingleVisibilityGroup() throws OperationException {
        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "A");
        entity1.putProperty(TestTypes.VISIBILITY, "vis1");
        final Entity entity2 = new Entity(TestGroups.ENTITY, "B");
        entity2.putProperty(TestTypes.VISIBILITY, "vis1");

        elements.add(entity1);
        elements.add(entity2);

        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();
        graph.execute(addElements, USER_VIS_1);

        final GetRelatedElements<EntitySeed, Element> get = new GetRelatedElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("A"))
                .addSeed(new EntitySeed("B"))
                .build();

        final CloseableIterable<Element> userVis1Iterable = graph.execute(get, USER_VIS_1);
        final CloseableIterable<Element> userVis2Iterable = graph.execute(get, USER_VIS_2);

        final List<Element> userVis1Results = Lists.newArrayList(userVis1Iterable);
        final List<Element> userVis2Results = Lists.newArrayList(userVis2Iterable);

        assertThat(userVis1Results, hasSize(2));
        assertThat(userVis2Results, is(empty()));

        for (final Element e : userVis1Results) {
            // Check that all visible entities contain the visibility property
            assertTrue("Missing visibility property.", e.getProperties()
                                                        .containsKey(TestTypes.VISIBILITY));

            // Check that the visibility key contai
            // ns the correct value
            assertEquals("Visibility property should be \"vis1\"",
                    e.getProperties()
                     .get(TestTypes.VISIBILITY)
                     .toString(), "vis1");
        }

        userVis1Iterable.close();
        userVis2Iterable.close();
    }

    @Test
    @TraitRequirement(StoreTrait.VISIBILITY)
    public void shouldAccessMultipleVisibilityGroups_and() throws OperationException {

        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "B");
        entity1.putProperty(TestTypes.VISIBILITY, "vis1&vis2");
        elements.add(entity1);

        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();
        graph.execute(addElements, new User());

        final GetRelatedElements<EntitySeed, Element> get = new GetRelatedElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("B"))
                .build();

        final CloseableIterable<Element> iterable = graph.execute(get, new User(User.UNKNOWN_USER_ID, Sets
                .newHashSet("vis1", "vis2")));

        final List<Element> results = Lists.newArrayList(iterable);

        assertThat("Results do not contain all expected Elements.", results, hasSize(1));

        for (final Element e : iterable) {
            assertTrue(e.getProperties()
                        .containsKey(TestTypes.VISIBILITY));
        }

        iterable.close();
    }

    @Test
    @TraitRequirement(StoreTrait.VISIBILITY)
    public void shouldAccessMultipleVisibilityGroups_or() throws OperationException {

        final Set<Element> elements = new HashSet<>();
        final Entity entity1 = new Entity(TestGroups.ENTITY, "B");
        entity1.putProperty(TestTypes.VISIBILITY, "vis1|vis2");
        elements.add(entity1);

        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();
        graph.execute(addElements, new User());

        final GetRelatedElements<EntitySeed, Element> get = new GetRelatedElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed("B"))
                .build();
        final CloseableIterable<Element> iterable = graph.execute(get, new User(User.UNKNOWN_USER_ID, Sets
                .newHashSet("vis1")));

        final List<Element> results = Lists.newArrayList(iterable);

        assertThat("Results do not contain all expected Elements.", results, hasSize(1));

        for (final Element e : results) {
            assertTrue(e.getProperties()
                        .containsKey(TestTypes.VISIBILITY));
        }

        iterable.close();
    }

    @Override
    protected Schema createSchema() {
        return new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type(TestTypes.VISIBILITY, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(new StringConcat())
                        .serialiser(new VisibilityITSerialiser())
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestTypes.VISIBILITY, TestTypes.VISIBILITY)
                        .build())
                .visibilityProperty(TestTypes.VISIBILITY)
                .build();
    }

    private Schema createSchemaNoVisibility() {
        return new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .build())
                .build();
    }

    public static final class VisibilityITSerialiser extends AbstractSerialisation<String> {

        @Override
        public boolean canHandle(final Class clazz) {
            return String.class.equals(clazz);
        }

        @Override
        public byte[] serialise(final String value) throws SerialisationException {
            try {
                return value.getBytes(CommonConstants.UTF_8);
            } catch (UnsupportedEncodingException e) {
                throw new SerialisationException(e.getMessage(), e);
            }
        }

        @Override
        public String deserialise(final byte[] bytes) throws SerialisationException {
            try {
                return new String(bytes, CommonConstants.UTF_8);
            } catch (UnsupportedEncodingException e) {
                throw new SerialisationException(e.getMessage(), e);
            }
        }

        @Override
        public byte[] serialiseNull() {
            return new byte[]{};
        }

        @Override
        public String deserialiseEmptyBytes() {
            return "";
        }

        @Override
        public boolean isByteOrderPreserved() {
            return true;
        }
    }

}
