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
package uk.gov.gchq.gaffer.rest.service.v2;

import org.hamcrest.core.IsCollectionContaining;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.impl.predicate.IsA;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.Not;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static uk.gov.gchq.gaffer.store.StoreTrait.INGEST_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_TRANSFORMATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.STORE_VALIDATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.TRANSFORMATION;

@RunWith(MockitoJUnitRunner.class)
public class GraphConfigurationServiceV2Test {

    private static final String GRAPH_ID = "graphId";

    @InjectMocks
    private GraphConfigurationServiceV2 service;

    @Mock
    private GraphFactory graphFactory;

    @Mock
    private UserFactory userFactory;

    @Mock
    private Store store;

    @Before
    public void setup() {
        final Set<StoreTrait> traits = new HashSet<>(Arrays.asList(INGEST_AGGREGATION, PRE_AGGREGATION_FILTERING, POST_TRANSFORMATION_FILTERING, POST_AGGREGATION_FILTERING, TRANSFORMATION, STORE_VALIDATION));
        given(store.getSchema()).willReturn(new Schema());
        given(store.getProperties()).willReturn(new StoreProperties());
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .description("test graph")
                .store(store)
                .build();
        final StoreProperties props = new StoreProperties();
        given(store.getProperties()).willReturn(props);

        final Set<Class<? extends Operation>> operations = new HashSet<>();
        operations.add(AddElements.class);
        given(graphFactory.getGraph()).willReturn(graph);
        given(graph.getSupportedOperations()).willReturn(operations);
        given(graph.isSupported(AddElements.class)).willReturn(true);

        given(userFactory.createContext()).willReturn(new Context());

        given(graph.getStoreTraits()).willReturn(traits);
    }

    @Test
    public void shouldReturnDescription() {
        // When
        final int status = service.getDescription().getStatus();
        final String description = service.getDescription().getEntity().toString();

        // Then
        assertEquals(200, status);
        assertEquals("test graph", description);
    }

    @Test
    public void shouldGetFilterFunctions() {
        // When
        final Set<Class> classes = (Set<Class>) service.getFilterFunction(null).getEntity();

        // Then
        assertThat(classes, IsCollectionContaining.hasItem(IsA.class));
    }

    @Test
    public void shouldGetFilterFunctionsWithInputClass() {
        // When
        final Set<Class> classes = (Set<Class>) service.getFilterFunction(Long.class.getName()).getEntity();

        // Then
        assertThat(classes, IsCollectionContaining.hasItem(IsLessThan.class));
        assertThat(classes, IsCollectionContaining.hasItem(IsMoreThan.class));
        assertThat(classes, IsCollectionContaining.hasItem(Not.class));
    }

    @Test
    public void shouldThrowExceptionWhenGetFilterFunctionsWithUnknownClassName() {
        // When / Then
        try {
            service.getFilterFunction("unknown className");
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Input class was not recognised:"));
        }
    }

    @Test
    public void shouldGetSerialisedFields() {
        // When
        final Set<String> fields = (Set<String>) service.getSerialisedFields(IsA.class.getName()).getEntity();

        // Then
        assertEquals(1, fields.size());
        assertTrue(fields.contains("type"));
    }

    @Test
    public void shouldThrowExceptionWhenGetSerialisedFieldsWithUnknownClassName() {
        // When / Then
        try {
            service.getSerialisedFields("unknown className");
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
            assertTrue(e.getMessage().contains("Class name was not recognised:"));
        }
    }

    @Test
    public void shouldGetStoreTraits() {
        // When
        final Set<StoreTrait> traits = (Set<StoreTrait>) service.getStoreTraits().getEntity();

        // Then
        assertNotNull(traits);
        assertEquals("Collection size should be 6", 6, traits.size());
        assertTrue("Collection should contain INGEST_AGGREGATION trait", traits.contains(INGEST_AGGREGATION));
        assertTrue("Collection should contain PRE_AGGREGATION_FILTERING trait", traits.contains(PRE_AGGREGATION_FILTERING));
        assertTrue("Collection should contain POST_AGGREGATION_FILTERING trait", traits.contains(POST_AGGREGATION_FILTERING));
        assertTrue("Collection should contain POST_TRANSFORMATION_FILTERING trait", traits.contains(POST_TRANSFORMATION_FILTERING));
        assertTrue("Collection should contain TRANSFORMATION trait", traits.contains(TRANSFORMATION));
        assertTrue("Collection should contain STORE_VALIDATION trait", traits.contains(STORE_VALIDATION));
    }

    @Test
    public void shouldGetTransformFunctions() {
        // When
        final Set<Class> classes = (Set<Class>) service.getTransformFunctions().getEntity();

        // Then
        assertFalse(classes.isEmpty());
    }

    @Test
    public void shouldGetElementGenerators() {
        // When
        final Set<Class> classes = (Set<Class>) service.getElementGenerators().getEntity();

        // Then
        assertFalse(classes.isEmpty());
    }

    @Test
    public void shouldGetObjectGenerators() {
        // When
        final Set<Class> classes = (Set<Class>) service.getObjectGenerators().getEntity();

        // Then
        assertFalse(classes.isEmpty());
    }

    @Test
    public void shouldSerialiseAndDeserialiseGetStoreTraits() throws SerialisationException {
        // When
        byte[] bytes = JSONSerialiser.serialise(service.getStoreTraits().getEntity());
        final Set<StoreTrait> traits = JSONSerialiser.deserialise(bytes, Set.class);

        // Then
        assertNotNull(traits);
        assertEquals("Collection size should be 6", 6, traits.size());
        assertTrue("Collection should contain INGEST_AGGREGATION trait", traits.contains(INGEST_AGGREGATION.name()));
        assertTrue("Collection should contain PRE_AGGREGATION_FILTERING trait", traits.contains(PRE_AGGREGATION_FILTERING.name()));
        assertTrue("Collection should contain POST_AGGREGATION_FILTERING trait", traits.contains(POST_AGGREGATION_FILTERING.name()));
        assertTrue("Collection should contain POST_TRANSFORMATION_FILTERING trait", traits.contains(POST_TRANSFORMATION_FILTERING.name()));
        assertTrue("Collection should contain TRANSFORMATION trait", traits.contains(TRANSFORMATION.name()));
        assertTrue("Collection should contain STORE_VALIDATION trait", traits.contains(STORE_VALIDATION.name()));
    }
}
