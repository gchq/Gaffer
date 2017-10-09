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

package uk.gov.gchq.gaffer.rest.service.v1;

import org.hamcrest.core.IsCollectionContaining;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.impl.predicate.IsA;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.Not;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static uk.gov.gchq.gaffer.store.StoreTrait.INGEST_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_TRANSFORMATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.STORE_VALIDATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.TRANSFORMATION;

@RunWith(MockitoJUnitRunner.class)
public class GraphConfigurationServiceTest {

    private static final String GRAPH_ID = "graphId";

    @InjectMocks
    private GraphConfigurationService service;

    @Mock
    private GraphFactory graphFactory;

    @Mock
    private UserFactory userFactory;

    @Mock
    private Store store;

    @Before
    public void setup() {
        final Schema schema = mock(Schema.class);
        final Set<StoreTrait> traits = new HashSet<>(Arrays.asList(INGEST_AGGREGATION, PRE_AGGREGATION_FILTERING, POST_TRANSFORMATION_FILTERING, POST_AGGREGATION_FILTERING, TRANSFORMATION, STORE_VALIDATION));
        given(store.getSchema()).willReturn(schema);
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .store(store)
                .build();
        final Set<Class<? extends Operation>> operations = new HashSet<>();
        operations.add(AddElements.class);
        given(graphFactory.getGraph()).willReturn(graph);
        given(graph.getSupportedOperations()).willReturn(operations);
        given(graph.isSupported(AddElements.class)).willReturn(true);

        given(userFactory.createContext()).willReturn(new Context());

        given(graph.getStoreTraits()).willReturn(traits);
    }

    @Test
    public void shouldGetFilterFunctions() throws IOException {
        // When
        final Set<Class> classes = service.getFilterFunctions(null);

        // Then
        assertThat(classes, IsCollectionContaining.hasItem(IsA.class));
    }

    @Test
    public void shouldGetFilterFunctionsWithInputClass() throws IOException {
        // When
        final Set<Class> classes = service.getFilterFunctions(Long.class.getName());

        // Then
        assertThat(classes, IsCollectionContaining.hasItem(IsLessThan.class));
        assertThat(classes, IsCollectionContaining.hasItem(IsMoreThan.class));
        assertThat(classes, IsCollectionContaining.hasItem(Not.class));
    }

    @Test
    public void shouldThrowExceptionWhenGetFilterFunctionsWithUnknownClassName() throws IOException {
        // When / Then
        try {
            service.getFilterFunctions("an unknown class name");
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldGetSerialisedFields() throws IOException {
        // When
        final Set<String> fields = service.getSerialisedFields(IsA.class.getName());

        // Then
        assertEquals(1, fields.size());
        assertTrue(fields.contains("type"));
    }

    @Test
    public void shouldThrowExceptionWhenGetSerialisedFieldsWithUnknownClassName() throws IOException {
        // When / Then
        try {
            service.getSerialisedFields("an unknown class name");
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldGetNextOperations() throws IOException {
        // Given
        final Set<Class<? extends Operation>> expectedNextOperations = mock(Set.class);
        given(store.getNextOperations(GetElements.class)).willReturn(expectedNextOperations);

        // When
        final Set<Class> nextOperations = service.getNextOperations(GetElements.class.getName());


        // Then
        assertSame(expectedNextOperations, nextOperations);
    }

    @Test
    public void shouldThrowExceptionWhenGetNextOperationsWithUnknownClassName() throws IOException {
        // When / Then
        try {
            service.getNextOperations("an unknown class name");
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Operation class was not found"));
        }
    }

    @Test
    public void shouldThrowExceptionWhenGetNextOperationsWithNonOperationClassName() throws IOException {
        // When / Then
        try {
            service.getNextOperations(String.class.getName());
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("does not extend Operation"));
        }
    }

    @Test
    public void shouldGetStoreTraits() throws IOException {
        // When
        final Set<StoreTrait> traits = service.getStoreTraits();
        // Then
        assertNotNull(traits);
        assertTrue("Collection size should be 6", traits.size() == 6);
        assertTrue("Collection should contain INGEST_AGGREGATION trait", traits.contains(INGEST_AGGREGATION));
        assertTrue("Collection should contain PRE_AGGREGATION_FILTERING trait", traits.contains(PRE_AGGREGATION_FILTERING));
        assertTrue("Collection should contain POST_AGGREGATION_FILTERING trait", traits.contains(POST_AGGREGATION_FILTERING));
        assertTrue("Collection should contain POST_TRANSFORMATION_FILTERING trait", traits.contains(POST_TRANSFORMATION_FILTERING));
        assertTrue("Collection should contain TRANSFORMATION trait", traits.contains(TRANSFORMATION));
        assertTrue("Collection should contain STORE_VALIDATION trait", traits.contains(STORE_VALIDATION));
    }

    @Test
    public void shouldGetTransformFunctions() throws IOException {
        // When
        final Set<Class> classes = service.getTransformFunctions();

        // Then
        assertTrue(!classes.isEmpty());
    }

    @Test
    public void shouldGetElementGenerators() throws IOException {
        // When
        final Set<Class> classes = service.getElementGenerators();

        // Then
        assertTrue(!classes.isEmpty());
    }

    @Test
    public void shouldGetObjectGenerators() throws IOException {
        // When
        final Set<Class> classes = service.getObjectGenerators();

        // Then
        assertTrue(!classes.isEmpty());
    }

    @Test
    public void shouldGetAllAvailableOperations() throws IOException {
        // When
        final Set<Class> supportedOperations = service.getOperations();

        // Then
        assertTrue(!supportedOperations.isEmpty());
        assertEquals(1, supportedOperations.size());
    }

    @Test
    public void shouldValidateWhetherOperationIsSupported() throws IOException {
        // When
        final Set<Class> supportedOperations = service.getOperations();

        for (final Class<? extends Operation> operationClass : supportedOperations) {
            // Then
            assertTrue(service.isOperationSupported(operationClass));
        }
    }

    @Test
    public void shouldSerialiseAndDeserialiseGetStoreTraits() throws IOException {
        // When
        byte[] bytes = JSONSerialiser.serialise(service.getStoreTraits());
        final Set<StoreTrait> traits = JSONSerialiser.deserialise(bytes, Set.class);

        // Then
        assertNotNull(traits);
        assertTrue("Collection size should be 6", traits.size() == 6);
        assertTrue("Collection should contain INGEST_AGGREGATION trait", traits.contains(INGEST_AGGREGATION.name()));
        assertTrue("Collection should contain PRE_AGGREGATION_FILTERING trait", traits.contains(PRE_AGGREGATION_FILTERING.name()));
        assertTrue("Collection should contain POST_AGGREGATION_FILTERING trait", traits.contains(POST_AGGREGATION_FILTERING.name()));
        assertTrue("Collection should contain POST_TRANSFORMATION_FILTERING trait", traits.contains(POST_TRANSFORMATION_FILTERING.name()));
        assertTrue("Collection should contain TRANSFORMATION trait", traits.contains(TRANSFORMATION.name()));
        assertTrue("Collection should contain STORE_VALIDATION trait", traits.contains(STORE_VALIDATION.name()));
    }
}
