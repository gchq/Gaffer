/*
 * Copyright 2016-2021 Crown Copyright
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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
import uk.gov.gchq.gaffer.store.StoreProperties;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static uk.gov.gchq.gaffer.store.StoreTrait.INGEST_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_TRANSFORMATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.STORE_VALIDATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.TRANSFORMATION;

@ExtendWith(MockitoExtension.class)
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

    @BeforeEach
    public void setup() {
        final Set<StoreTrait> traits = new HashSet<>(Arrays.asList(INGEST_AGGREGATION, PRE_AGGREGATION_FILTERING, POST_TRANSFORMATION_FILTERING, POST_AGGREGATION_FILTERING, TRANSFORMATION, STORE_VALIDATION));
        lenient().when(store.getSchema()).thenReturn(new Schema());
        lenient().when(store.getProperties()).thenReturn(new StoreProperties());
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .store(store)
                .build();
        final Set<Class<? extends Operation>> operations = new HashSet<>();
        operations.add(AddElements.class);
        lenient().when(graphFactory.getGraph()).thenReturn(graph);
        lenient().when(graph.getSupportedOperations()).thenReturn(operations);
        lenient().when(graph.isSupported(AddElements.class)).thenReturn(true);

        lenient().when(userFactory.createContext()).thenReturn(new Context());

        lenient().when(graph.getStoreTraits()).thenReturn(traits);
    }

    @Test
    public void shouldGetFilterFunctions() throws IOException {
        // When
        final Set<Class> classes = service.getFilterFunctions(null);

        // Then
        assertThat(classes).contains(IsA.class);
    }

    @Test
    public void shouldGetFilterFunctionsWithInputClass() throws IOException {
        // When
        final Set<Class> classes = service.getFilterFunctions(Long.class.getName());

        // Then
        assertThat(classes).contains(IsLessThan.class, IsMoreThan.class, Not.class);
    }

    @Test
    public void shouldThrowExceptionWhenGetFilterFunctionsWithUnknownClassName() throws IOException {
        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> service.getFilterFunctions("an unknown class name"))
                .extracting("message")
                .isNotNull();
    }

    @Test
    public void shouldGetSerialisedFields() throws IOException {
        // When
        final Set<String> fields = service.getSerialisedFields(IsA.class.getName());

        // Then
        assertThat(fields).hasSize(1)
                .contains("type");
    }

    @Test
    public void shouldThrowExceptionWhenGetSerialisedFieldsWithUnknownClassName() throws IOException {
        // When / Then
        assertThatIllegalArgumentException().isThrownBy(() -> service.getSerialisedFields("an unknown class name")).extracting("message").isNotNull();
    }

    @Test
    public void shouldGetNextOperations() throws IOException {
        // Given
        final Set<Class<? extends Operation>> expectedNextOperations = mock(Set.class);
        lenient().when(store.getNextOperations(GetElements.class)).thenReturn(expectedNextOperations);

        // When
        final Set<Class> nextOperations = service.getNextOperations(GetElements.class.getName());

        // Then
        assertSame(expectedNextOperations, nextOperations);
    }

    @Test
    public void shouldThrowExceptionWhenGetNextOperationsWithUnknownClassName() throws IOException {
        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> service.getNextOperations("an unknown class name"))
                .withMessageContaining("Operation class was not found");
    }

    @Test
    public void shouldThrowExceptionWhenGetNextOperationsWithNonOperationClassName() throws IOException {
        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> service.getNextOperations(String.class.getName()))
                .withMessageContaining("does not extend Operation");
    }

    @Test
    public void shouldGetStoreTraits() throws IOException {
        // When
        final Set<StoreTrait> traits = service.getStoreTraits();
        // Then
        assertNotNull(traits);
        assertThat(traits).as("Collection size should be 6").hasSize(6);
        assertTrue(traits.contains(INGEST_AGGREGATION),
                "Collection should contain INGEST_AGGREGATION trait");
        assertTrue(traits.contains(PRE_AGGREGATION_FILTERING),
                "Collection should contain PRE_AGGREGATION_FILTERING trait");
        assertTrue(traits.contains(POST_AGGREGATION_FILTERING),
                "Collection should contain POST_AGGREGATION_FILTERING trait");
        assertTrue(traits.contains(POST_TRANSFORMATION_FILTERING),
                "Collection should contain POST_TRANSFORMATION_FILTERING trait");
        assertTrue(traits.contains(TRANSFORMATION),
                "Collection should contain TRANSFORMATION trait");
        assertTrue(traits.contains(STORE_VALIDATION),
                "Collection should contain STORE_VALIDATION trait");
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
        assertThat(supportedOperations).hasSize(1);
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
        assertEquals(6, traits.size(), "Collection size should be 6");
        assertTrue(traits.contains(INGEST_AGGREGATION.name()),
                "Collection should contain INGEST_AGGREGATION trait");
        assertTrue(traits.contains(PRE_AGGREGATION_FILTERING.name()),
                "Collection should contain PRE_AGGREGATION_FILTERING trait");
        assertTrue(traits.contains(POST_AGGREGATION_FILTERING.name()),
                "Collection should contain POST_AGGREGATION_FILTERING trait");
        assertTrue(traits.contains(POST_TRANSFORMATION_FILTERING.name()),
                "Collection should contain POST_TRANSFORMATION_FILTERING trait");
        assertTrue(traits.contains(TRANSFORMATION.name()),
                "Collection should contain TRANSFORMATION trait");
        assertTrue(traits.contains(STORE_VALIDATION.name()),
                "Collection should contain STORE_VALIDATION trait");
    }
}
