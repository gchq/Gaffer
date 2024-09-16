/*
 * Copyright 2016-2023 Crown Copyright
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

import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.impl.predicate.IsA;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.Not;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static uk.gov.gchq.gaffer.store.StoreTrait.INGEST_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_TRANSFORMATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.STORE_VALIDATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.TRANSFORMATION;

@ExtendWith(MockitoExtension.class)
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

    @BeforeEach
    public void setup() throws OperationException {
        // TODO: Mockito stubbing should be personalised on a per test basis. This wasn't done, instead lenient() has been used to bypass warnings about this.
        final Set<StoreTrait> traits = new HashSet<>(Arrays.asList(INGEST_AGGREGATION, PRE_AGGREGATION_FILTERING, POST_TRANSFORMATION_FILTERING, POST_AGGREGATION_FILTERING, TRANSFORMATION, STORE_VALIDATION));
        when(store.getSchema()).thenReturn(new Schema());
        when(store.getProperties()).thenReturn(new StoreProperties());
        lenient().when(store.getGraphId()).thenReturn(GRAPH_ID);
        // Spy on the Graph, so we can stub only GetTraits Operation execution
        final Graph graph = spy(new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID) // This graphId is not used as store is mocked
                        .build())
                .description("test graph")
                .store(store)
                .build());
        lenient().doReturn(traits).when(graph).execute(any(GetTraits.class), any(Context.class));

        final StoreProperties props = new MapStoreProperties();
        lenient().when(store.getProperties()).thenReturn(props);

        final Set<Class<? extends Operation>> operations = new HashSet<>();
        operations.add(AddElements.class);
        lenient().when(graphFactory.getGraph()).thenReturn(graph);
        lenient().when(graph.getSupportedOperations()).thenReturn(operations);
        lenient().when(graph.isSupported(AddElements.class)).thenReturn(true);

        lenient().when(userFactory.createContext()).thenReturn(new Context());
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
    public void shouldReturnGraphId() {
        // When
        final int status = service.getGraphId().getStatus();
        final String graphId = service.getGraphId().getEntity().toString();

        // Then
        assertEquals(200, status);
        assertEquals(GRAPH_ID, graphId);
    }

    @Test
    public void shouldGetFilterFunctions() {
        // When
        final Set<Class> classes = (Set<Class>) service.getFilterFunction(null).getEntity();

        // Then
        assertThat(classes).contains(IsA.class);
    }

    @Test
    public void shouldGetFilterFunctionsWithInputClass() {
        // When
        final Set<Class> classes = (Set<Class>) service.getFilterFunction(Long.class.getName()).getEntity();

        // Then
        assertThat(classes).contains(IsLessThan.class, IsMoreThan.class, Not.class);
    }

    @Test
    public void shouldThrowExceptionWhenGetFilterFunctionsWithUnknownClassName() {
        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> service.getFilterFunction("unknown className"))
                .withMessageContaining("Input class was not recognised:");
    }

    @Test
    public void shouldGetSerialisedFields() {
        // When
        final Set<String> fields = (Set<String>) service.getSerialisedFields(IsA.class.getName()).getEntity();

        // Then
        assertThat(fields)
                .hasSize(1)
                .contains("type");
    }

    @Test
    public void shouldGetSerialisedFieldsForGetElementsClass() {
        // When
        final Set<String> fields = (Set<String>) service.getSerialisedFields(GetElements.class.getName()).getEntity();

        final Set<String> expectedFields = new HashSet<>();
        expectedFields.add("input");
        expectedFields.add("view");
        expectedFields.add("includeIncomingOutGoing");
        expectedFields.add("options");
        expectedFields.add("directedType");
        expectedFields.add("views");

        // Then
        assertEquals(expectedFields, fields);
    }

    @Test
    public void shouldGetCorrectSerialisedFieldsForEdgeClass() {
        // When
        final Map<String, String> fields = (Map<String, String>) service.getSerialisedFieldClasses(Edge.class.getName()).getEntity();

        final Map<String, String> expectedFields = new HashMap<>();
        expectedFields.put("class", Class.class.getName());
        expectedFields.put("source", Object.class.getName());
        expectedFields.put("destination", Object.class.getName());
        expectedFields.put("matchedVertex", String.class.getName());
        expectedFields.put("group", String.class.getName());
        expectedFields.put("properties", Properties.class.getName());
        expectedFields.put("directed", Boolean.class.getName());
        expectedFields.put("directedType", String.class.getName());

        // Then
        assertEquals(expectedFields, fields);
    }

    @Test
    public void shouldGetCorrectSerialisedFieldsForGetWalksClass() {
        // When
        final Map<String, String> fields = (Map<String, String>) service.getSerialisedFieldClasses(GetWalks.class.getName()).getEntity();

        final Map<String, String> expectedFields = new HashMap<>();
        expectedFields.put("operations", "java.util.List<uk.gov.gchq.gaffer.operation.io.Output<java.lang.Iterable<uk.gov.gchq.gaffer.data.element.Element>>>");
        expectedFields.put("input", "java.lang.Object[]");
        expectedFields.put("includePartial", "java.lang.Boolean");
        expectedFields.put("options", "java.util.Map<java.lang.String,java.lang.String>");
        expectedFields.put("resultsLimit", Integer.class.getName());
        expectedFields.put("conditional", "uk.gov.gchq.gaffer.operation.util.Conditional");

        // Then
        assertEquals(expectedFields, fields);
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
    public void shouldReturnStoreType() {
        // When
        final int status = service.getStoreType().getStatus();
        final String storeType = service.getStoreType().getEntity().toString();

        // Then
        assertEquals(200, status);
        assertEquals("uk.gov.gchq.gaffer.mapstore.MapStore", storeType);
    }

    @Test
    public void shouldGetStoreTraits() {
        // When
        final Set<StoreTrait> traits = (Set<StoreTrait>) service.getStoreTraits().getEntity();

        // Then
        assertNotNull(traits);
        assertEquals(6, traits.size(), "Collection size should be 6");
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
    public void shouldGetTransformFunctions() {
        // When
        final Set<Class> classes = (Set<Class>) service.getTransformFunctions().getEntity();

        // Then
        assertFalse(classes.isEmpty());
    }

    @Test
    public void shouldGetAggregationFunctions() {
        // When
        final Set<Class> classes = (Set<Class>) service.getAggregationFunctions().getEntity();

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
        final Set<String> traits = JSONSerialiser.deserialise(bytes, Set.class);

        // Then
        assertEquals(Sets.newHashSet(
                        INGEST_AGGREGATION.name(),
                        PRE_AGGREGATION_FILTERING.name(),
                        POST_AGGREGATION_FILTERING.name(),
                        POST_TRANSFORMATION_FILTERING.name(),
                        TRANSFORMATION.name(),
                        STORE_VALIDATION.name()
                ),
                traits);
    }
}
