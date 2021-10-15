/*
 * Copyright 2020-2021 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.controller;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.impl.predicate.IsA;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.Not;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.gchq.gaffer.store.StoreTrait.INGEST_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;

public class GraphConfigurationControllerTest {

    @Mock
    private GraphFactory graphFactory;

    @BeforeEach
    public void initialiseMocks() {
        MockitoAnnotations.initMocks(this);
        Mockito.reset(graphFactory);
    }

    @Test
    public void shouldReturnDescription() {
        // Given
        when(graphFactory.getGraph()).thenReturn(new Graph.Builder()
                .config(new GraphConfig("id"))
                .addSchema(new Schema())
                .storeProperties(new MapStoreProperties())
                .description("test graph")
                .build());

        // When
        GraphConfigurationController controller = new GraphConfigurationController(graphFactory);

        final String description = controller.getDescription();

        // Then
        assertEquals("test graph", description);
    }

    @Test
    public void shouldReturnGraphId() {
        // Given
        when(graphFactory.getGraph()).thenReturn(new Graph.Builder()
                .config(new GraphConfig("id"))
                .addSchema(new Schema())
                .storeProperties(new MapStoreProperties())
                .description("test graph")
                .build());

        // When
        GraphConfigurationController controller = new GraphConfigurationController(graphFactory);

        final String graphId = controller.getGraphId();

        // Then
        assertEquals("id", graphId);
    }

    @Test
    public void shouldGetFilterFunctions() {
        // Given
        GraphConfigurationController controller = new GraphConfigurationController(graphFactory);

        // When
        final Set<Class> classes =  controller.getFilterFunctions();

        // Then
        assertThat(classes).contains(IsA.class);
    }

    @Test
    public void shouldGetFilterFunctionsWithNullInput() {
        // Given
        GraphConfigurationController controller = new GraphConfigurationController(graphFactory);

        // When
        final Set<Class> classes =  controller.getFilterFunctions(null);

        // Then
        assertThat(classes).contains(IsA.class);
    }

    @Test
    public void shouldGetFilterFunctionsWithInputClass() {
        // Given
        GraphConfigurationController controller = new GraphConfigurationController(graphFactory);

        // When
        final Set<Class> classes = controller.getFilterFunctions(Long.class.getName());

        // Then
        assertThat(classes).contains(IsLessThan.class, IsMoreThan.class, Not.class);
    }

    @Test
    public void shouldGetFilterFunctionsUsingShortClassName() {
        // Given
        GraphConfigurationController controller = new GraphConfigurationController(graphFactory);

        // When
        final Set<Class> classes = controller.getFilterFunctions("Long");

        // Then
        assertThat(classes).contains(IsLessThan.class, IsMoreThan.class, Not.class);
    }

    @Test
    public void shouldThrowExceptionWhenGetFilterFunctionsWithUnknownClassName() {
        // Given
        GraphConfigurationController controller = new GraphConfigurationController(graphFactory);

        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> controller.getFilterFunctions("random class"))
                .withMessage("Could not find input class: random class");
    }

    @Test
    public void shouldGetSerialisedFields() {
        // Given
        GraphConfigurationController controller = new GraphConfigurationController(graphFactory);

        // When
        final Set<String> fields = controller.getSerialisedFields(IsA.class.getName());

        // Then
        assertThat(fields).hasSize(1)
                .contains("type");
    }

    @Test
    public void shouldGetSerialisedFieldsForGetElementsClass() {
        // Given
        GraphConfigurationController controller = new GraphConfigurationController(graphFactory);

        // When
        final Set<String> fields = controller.getSerialisedFields(GetElements.class.getName());

        final Set<String> expectedFields = new HashSet<>();
        expectedFields.add("input");
        expectedFields.add("view");
        expectedFields.add("includeIncomingOutGoing");
        expectedFields.add("seedMatching");
        expectedFields.add("options");
        expectedFields.add("directedType");
        expectedFields.add("views");

        // Then
        assertEquals(expectedFields, fields);
    }

    @Test
    public void shouldGetCorrectSerialisedFieldsForEdgeClass() {
        // Given
        GraphConfigurationController controller = new GraphConfigurationController(graphFactory);

        // When
        final Map<String, String> fields = controller.getSerialisedFieldClasses(Edge.class.getName());

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
        // Given
        GraphConfigurationController controller = new GraphConfigurationController(graphFactory);

        // When
        final Map<String, String> fields = controller.getSerialisedFieldClasses(GetWalks.class.getName());

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
        // Given
        GraphConfigurationController controller = new GraphConfigurationController(graphFactory);

        // When / Then
         assertThatIllegalArgumentException()
                 .isThrownBy(() -> controller.getSerialisedFields("unknown className"))
                 .withMessage("Class name was not recognised: unknown className");
    }

    @Test
    public void shouldGetStoreTraits() {
        // Given
        when(graphFactory.getGraph()).thenReturn(new Graph.Builder()
                .config(new GraphConfig("id"))
                .addSchema(new Schema())
                .storeProperties(new MapStoreProperties()) // Will be a map store
                .build());

        GraphConfigurationController controller = new GraphConfigurationController(graphFactory);


        // When
        final Set<StoreTrait> traits = controller.getStoreTraits();

        // Then
        assertEquals(MapStore.TRAITS, traits);
    }

    @Test
    public void shouldGetTransformFunctions() {
        // Given
        GraphConfigurationController controller = new GraphConfigurationController(graphFactory);

        // When
        final Set<Class> classes = controller.getTransformFunctions();

        // Then
        assertFalse(classes.isEmpty());
    }

    @Test
    public void shouldGetElementGenerators() {
        // Given
        GraphConfigurationController controller = new GraphConfigurationController(graphFactory);

        // When
        final Set<Class> classes = controller.getElementGenerators();

        // Then
        assertFalse(classes.isEmpty());
    }

    @Test
    public void shouldGetObjectGenerators() {
        // Given
        GraphConfigurationController controller = new GraphConfigurationController(graphFactory);

        // When
        final Set<Class> classes = controller.getObjectGenerators();

        // Then
        assertFalse(classes.isEmpty());
    }

    @Test
    public void shouldSerialiseAndDeserialiseGetStoreTraits() throws SerialisationException {
        // Given
        Store store = mock(Store.class);

        Schema schema = new Schema();
        StoreProperties props = new StoreProperties();
        Set<StoreTrait> storeTraits = Sets.newHashSet(
                INGEST_AGGREGATION,
                PRE_AGGREGATION_FILTERING,
                POST_AGGREGATION_FILTERING
                );

        when(store.getSchema()).thenReturn(schema);
        when(store.getProperties()).thenReturn(props);
        when(store.getTraits()).thenReturn(storeTraits);

        Graph graph = new Graph.Builder()
                .config(new GraphConfig("id"))
                .addSchema(new Schema())
                .store(store)
                .build();

        when(graphFactory.getGraph()).thenReturn(graph);

        GraphConfigurationController controller = new GraphConfigurationController(graphFactory);

        // When
        byte[] bytes = JSONSerialiser.serialise(controller.getStoreTraits());
        final Set<String> traits = JSONSerialiser.deserialise(bytes, Set.class);

        // Then
        assertEquals(Sets.newHashSet(
                INGEST_AGGREGATION.name(),
                PRE_AGGREGATION_FILTERING.name(),
                POST_AGGREGATION_FILTERING.name()
                ),
                traits);
    }


}
