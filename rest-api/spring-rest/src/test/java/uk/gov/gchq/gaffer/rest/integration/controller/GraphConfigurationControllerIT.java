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

package uk.gov.gchq.gaffer.rest.integration.controller;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;

import uk.gov.gchq.gaffer.core.exception.Error;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.MockGraphFactory;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class GraphConfigurationControllerIT extends AbstractRestApiIT {

    @Autowired
    private GraphFactory graphFactory;

    private MockGraphFactory getMockFactory() {
        return (MockGraphFactory) graphFactory;
    }

    @Before
    public void resetMocks() {
        Mockito.reset(getMockFactory().getMock());
    }

    @Test
    public void shouldReturn200WhenReturningSchema() {
        // Given
        Graph emptyGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("id")
                        .build())
                .storeProperties(new MapStoreProperties())
                .addSchema(new Schema())
                .build();

        when(graphFactory.getGraph()).thenReturn(emptyGraph);

        // When
        ResponseEntity<Schema> response = get("/graph/config/schema", Schema.class);

        // Then
        checkResponse(response, 200);
        assertThat(response.getBody()).isEqualTo(new Schema());
    }

    @Test
    public void shouldReturn200WhenReturningDescription() {
        // Given
        Graph emptyGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("id")
                        .description("test description")
                        .build())
                .storeProperties(new MapStoreProperties())
                .addSchema(new Schema())
                .build();

        when(graphFactory.getGraph()).thenReturn(emptyGraph);

        // When
        ResponseEntity<String> response = get("/graph/config/description", String.class);

        // Then
        checkResponse(response, 200);
        assertThat(response.getBody()).isEqualTo("test description");
    }

    @Test
    public void shouldReturn200WhenReturningDescriptionIfUnset() {
        // Given
        Graph emptyGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("id")
                        // no description
                        .build())
                .storeProperties(new MapStoreProperties())
                .addSchema(new Schema())
                .build();

        when(graphFactory.getGraph()).thenReturn(emptyGraph);

        // When
        ResponseEntity<String> response = get("/graph/config/description", String.class);

        // Then
        checkResponse(response, 200);
        assertThat(response.getBody()).isNull();
    }

    @Test
    public void shouldReturn200WhenReturningGraphId() {
        // Given
        Graph emptyGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("id")
                        .description("test description")
                        .build())
                .storeProperties(new MapStoreProperties())
                .addSchema(new Schema())
                .build();

        when(graphFactory.getGraph()).thenReturn(emptyGraph);

        // When
        ResponseEntity<String> response = get("/graph/config/graphId", String.class);

        // Then
        checkResponse(response, 200);
        assertThat(response.getBody()).isEqualTo("id");
    }

    @Test
    public void shouldReturn200WithListOfAllFilterFunctions() {
        // Given
        Graph emptyGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("id")
                        .build())
                .storeProperties(new MapStoreProperties())
                .addSchema(new Schema())
                .build();

        when(graphFactory.getGraph()).thenReturn(emptyGraph);

        // When
        ResponseEntity<Set> response = get("/graph/config/filterFunctions", Set.class);

        // Then
        checkResponse(response, 200);
    }

    @Test
    public void shouldReturn200WithListOfAllTransformFunctions() {
        // Given
        Graph emptyGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("id")
                        .build())
                .storeProperties(new MapStoreProperties())
                .addSchema(new Schema())
                .build();

        when(graphFactory.getGraph()).thenReturn(emptyGraph);

        // When
        ResponseEntity<Set> response = get("/graph/config/transformFunctions", Set.class);

        // Then
        checkResponse(response, 200);
    }

    @Test
    public void shouldReturn200WithListOfAllObjectGenerators() {
        // Given
        Graph emptyGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("id")
                        .build())
                .storeProperties(new MapStoreProperties())
                .addSchema(new Schema())
                .build();

        when(graphFactory.getGraph()).thenReturn(emptyGraph);

        // When
        ResponseEntity<Set> response = get("/graph/config/objectGenerators", Set.class);

        // Then
        checkResponse(response, 200);
    }

    @Test
    public void shouldReturn200WithListOfAllElementGenerators() {
        // Given
        Graph emptyGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("id")
                        .build())
                .storeProperties(new MapStoreProperties())
                .addSchema(new Schema())
                .build();

        when(graphFactory.getGraph()).thenReturn(emptyGraph);

        // When
        ResponseEntity<Set> response = get("/graph/config/objectGenerators", Set.class);

        // Then
        checkResponse(response, 200);
    }

    @Test
    public void shouldReturn200WithAllStoreTraits() {
        // Given
        Graph emptyGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("id")
                        .build())
                .storeProperties(new MapStoreProperties())
                .addSchema(new Schema())
                .build();

        when(graphFactory.getGraph()).thenReturn(emptyGraph);

        // When
        ResponseEntity<Set> response = get("/graph/config/storeTraits", Set.class);

        // The response will come back as a Set of strings. We'll convert these to store traits
        Set<StoreTrait> responseTraits = new HashSet<>();
        response.getBody().forEach(trait -> responseTraits.add(StoreTrait.valueOf((String) trait)));

        // Then
        checkResponse(response, 200);
        assertThat(responseTraits).isEqualTo(MapStore.TRAITS);
    }

    @Test
    public void shouldReturn500WhenANonExistentClassIsProvidedForGetFilterFunctions() {
        // Given
        Graph emptyGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("id")
                        .build())
                .storeProperties(new MapStoreProperties())
                .addSchema(new Schema())
                .build();

        when(graphFactory.getGraph()).thenReturn(emptyGraph);

        // When
        ResponseEntity<Error> response = get("/graph/config/filterFunctions/a.random.thing", Error.class);

        // Then
        checkResponse(response, 500);
        assertThat(response.getBody().getSimpleMessage()).isEqualTo("Could not find input class: a.random.thing");
    }

    @Test
    public void shouldReturn500WhenANonExistentClassIsProvidedEndingInClassForGetFilterFunctions() {
        // Given
        Graph emptyGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("id")
                        .build())
                .storeProperties(new MapStoreProperties())
                .addSchema(new Schema())
                .build();

        when(graphFactory.getGraph()).thenReturn(emptyGraph);

        // When
        ResponseEntity<Error> response = get("/graph/config/filterFunctions/a.random.class", Error.class);

        // Then
        checkResponse(response, 500);
        assertThat(response.getBody().getSimpleMessage()).isEqualTo("Could not find input class: a.random.class");
    }

    @Test
    public void shouldReturn500WhenANonExistentClassIsProvidedForGetSerialiseFields() {
        // Given
        Graph emptyGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("id")
                        .build())
                .storeProperties(new MapStoreProperties())
                .addSchema(new Schema())
                .build();

        when(graphFactory.getGraph()).thenReturn(emptyGraph);

        // When
        ResponseEntity<Error> response = get("/graph/config/serialisedFields/a.random.class", Error.class);

        // Then
        checkResponse(response, 500);
        assertThat(response.getBody().getSimpleMessage()).isEqualTo("Class name was not recognised: a.random.class");
    }

}
