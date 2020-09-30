package uk.gov.gchq.gaffer.rest.controller.integration;


import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;

import uk.gov.gchq.gaffer.core.exception.Error;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.MockGraphFactory;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Set;

import static org.junit.Assert.assertEquals;
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
        assertEquals(new Schema(), response.getBody());
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
        assertEquals("test description", response.getBody());
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
        assertEquals(null, response.getBody());
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
        assertEquals("Could not find input class: a.random.thing", response.getBody().getSimpleMessage());
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
        assertEquals("Could not find input class: a.random.class", response.getBody().getSimpleMessage());
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
        assertEquals("Class name was not recognised: a.random.class", response.getBody().getSimpleMessage());
    }

}
