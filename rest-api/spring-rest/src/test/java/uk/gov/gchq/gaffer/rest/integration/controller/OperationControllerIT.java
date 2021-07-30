/*
 * Copyright 2020 Crown Copyright
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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;

import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.core.exception.Error;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.job.GetAllJobDetails;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.MockGraphFactory;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import java.io.IOException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static uk.gov.gchq.gaffer.cache.util.CacheProperties.CACHE_SERVICE_CLASS;
import static uk.gov.gchq.gaffer.core.exception.Status.SERVICE_UNAVAILABLE;
import static uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser.createDefaultMapper;

public class OperationControllerIT extends AbstractRestApiIT {

    @Autowired
    private GraphFactory graphFactory; // This will be a Mock (see application-test.properties)

    private MockGraphFactory getGraphFactory() {
        return (MockGraphFactory) graphFactory;
    }


    @Test
    public void shouldReturnHelpfulErrorMessageIfJsonIsIncorrect() {
        // Given
        Graph graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(this.getClass()))
                .storeProperties(new MapStoreProperties())
                .addSchema(new Schema())
                .build();

        when(getGraphFactory().getGraph()).thenReturn(graph);

        // When
        String request = "{\"class\"\"GetAllElements\"}";

        LinkedMultiValueMap headers = new LinkedMultiValueMap();
        headers.add("Content-Type", "application/json;charset=utf-8");

        final ResponseEntity<Error> response = post("/graph/operations/execute",
                new HttpEntity(request, headers),
                Error.class);

        // Then
        assertEquals(400, response.getStatusCode().value());
        assertEquals(400, response.getBody().getStatusCode());
        assertTrue(response.getBody().getSimpleMessage().contains("was expecting a colon to separate field name and value"));
    }

    @Test
    public void shouldReturnHelpfulErrorMessageIfOperationIsUnsupported() {
        // Given
        Graph graph = new Graph.Builder()
            .config(StreamUtil.graphConfig(this.getClass()))
            .storeProperties(new MapStoreProperties())
            .addSchema(new Schema())
            .build();

        when(getGraphFactory().getGraph()).thenReturn(graph);

        // When
        final ResponseEntity<Error> response = post("/graph/operations/execute",
            new GetAllGraphIds(),
            Error.class);

        // Then

        assertNotNull(response.getBody().getSimpleMessage());
        assertTrue(response.getBody().getSimpleMessage().contains("GetAllGraphIds is not supported by the MapStore"));
    }

    @Test
    public void shouldReturn403WhenUnauthorised() throws IOException {
        // Given
        Graph graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(this.getClass()))
                .storeProperties(new MapStoreProperties())
                .addSchema(new Schema())
                .build();

        when(getGraphFactory().getGraph()).thenReturn(graph);

        // When
        final ResponseEntity<Error> response = post("/graph/operations/execute",
                new GetAllElements(),
                Error.class);

        // Then
        assertEquals(403, response.getStatusCode().value());
        assertEquals(403, response.getBody().getStatusCode());
    }

    @Test
    public void shouldPropagateStatusInformationContainedInOperationExceptionsThrownByOperationHandlers() throws IOException {
        // Given
        final StoreProperties storeProperties = new MapStoreProperties();
        storeProperties.set(StoreProperties.JOB_TRACKER_ENABLED, Boolean.FALSE.toString());

        final Graph graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(this.getClass()))
                .storeProperties(storeProperties)
                .addSchema(new Schema())
                .build();

        when(getGraphFactory().getGraph()).thenReturn(graph);

        // When
        final ResponseEntity<Error> response = post("/graph/operations/execute",
                new GetAllJobDetails(),
                Error.class);
        // Then
        assertEquals(SERVICE_UNAVAILABLE.getStatusCode(), response.getStatusCode().value());
    }

    @Test
    public void shouldReturnSameJobIdInHeaderAsGetAllJobDetailsOperation() throws IOException {
        // Given
        StoreProperties properties = new MapStoreProperties();
        properties.setJobTrackerEnabled(true);
        properties.set(CACHE_SERVICE_CLASS, HashMapCacheService.class.getName());

        Graph graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(this.getClass()))
                .storeProperties(properties)
                .addSchema(new Schema())
                .build();

        when(getGraphFactory().getGraph()).thenReturn(graph);

        // When
        final ResponseEntity<Set> response = post("/graph/operations/execute",
                new GetAllJobDetails(),
                Set.class);

        // Then
        try {
            assertTrue(response.getBody().toString().contains(response.getHeaders().get("job-id").get(0)));
        } catch (final AssertionError e) {
            System.out.println("Job ID was not found in the Header");
            System.out.println("Header was: " + response.getHeaders().get("job-id"));
            System.out.println("Body was: " + response.getBody());
            throw e;
        }
    }

    @Test
    public void shouldCorrectlyStreamExecuteChunked() throws Exception {
        // Given
        final Schema schema =  new Schema.Builder()
                .entity("g1", new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .build())
                .type("string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(new StringConcat())
                        .build())
                .build();

        Graph graph = new Graph.Builder()
                .config(new GraphConfig("id"))
                .storeProperties(new MapStoreProperties())
                .addSchema(schema)
                .build();

        when(getGraphFactory().getGraph()).thenReturn(graph);

        Entity ent1 = new Entity.Builder()
                .group("g1")
                .vertex("v1")
                .build();

        Entity ent2 = new Entity.Builder()
                .group("g1")
                .vertex("v2")
                .build();

        final ObjectMapper mapper = createDefaultMapper();

        graph.execute(new AddElements.Builder()
                .input(ent1)
                .build(), new Context());

        graph.execute(new AddElements.Builder()
                .input(ent2)
                .build(), new Context());

        // When
        final ResponseEntity<String> response = post("/graph/operations/execute/chunked",
                new GetAllElements.Builder()
                .build(),
                String.class);

        // Then
        String expected = mapper.writeValueAsString(ent1) + "\r\n" + mapper.writeValueAsString(ent2) + "\r\n";
        assertEquals(expected, response.getBody());
    }
}
