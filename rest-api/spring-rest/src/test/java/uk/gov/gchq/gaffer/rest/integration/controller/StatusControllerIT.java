/*
 * Copyright 2021 Crown Copyright
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
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.rest.SystemStatus;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.MockGraphFactory;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class StatusControllerIT extends AbstractRestApiIT {

    @Autowired
    private GraphFactory graphFactory;

    @Before
    public void beforeEach() {
        Mockito.reset(((MockGraphFactory) graphFactory).getMock());
    }

    @Test
    public void shouldReturn500ErrorWhenGraphFactoryFailsToCreateGraph() {
        // Given
        when(graphFactory.getGraph()).thenThrow(new RuntimeException("Something went wrong"));

        // When
        ResponseEntity<Error> response = get("/graph/status", Error.class);

        // Then Check response
        checkResponse(response, 500);
        assertEquals(500, response.getBody().getStatusCode());
        assertEquals("Unable to create graph.", response.getBody().getSimpleMessage());
    }

    @Test
    public void shouldReturn200WhenGraphFactoryCreatesGraph() {
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
        ResponseEntity<SystemStatus> response = get("/graph/status", SystemStatus.class);

        // Then
        checkResponse(response, 200);
        assertEquals(SystemStatus.UP, response.getBody());
    }
}
