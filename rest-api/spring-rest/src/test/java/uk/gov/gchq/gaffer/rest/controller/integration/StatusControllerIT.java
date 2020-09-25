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

package uk.gov.gchq.gaffer.rest.controller.integration;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.TestRestTemplate;
import org.springframework.boot.test.WebIntegrationTest;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.web.client.RestTemplate;

import uk.gov.gchq.gaffer.core.exception.Error;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.rest.Application;
import uk.gov.gchq.gaffer.rest.SystemStatus;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.MockGraphFactory;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(Application.class)
@WebIntegrationTest(randomPort = true)
public class StatusControllerIT {

    private RestTemplate restTemplate = new TestRestTemplate();

    @Value("${local.server.port}")
    private int port;

    @Value("${server.context-path}")
    private String contextPath;

    @Autowired
    private GraphFactory graphFactory;

    private MockGraphFactory getGraphFactory() {
        return (MockGraphFactory) graphFactory;
    }

    @Before
    public void beforeEach() {
        Mockito.reset(((MockGraphFactory) graphFactory).getMock());
    }

    @Test
    public void shouldReturn500ErrorWhenGraphFactoryFailsToCreateGraph() {
        // Given
        when(graphFactory.getGraph()).thenThrow(new RuntimeException("Something went wrong"));

        // When
        ResponseEntity<Error> response = restTemplate.getForEntity("http://localhost:" + port + "/" + contextPath + "/graph/status", Error.class);

        // Then Check response
        assertEquals(500, response.getStatusCode().value());
        assertEquals(500, response.getBody().getStatusCode());
        assertEquals("Unable to create graph.", response.getBody().getSimpleMessage());

        // Then check Gaffer header
        assertTrue("Gaffer header was not present", response.getHeaders().containsKey(GAFFER_MEDIA_TYPE_HEADER));
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
        ResponseEntity<SystemStatus> response = restTemplate.getForEntity("http://localhost:" + port + "/" + contextPath + "/graph/status", SystemStatus.class);

        // Then
        assertEquals(200, response.getStatusCode().value());
        assertEquals(SystemStatus.UP, response.getBody());

        // Then check Gaffer header
        assertTrue("Gaffer header was not present", response.getHeaders().containsKey(GAFFER_MEDIA_TYPE_HEADER));
    }
}
