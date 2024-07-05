/*
 * Copyright 2024 Crown Copyright
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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.MODERN_CONFIGURATION;

@ExtendWith(SpringExtension.class)
@WebMvcTest(value = GremlinController.class)
@Import(GremlinControllerTest.TestConfig.class)
class GremlinControllerTest {

    private static final MapStoreProperties MAP_STORE_PROPERTIES = MapStoreProperties.loadStoreProperties("/tinkerpop/map-store.properties");

    @TestConfiguration
    static class TestConfig {
        @Bean
        public GraphTraversalSource g() {
            Graph graph = GafferPopModernTestUtils.createModernGraph(TestConfig.class, MAP_STORE_PROPERTIES, MODERN_CONFIGURATION);
            return graph.traversal();
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private GraphTraversalSource g;

    @Test
    void shouldReturnExplainOfValidGremlinQuery() throws Exception {
        // Given
        String gremlinString = "g.V().toList()";
        List<String> expectedOperations = Arrays.asList(GetAllElements.class.getName(), Limit.class.getName());

        // When
        MvcResult result = mockMvc
            .perform(MockMvcRequestBuilders
                .post("/rest/gremlin/explain")
                .content(gremlinString)
                .contentType(TEXT_PLAIN_VALUE))
            .andReturn();

        // Then
        // Ensure OK response
        assertThat(result.getResponse().getStatus()).isEqualTo(200);

        // Get and check response
        JSONObject jsonResponse = new JSONObject(result.getResponse().getContentAsString());
        assertThat(jsonResponse.has("overview")).isTrue();
        assertThat(jsonResponse.has("chain")).isTrue();

        // Check the operations that ran are as expected
        JSONArray operations = jsonResponse.getJSONObject("chain").getJSONArray("operations");
        assertThat(operations)
            .map(json -> ((JSONObject) json).getString("class"))
            .containsExactlyElementsOf(expectedOperations);
    }

    @Test
    void shouldRejectMalformedGremlinQuery() throws Exception {
        // Given
        String gremlinString = "g.V().stepDoesNotExist().toList()";

        // When
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders
                        .post("/rest/gremlin/explain")
                        .content(gremlinString)
                        .contentType(TEXT_PLAIN_VALUE))
                .andReturn();

        // Then
        // Expect a server error response
        assertThat(result.getResponse().getStatus()).isEqualTo(500);
    }

    @Test
    void shouldReturnExplainOfValidCypherQuery() throws Exception {
        // Given
        String cypherString = "MATCH (p:person) WHERE ID(p) = '1' RETURN p";
        List<String> expectedOperations = Arrays.asList(GetElements.class.getName());

        // When
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders
                        .post("/rest/gremlin/cypher/explain")
                        .content(cypherString)
                        .contentType(TEXT_PLAIN_VALUE))
                .andReturn();

        // Then
        // Ensure OK response
        assertThat(result.getResponse().getStatus()).isEqualTo(200);

        // Get and check response
        JSONObject jsonResponse = new JSONObject(result.getResponse().getContentAsString());
        assertThat(jsonResponse.has("overview")).isTrue();
        assertThat(jsonResponse.has("chain")).isTrue();
        assertThat(jsonResponse.has("gremlin")).isTrue();

        // Check the operations that ran are as expected
        JSONArray operations = jsonResponse.getJSONObject("chain").getJSONArray("operations");
        assertThat(operations)
                .map(json -> ((JSONObject) json).getString("class"))
                .containsExactlyElementsOf(expectedOperations);
    }

    @Test
    void shouldRejectMalformedCypherQuery() throws Exception {
        // Given
        String cypherString = "MATCH (p:person) WHERE RETURN p";

        // When
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders
                        .post("/rest/gremlin/cypher/explain")
                        .content(cypherString)
                        .contentType(TEXT_PLAIN_VALUE))
                .andReturn();

        // Then
        // Expect a server error response
        assertThat(result.getResponse().getStatus()).isEqualTo(500);
    }


}
