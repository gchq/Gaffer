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
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.rest.factory.spring.AbstractUserFactory;
import uk.gov.gchq.gaffer.rest.factory.spring.UnknownUserFactory;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.StoreType;
import uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_NDJSON;
import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.MARKO;

@ExtendWith(SpringExtension.class)
@WebMvcTest(value = GremlinController.class)
@Import(GremlinControllerTest.TestConfig.class)
class GremlinControllerTest {

    private static final String GREMLIN_EXECUTE_ENDPOINT = "/rest/gremlin/execute";
    private static final String GREMLIN_EXPLAIN_ENDPOINT = "/rest/gremlin/explain";
    private static final String CYPHER_EXECUTE_ENDPOINT = "/rest/gremlin/cypher/execute";
    private static final String CYPHER_EXPLAIN_ENDPOINT = "/rest/gremlin/cypher/explain";

    @TestConfiguration
    static class TestConfig {
        @Bean
        public GraphTraversalSource g() {
            Graph graph = GafferPopModernTestUtils.createModernGraph(TestConfig.class, StoreType.MAP);
            return graph.traversal();
        }

        @Bean
        public AbstractUserFactory userFactory() {
            return new UnknownUserFactory();
        }

        @Bean
        public Long timeout() {
            return 30000L;
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private GraphTraversalSource g;

    @Test
    void shouldExecuteValidGremlinQuery() throws Exception {
        String gremlinString = "g.V('" + MARKO.getId() + "').toList()";

        // Create the expected output
        OutputStream expectedOutput = new ByteArrayOutputStream();
        GremlinController.GRAPHSON_V3_WRITER.writeObject(expectedOutput, Arrays.asList(MARKO.toVertex((GafferPopGraph) g.getGraph())));

        // When
        MvcResult result = mockMvc
            .perform(MockMvcRequestBuilders
                .post(GREMLIN_EXECUTE_ENDPOINT)
                .content(gremlinString)
                .contentType(TEXT_PLAIN_VALUE)
                .accept(APPLICATION_NDJSON))
            .andExpect(MockMvcResultMatchers.request().asyncStarted())
            .andReturn();
        // Kick of the async dispatch so the result is available
        mockMvc.perform(MockMvcRequestBuilders.asyncDispatch(result));

        // Then
        // Ensure OK response
        assertThat(result.getResponse().getStatus()).isEqualTo(200);

        // Get and check response
        assertThat(result.getResponse().getContentAsString()).isEqualTo(expectedOutput.toString());
    }

    @Test
    void shouldReturnExplainOfValidGremlinQuery() throws Exception {
        // Given
        String gremlinString = "g.V('" + MARKO.getId() + "').toList()";
        List<String> expectedOperations = Arrays.asList(GetElements.class.getName(), Limit.class.getName());

        // When
        MvcResult result = mockMvc
            .perform(MockMvcRequestBuilders
                .post(GREMLIN_EXPLAIN_ENDPOINT)
                .content(gremlinString)
                .contentType(TEXT_PLAIN_VALUE))
            .andReturn();

        // Then
        // Ensure OK response
        assertThat(result.getResponse().getStatus()).isEqualTo(200);

        // Get and check response
        JSONObject jsonResponse = new JSONObject(result.getResponse().getContentAsString());
        assertThat(jsonResponse.has(GremlinController.EXPLAIN_OVERVIEW_KEY)).isTrue();
        assertThat(jsonResponse.has(GremlinController.EXPLAIN_OP_CHAIN_KEY)).isTrue();

        // Check the operations that ran are as expected
        JSONArray operations = jsonResponse.getJSONObject("chain").getJSONArray("operations");
        assertThat(operations)
            .map(json -> ((JSONObject) json).getString("class"))
            .containsExactlyElementsOf(expectedOperations);
    }

    @Test
    void shouldRejectMalformedGremlinQueryFromExplain() throws Exception {
        // Given
        String gremlinString = "g.V().stepDoesNotExist().toList()";

        // When
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders
                        .post(GREMLIN_EXPLAIN_ENDPOINT)
                        .content(gremlinString)
                        .contentType(TEXT_PLAIN_VALUE))
                .andReturn();

        // Then
        // Expect a server error response
        assertThat(result.getResponse().getStatus()).isEqualTo(500);
    }

    @Test
    void shouldRejectMalformedGremlinQueryFromExecute() throws Exception {
        // Given
        String gremlinString = "g.V().stepDoesNotExist().toList()";

        // When
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders
                        .post(GREMLIN_EXECUTE_ENDPOINT)
                        .content(gremlinString)
                        .contentType(TEXT_PLAIN_VALUE)
                        .accept(APPLICATION_NDJSON))
                .andReturn();

        // Then
        // Expect a server error response
        assertThat(result.getResponse().getStatus()).isEqualTo(500);
    }

    @Test
    void shouldExecuteValidCypherQuery() throws Exception {
        String cypherString = "MATCH (p:person) WHERE ID(p) = '" + MARKO.getId() + "' RETURN p";

        // Create the expected output (a cypher query returns a very specific graphson format)
        JSONObject expected = new JSONObject()
            .put("@type", "g:List")
            .put("@value", new JSONArray()
                .put(new JSONObject()
                    .put("@type", "g:Map")
                    .put("@value", new JSONArray()
                        .put("p")
                        .put(new JSONObject()
                            .put("@type", "g:Map")
                            .put("@value", new JSONArray()
                                .put(new JSONObject()
                                    .put("@type", "g:T")
                                    .put("@value", "id"))
                                .put("1")
                                .put(new JSONObject()
                                    .put("@type", "g:T")
                                    .put("@value", "label"))
                                .put("person")
                                .put("name")
                                .put(new JSONObject()
                                    .put("@type", "g:List")
                                    .put("@value", new JSONArray()
                                        .put("marko")))
                                .put("age")
                                .put(new JSONObject()
                                    .put("@type", "g:List")
                                    .put("@value", new JSONArray()
                                        .put(new JSONObject()
                                            .put("@type", "g:Int32")
                                            .put("@value", 29)))))))));
        // When
        MvcResult result = mockMvc
            .perform(MockMvcRequestBuilders
                .post(CYPHER_EXECUTE_ENDPOINT)
                .content(cypherString)
                .contentType(TEXT_PLAIN_VALUE)
                .accept(APPLICATION_NDJSON))
            .andExpect(MockMvcResultMatchers.request().asyncStarted())
            .andReturn();

        // Kick of the async dispatch so the result is available
        mockMvc.perform(MockMvcRequestBuilders.asyncDispatch(result));

        // Then
        // Ensure OK response
        assertThat(result.getResponse().getStatus()).isEqualTo(200);

        // Get and check response
        assertThat(new JSONObject(result.getResponse().getContentAsString()))
            .hasToString(expected.toString());
    }

    @Test
    void shouldReturnExplainOfValidCypherQuery() throws Exception {
        // Given
        String cypherString = "MATCH (p:person) WHERE ID(p) = '" + MARKO.getId() + "' RETURN p";

        List<String> expectedOperations = Arrays.asList(GetElements.class.getName(), Limit.class.getName());

        // When
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders
                        .post(CYPHER_EXPLAIN_ENDPOINT)
                        .content(cypherString)
                        .contentType(TEXT_PLAIN_VALUE))
                .andReturn();

        // Then
        // Ensure OK response
        assertThat(result.getResponse().getStatus()).isEqualTo(200);

        // Get and check response
        JSONObject jsonResponse = new JSONObject(result.getResponse().getContentAsString());
        assertThat(jsonResponse.has(GremlinController.EXPLAIN_OVERVIEW_KEY)).isTrue();
        assertThat(jsonResponse.has(GremlinController.EXPLAIN_OP_CHAIN_KEY)).isTrue();
        assertThat(jsonResponse.has(GremlinController.EXPLAIN_GREMLIN_KEY)).isTrue();

        // Check the operations that ran are as expected
        JSONArray operations = jsonResponse.getJSONObject("chain").getJSONArray("operations");
        assertThat(operations)
                .map(json -> ((JSONObject) json).getString("class"))
                .containsExactlyElementsOf(expectedOperations);
    }

    @Test
    void shouldReturnExplainOfCypherQueryWithExtensions() throws Exception {
        // Given (uses the toInteger custom function)
        String cypherString = "MATCH (p:person) WHERE p.age > toInteger(22) RETURN p";

        List<String> expectedOperations = Arrays.asList(GetAllElements.class.getName(), Limit.class.getName());

        // When
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders
                        .post(CYPHER_EXPLAIN_ENDPOINT)
                        .content(cypherString)
                        .contentType(TEXT_PLAIN_VALUE))
                .andReturn();

        // Then
        // Ensure OK response
        assertThat(result.getResponse().getStatus()).isEqualTo(200);

        // Get and check response
        JSONObject jsonResponse = new JSONObject(result.getResponse().getContentAsString());
        assertThat(jsonResponse.has(GremlinController.EXPLAIN_OVERVIEW_KEY)).isTrue();
        assertThat(jsonResponse.has(GremlinController.EXPLAIN_OP_CHAIN_KEY)).isTrue();
        assertThat(jsonResponse.has(GremlinController.EXPLAIN_GREMLIN_KEY)).isTrue();

        // Check the operations that ran are as expected
        JSONArray operations = jsonResponse.getJSONObject("chain").getJSONArray("operations");
        assertThat(operations)
                .map(json -> ((JSONObject) json).getString("class"))
                .containsExactlyElementsOf(expectedOperations);
    }

    @Test
    void shouldRejectMalformedCypherQueryFromExplain() throws Exception {
        // Given
        String cypherString = "MATCH (p:person) WHERE RETURN p";

        // When
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders
                        .post(CYPHER_EXPLAIN_ENDPOINT)
                        .content(cypherString)
                        .contentType(TEXT_PLAIN_VALUE))
                .andReturn();

        // Then
        // Expect a server error response
        assertThat(result.getResponse().getStatus()).isEqualTo(500);
    }

    @Test
    void shouldRejectMalformedCypherQueryFromExecute() throws Exception {
        // Given
        String cypherString = "MATCH (p:person) WHERE RETURN p";

        // When
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders
                        .post(CYPHER_EXECUTE_ENDPOINT)
                        .content(cypherString)
                        .contentType(TEXT_PLAIN_VALUE)
                        .accept(APPLICATION_NDJSON))
                .andReturn();

        // Then
        // Expect a server error response
        assertThat(result.getResponse().getStatus()).isEqualTo(500);
    }


}
