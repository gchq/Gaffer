/*
 * Copyright 2020-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.integration.config;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.collect.Sets;

import org.apache.datasketches.hll.HllSketch;
import org.assertj.core.data.Percentage;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.spring.AbstractUserFactory;
import uk.gov.gchq.gaffer.rest.factory.spring.UnknownUserFactory;
import uk.gov.gchq.gaffer.sketches.serialisation.json.SketchesJsonModules;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.user.User;

import javax.ws.rs.core.MediaType;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@Import(JsonSerialisationConfigIT.TestConfig.class)
@ActiveProfiles("test")
class JsonSerialisationConfigIT {

    @TestConfiguration
    static class TestConfig {

        @Bean
        @Primary
        @Profile("test")
        public GraphFactory createTestGraphFactory() {
            return new SerialisationGraphFactory();
        }

        @Bean
        @Profile("test")
        public AbstractUserFactory userFactory() {
            return new UnknownUserFactory();
        }
    }

    @Autowired
    private GraphFactory graphFactory;

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void shouldSerialiseHyperLogLogPlussesWhenSerialiserModulesConfigured() throws Exception {
        // Given
        final HyperLogLogPlus hllp = new HyperLogLogPlus(5, 5);
        hllp.offer(1);
        hllp.offer(2);

        graphFactory.getGraph().execute(new AddElements.Builder()
            .input(new Entity.Builder()
                .vertex("vertex1")
                .group("CardinalityHllp")
                .property("hllp", hllp)
                .build())
            .build(), new User());

        final JSONObject expectedHllpProperty = new JSONObject()
            .put("com.clearspring.analytics.stream.cardinality.HyperLogLogPlus", new JSONObject()
                .put("hyperLogLogPlus", new JSONObject()
                    .put("hyperLogLogPlusSketchBytes", "/////gUFAQL7C4AJ")
                    .put("cardinality", 2)));

        // When
        final JSONObject jsonQuery = new JSONObject()
            .put("class", "GetElements")
            .put("input", new JSONArray()
                .put(new JSONObject()
                    .put("class", "EntitySeed")
                    .put("vertex", "vertex1")))
            .put("view", new JSONObject()
                .put("entities", new JSONObject()
                    .put("CardinalityHllp", new JSONObject())));


         // When
        MvcResult result = mockMvc
            .perform(MockMvcRequestBuilders
                .post("/rest/graph/operations/execute")
                .content(jsonQuery.toString())
                .contentType(MediaType.APPLICATION_JSON))
            .andReturn();

        assertThat(result.getResponse().getStatus()).isEqualTo(200);

        // Get and check response
        JSONArray jsonResponse = new JSONArray(result.getResponse().getContentAsString());

        assertThat(jsonResponse).hasSize(1);
        JSONObject hllpProp = jsonResponse.getJSONObject(0).getJSONObject("properties").getJSONObject("hllp");
        assertThat(hllpProp.toMap()).isEqualTo(expectedHllpProperty.toMap());
    }

    @Test
    public void shouldSerialiseHllSketchWhenSerialiserModulesConfigured() throws Exception {
        // Given
        final HllSketch hllSketch = new HllSketch(10);
        hllSketch.update(1);
        hllSketch.update(2);

        graphFactory.getGraph().execute(new AddElements.Builder()
            .input(new Entity.Builder()
                .vertex("vertex1")
                .group("CardinalityHllSketch")
                .property("hllSketch", hllSketch)
                .build())
            .build(), new User());

        // When
        final JSONObject jsonQuery = new JSONObject()
            .put("class", "GetElements")
            .put("input", new JSONArray()
                .put(new JSONObject()
                    .put("class", "EntitySeed")
                    .put("vertex", "vertex1")))
            .put("view", new JSONObject()
                .put("entities", new JSONObject()
                    .put("CardinalityHllSketch", new JSONObject())));

        // When
        MvcResult result = mockMvc
            .perform(MockMvcRequestBuilders
                .post("/rest/graph/operations/execute")
                .content(jsonQuery.toString())
                .contentType(MediaType.APPLICATION_JSON))
            .andReturn();

        // When
        assertThat(result.getResponse().getStatus()).isEqualTo(200);

        // Get and check response
        JSONArray jsonResponse = new JSONArray(result.getResponse().getContentAsString());

        assertThat(jsonResponse).hasSize(1);

        JSONObject hllpSketch = jsonResponse.getJSONObject(0)
            .getJSONObject("properties")
            .getJSONObject("hllSketch")
            .getJSONObject(HllSketch.class.getName());

        assertThat(hllpSketch.toMap()).containsKey("cardinality");
        assertThat(hllpSketch.getDouble("cardinality")).isCloseTo(2, Percentage.withPercentage(0.001));
    }


    public static class SerialisationGraphFactory implements GraphFactory {

        private Graph graph;

        @Override
        public Graph.Builder createGraphBuilder() {
            StoreProperties props = new MapStoreProperties();
            props.setJsonSerialiserModules(Sets.newHashSet(SketchesJsonModules.class));

            return new Graph.Builder()
                .addSchema(StreamUtil.openStream(getClass(), "/cardinalitySchema/schema.json"))
                .storeProperties(props)
                .config(new GraphConfig("graph"));
        }

        @Override
        public Graph getGraph() {
            if (graph == null) {
                graph = createGraph();
            }
            return graph;
        }
    }

}
