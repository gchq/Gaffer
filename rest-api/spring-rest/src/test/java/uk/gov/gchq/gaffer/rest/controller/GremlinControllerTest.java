package uk.gov.gchq.gaffer.rest.controller;

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
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.MODERN_CONFIGURATION;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;;

@ExtendWith(SpringExtension.class)
@WebMvcTest(value = GremlinController.class)
@Import(GremlinControllerTest.TestConfig.class)
public class GremlinControllerTest {

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
    void canGetTraversal() throws Exception {

        //assertThat(controller.execute("g.V()")).isEqualTo("null");

        MvcResult result = mockMvc.perform(MockMvcRequestBuilders.post("/gremlin/execute").content("g.V().toList()")).andReturn();

        // Ensure OK response
        assertThat(result.getResponse().getStatus()).isEqualTo(200);


        assertThat(result.getResponse().getContentAsString()).isEqualTo("null");
    }



}
