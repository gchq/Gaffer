package uk.gov.gchq.gaffer.rest.integration.handler;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.rest.factory.spring.AbstractUserFactory;
import uk.gov.gchq.gaffer.rest.factory.spring.UnknownUserFactory;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.MODERN_CONFIGURATION;

import java.util.List;
import java.util.stream.Collectors;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(GremlinWebSocketIT.TestConfig.class)
@ActiveProfiles("test")
class GremlinWebSocketIT {
    private static final MapStoreProperties MAP_STORE_PROPERTIES = MapStoreProperties
            .loadStoreProperties("/tinkerpop/map-store.properties");

    @TestConfiguration
    static class TestConfig {

        @Bean
        @Profile("test")
        public GraphTraversalSource g() {
            Graph graph = GafferPopModernTestUtils.createModernGraph(TestConfig.class, MAP_STORE_PROPERTIES, MODERN_CONFIGURATION);
            return graph.traversal();
        }

        @Bean
        @Profile("test")
        public AbstractUserFactory userFactory() {
            return new UnknownUserFactory();
        }
    }

    @LocalServerPort
    private Integer port;

    @Autowired
    private GraphTraversalSource g;

    private Client client;

    @BeforeEach
    void setup() {
        Cluster cluster = Cluster.build()
            .addContactPoint("localhost")
            .port(port)
            .serializer(new GraphSONMessageSerializerV3()).create();
        this.client = cluster.connect();
    }

    @Test
    void shouldAcceptBasicQueries() {
        String query = "g.V().outE()";

        List<Result> results = client.submit(query).stream().collect(Collectors.toList());

        results.forEach(result -> assertThat(result.toString()).isEqualTo("null"));

    }

}
