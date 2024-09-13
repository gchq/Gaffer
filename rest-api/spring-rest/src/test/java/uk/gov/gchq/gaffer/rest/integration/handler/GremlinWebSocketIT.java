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

import uk.gov.gchq.gaffer.rest.factory.spring.AbstractUserFactory;
import uk.gov.gchq.gaffer.rest.factory.spring.UnknownUserFactory;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.StoreType;
import uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphVariables.CYPHER_KEY;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.JOSH;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.MARKO;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.PETER;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.VADAS;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Integration testing for the Gremlin websocket. Testing is fairly simple as
 * GafferPop is more heavily tested in its own module, focus of testing here is
 * ensuring the websocket can at least accept Gremlin and GafferPop related
 * queries via a standard tinkerpop client connection.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(GremlinWebSocketIT.TestConfig.class)
@ActiveProfiles("test")
class GremlinWebSocketIT {

    @TestConfiguration
    static class TestConfig {

        @Bean
        @Profile("test")
        public GraphTraversalSource g() {
            Graph graph = GafferPopModernTestUtils.createModernGraph(TestConfig.class, StoreType.MAP);
            return graph.traversal();
        }

        @Bean
        @Profile("test")
        public AbstractUserFactory userFactory() {
            return new UnknownUserFactory();
        }

        @Bean
        @Profile("test")
        public Long timeout() {
            return 30000L;
        }
    }

    @LocalServerPort
    private Integer port;

    @Autowired
    private GraphTraversalSource g;

    private Client client;

    @BeforeEach
    void setup() {
        // Set up a client connection to the server
        Cluster cluster = Cluster.build()
            .addContactPoint("localhost")
            .port(port)
            .serializer(new GraphSONMessageSerializerV3()).create();
        this.client = cluster.connect();
    }

    @Test
    void shouldAcceptBasicGremlinQueries() {
        // Given
        String query = "g.V().hasLabel('person').toList()";

        // When
        List<Result> results = client.submit(query).stream().collect(Collectors.toList());

        // Then
        assertThat(results)
            .map(result -> result.getElement().id())
            .containsExactlyInAnyOrder(
                MARKO.getId(),
                VADAS.getId(),
                PETER.getId(),
                JOSH.getId());
    }

    @Test
    void shouldRejectMalformedGremlinQueries() {
        // Given
        String query = "g.V().thisStepDoesNotExist().toList()";

        // When/Then
        assertThatExceptionOfType(ExecutionException.class)
            .isThrownBy(() -> client.submit(query).all().get())
            .withMessageContaining("groovy.lang.MissingMethodException");
    }

    @Test
    void shouldAcceptQueryWithCypher() {
        // Given
        String cypherQuery = "MATCH (p:person) WHERE ID(p) = '" + MARKO.getId() + "' RETURN p.name";
        String gremlinQuery = "g.with(\"" + CYPHER_KEY + "\", " + "\"" + cypherQuery + "\").call().toList()";

        // When
        List<Result> results = client.submit(gremlinQuery).stream().collect(Collectors.toList());

        // Then
        // Cypher returns each result under the project name e.g. 'p' so we need to extract
        assertThat(results)
            .flatMap(result -> ((LinkedHashMap<Object, Object>) result.getObject()).values())
            .containsExactly(MARKO.getName());
    }

    @Test
    void shouldAcceptGremlinQueryUsingCustomCypherFunctions() {
        // Given
        String query = "g.V().hasLabel('person').values('age').map(cypherToString()).toList()";

        // When
        List<Result> results = client.submit(query).stream().collect(Collectors.toList());

        // Then
        assertThat(results)
            .map(result -> result.getObject())
            .containsExactlyInAnyOrder(
                String.valueOf(MARKO.getAge()),
                String.valueOf(VADAS.getAge()),
                String.valueOf(PETER.getAge()),
                String.valueOf(JOSH.getAge()));
    }

}
