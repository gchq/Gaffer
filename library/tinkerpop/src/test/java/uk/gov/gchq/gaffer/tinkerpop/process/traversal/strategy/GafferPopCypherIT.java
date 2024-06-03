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

package uk.gov.gchq.gaffer.tinkerpop.process.traversal.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.process.traversal.step.GafferPopHasStepIT;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.tinkerpop.process.traversal.strategy.optimisation.GafferPopGraphStepStrategy.CYPHER_KEY;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.MODERN_CONFIGURATION;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.PETER;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.VADAS;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.JOSH;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.MARKO;

public class GafferPopCypherIT {

    private static final MapStoreProperties MAP_STORE_PROPERTIES = MapStoreProperties.loadStoreProperties("/tinkerpop/map-store.properties");
    private static GraphTraversalSource g;

    @BeforeAll
    public static void beforeAll() {
        GafferPopGraph gafferPopGraph = GafferPopModernTestUtils.createModernGraph(GafferPopHasStepIT.class, MAP_STORE_PROPERTIES, MODERN_CONFIGURATION);
        g = gafferPopGraph.traversal();
    }

    @Test
    void shouldTranslateCypherWithSeededID() {
        // Given
        final String cypherQuery = "MATCH (p:person) WHERE ID(p) = '1' RETURN p";
        // When
        // Check we can do a seeded query with ID 1
        Map<Object, Object> results = ((LinkedHashMap<Object, Object>) g
                .with(CYPHER_KEY, cypherQuery)
                .call()
                .next());
        // Make sure only one result
        assertThat(results).size().isOne();

        // The cypher translator will return a property map of the matched node so get that
        Map<Object, Object> resultMap = (LinkedHashMap<Object, Object>) results.get("p");

        assertThat(resultMap).containsAllEntriesOf(MARKO.getPropertyMap());
    }

    @Test
    void shouldTranslateCypherWithPredicate() {
        // Given
        // Get names of all people older than 30
        final String cypherQuery = "MATCH (p:person) WHERE p.age > 30 RETURN p.name";

        // When
        List<Object> results = g
            .with(CYPHER_KEY, cypherQuery)
            .call()
            .toList();
        // Flatten the results as they will be like [{p.name=peter}, {p.name=josh} ...]
        List<Object> flattenedResults = results.stream()
            .flatMap(result ->((LinkedHashMap<Object, Object>) result).values().stream())
            .collect(Collectors.toList());

        // Then
        assertThat(flattenedResults).containsExactlyInAnyOrder(PETER.getName(), JOSH.getName());
    }

    @Test
    void shouldTranslateCypherEdgeTraversal() {
        // Given
        // Finds all the people marko 'knows'
        final String cypherQuery = "MATCH (marko:person {name: 'marko'})-[:knows]->(p) RETURN p.name";

        // When
        List<Object> results = g
            .with(CYPHER_KEY, cypherQuery)
            .call()
            .toList();
        // Flatten the results as they will be like [{p.name=peter}, {p.name=josh} ...]
        List<Object> flattenedResults = results.stream()
            .flatMap(result -> ((LinkedHashMap<Object, Object>) result).values().stream())
            .collect(Collectors.toList());

        // Then
        assertThat(flattenedResults).containsExactlyInAnyOrder(JOSH.getName(), VADAS.getName());
    }

}
