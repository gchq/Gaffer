package uk.gov.gchq.gaffer.tinkerpop;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.TEST_CONFIGURATION_1;

import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.ExecutorService;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopFederatedTestUtil;

public class GafferPopFederatedIT {
    private static Graph federatedGraph;
    private static GafferPopGraph gafferPopGraph;
    
    public static void tearDown() {
        HashMapGraphLibrary.clear();
        CacheServiceLoader.shutdown();
        ExecutorService.shutdown();
    }

    @Test
    public void shouldConstructFederatedGafferPopGraph() throws Exception {
        // Given
        federatedGraph = GafferPopFederatedTestUtil.setUpFederatedGraph(GafferPopFederatedIT.class);
        gafferPopGraph = GafferPopGraph.open(TEST_CONFIGURATION_1, federatedGraph);

        // When
        final Map<String, Object> variables = gafferPopGraph.variables().asMap();

        // Then
        assertThat(variables.get(GafferPopGraphVariables.SCHEMA)).isEqualTo(federatedGraph.getSchema());
        assertThat(variables.get(GafferPopGraphVariables.USER)).hasFieldOrPropertyWithValue("userId", "user01");

        tearDown();
    }
    
    @Test
    public void shouldGetAllElements() throws Exception {
        // Given
        federatedGraph = GafferPopFederatedTestUtil.setUpFederatedGraph(GafferPopFederatedIT.class);
        gafferPopGraph = GafferPopGraph.open(TEST_CONFIGURATION_1, federatedGraph);
        
        // When
        GraphTraversalSource g = gafferPopGraph.traversal();

        GraphTraversal<Vertex, Vertex> elements = g.V();

        // Then
        assertThat(elements)
            .toIterable()
            .hasSize(6);
            // TODO: check return statement matches what you expect

        tearDown();
    }

    @Test
    public void shouldGetAllEdges() throws Exception {
        // Given
        federatedGraph = GafferPopFederatedTestUtil.setUpFederatedGraph(GafferPopFederatedIT.class);
        gafferPopGraph = GafferPopGraph.open(TEST_CONFIGURATION_1, federatedGraph);

        // When
        GraphTraversalSource g = gafferPopGraph.traversal();

        GraphTraversal<Edge, Edge> results = g.E().hasLabel("software");

        // Then
        
    }
}