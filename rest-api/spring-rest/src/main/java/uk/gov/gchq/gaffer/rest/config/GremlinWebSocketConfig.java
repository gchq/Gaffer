package uk.gov.gchq.gaffer.rest.config;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.handler.GremlinWebSocketHandler;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;

@Configuration
@EnableWebSocket
public class GremlinWebSocketConfig implements WebSocketConfigurer {

    private GraphTraversalSource g;

    @Autowired
    public GremlinWebSocketConfig(GraphFactory graphFactory) throws Exception {
        try (Graph graph = GafferPopGraph.open(new Configurations().properties("/gaffer/gafferpop.properties"), graphFactory.getGraph())) {
            g = graph.traversal();
        }
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(new GremlinWebSocketHandler(g), "/gremlin");
    }

}
