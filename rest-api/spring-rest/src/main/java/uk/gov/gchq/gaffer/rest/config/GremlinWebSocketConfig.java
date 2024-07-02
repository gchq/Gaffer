package uk.gov.gchq.gaffer.rest.config;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

import uk.gov.gchq.gaffer.rest.handler.GremlinWebSocketHandler;

@Configuration
@EnableWebSocket
public class GremlinWebSocketConfig implements WebSocketConfigurer {

    private GraphTraversalSource g;

    @Autowired
    public GremlinWebSocketConfig(GraphTraversalSource g) {
        this.g = g;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(new GremlinWebSocketHandler(g), "/gremlin");
    }

}
