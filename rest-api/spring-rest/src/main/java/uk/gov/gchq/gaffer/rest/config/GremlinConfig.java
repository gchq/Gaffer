package uk.gov.gchq.gaffer.rest.config;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;

@Configuration
public class GremlinConfig {

    @Bean
    public GraphTraversalSource graphTraversalSource(final GraphFactory graphFactory) throws ConfigurationException {
        Graph graph = GafferPopGraph.open(new Configurations().properties("/gaffer/gafferpop.properties"), graphFactory.getGraph());
        return graph.traversal();
    }


}
