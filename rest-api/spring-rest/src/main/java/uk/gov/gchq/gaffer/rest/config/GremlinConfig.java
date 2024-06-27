package uk.gov.gchq.gaffer.rest.config;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;

@Configuration
public class GremlinConfig {

    @Bean
    public GraphTraversalSource graphTraversalSource(final GraphFactory graphFactory) throws ConfigurationException {
        Graph graph = GafferPopGraph.open(new Configurations().properties("/gafferpop.properties"), graphFactory.getGraph());
        return graph.traversal();
    }


}
