package uk.gov.gchq.gaffer.rest.config;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;

@Configuration
public class GremlinConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinConfig.class);

    /**
     * Default path to look for a GafferPop properties file if not defined in the store properties.
     */
    private static final String DEFAULT_PROPERTIES = "/gaffer/gafferpop.properties";

    @Bean
    public GraphTraversalSource graphTraversalSource(final GraphFactory graphFactory) throws Exception {
        // Determine where to look for the GafferPop properties
        String gafferPopProperties = graphFactory.getGraph().getStoreProperties().get(GafferPopGraph.GAFFERPOP_PROPERTIES);
        if (gafferPopProperties == null) {
            LOGGER.warn("GafferPop properties file was not specified using default location: {}", DEFAULT_PROPERTIES);
            gafferPopProperties = DEFAULT_PROPERTIES;
        }
        // Obtain the graph traversal
        try (Graph graph = GafferPopGraph.open(new Configurations().properties(gafferPopProperties), graphFactory.getGraph())) {
            return graph.traversal();
        } catch (ConfigurationException e) {
            LOGGER.error("Error loading GafferPop config, Gremlin will be unavailable: {}", e.getMessage());
            return EmptyGraph.instance().traversal();
        }
    }
}
