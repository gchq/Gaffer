/*
 * Copyright 2023 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.cucumber;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import io.cucumber.guice.CucumberModules;
import io.cucumber.guice.GuiceFactory;
import io.cucumber.guice.InjectorSource;
import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.features.World;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoResourceAccess;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;

@RunWith(Cucumber.class)
@CucumberOptions(
        tags = "not @RemoteOnly and not @MultiProperties and not @MetaProperties and not @GraphComputerOnly and not @AllowNullPropertyValues and not @UserSuppliedVertexPropertyIds and not @UserSuppliedEdgeIds and not @UserSuppliedVertexIds and not @TinkerServiceRegistry",
        glue = { "org.apache.tinkerpop.gremlin.features" },
        objectFactory = GuiceFactory.class,
        features = { "classpath:/org/apache/tinkerpop/gremlin/test/features" },
        plugin = {"junit:target/cucumber-reports/cucumber.xml", "html:target/cucumber-reports/cucumber.html"})
public final class GafferPopFeatureTest {

    public static final String TEST_USER_ID = "tinkerpopTestUser";
    public static final String[] TEST_OP_OPTIONS = new String[] {"key1:value1", "key2:value2"};
    public static final String TEST_STORE_PROPS = GafferPopFeatureTest.class.getClassLoader().getResource("gaffer/map-store.properties").getPath();
    public static final String TEST_TYPES_SCHEMA = GafferPopFeatureTest.class.getClassLoader().getResource("tinkerpop/schema/types").getPath();

    private static final Logger LOGGER = LoggerFactory.getLogger(GafferPopFeatureTest.class);

    private GafferPopFeatureTest() {
        // Utility class
    }

    public static final class ServiceModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(World.class).to(GafferPopGraphWorld.class);
        }
    }

    public static final class WorldInjectorSource implements InjectorSource {
        @Override
        public Injector getInjector() {
            return Guice.createInjector(Stage.PRODUCTION, CucumberModules.createScenarioModule(), new ServiceModule());
        }
    }

    /**
     * Provides the GafferPop implementation of a Tinkerpop world used for testing via Cucumber.
     * This is essentially responsible for preloading the graph data and configurations up front
     * for testing.
     * Note: Only the 'MODERN' graph data is currently supported but more could be added.
     */
    public static class GafferPopGraphWorld implements World {

        private static final GafferPopGraph MODERN = GafferPopGraph.open(new MapConfiguration(getBaseConfiguration(GraphData.MODERN)));
        private static final GafferPopGraph EMPTY = GafferPopGraph.open(new MapConfiguration(getBaseConfiguration(null)));

        static {
            readIntoGraph(MODERN, GraphData.MODERN);
        }

        @Override
        public GraphTraversalSource getGraphTraversalSource(final GraphData graphData) {
            if (null == graphData) {
                return EMPTY.traversal();
            } else if (graphData == GraphData.MODERN) {
                return MODERN.traversal();
            } else {
                throw new UnsupportedOperationException("GraphData not supported: " + graphData.name());
            }
        }

        /**
         * Loads the graph data into the given graph using the standard tinkerpop traversal mechanism.
         *
         * @param graph The graph to load the data into.
         * @param graphData The graph data.
         */
        private static void readIntoGraph(final Graph graph, final GraphData graphData) {
            try {
                final String dataFile = TestHelper.generateTempFileFromResource(graph.getClass(),
                        GryoResourceAccess.class, graphData.location().substring(graphData.location().lastIndexOf(File.separator) + 1), "", false).getAbsolutePath();
                LOGGER.info("Using data file: " + dataFile);
                graph.traversal().io(dataFile).read().iterate();
            } catch (IOException ioe) {
                throw new IllegalStateException(ioe);
            }
        }

        /**
         * Provides the configuration for the test graph based on the input data.
         *
         * @param graphData The graph data
         * @return The configuration for the test.
         */
        private static Map<String, Object> getBaseConfiguration(final GraphData graphData) {

            Map<String, Object> configuration = Stream.of(
                new SimpleEntry<>(GafferPopGraph.GRAPH, GafferPopGraph.class.getName()),
                new SimpleEntry<>(GafferPopGraph.GRAPH_ID, "empty"),
                new SimpleEntry<>(GafferPopGraph.OP_OPTIONS, TEST_OP_OPTIONS),
                new SimpleEntry<>(GafferPopGraph.USER_ID, TEST_USER_ID),
                new SimpleEntry<>(GafferPopGraph.STORE_PROPERTIES, TEST_STORE_PROPS),
                new SimpleEntry<>(GafferPopGraph.NOT_READ_ONLY_ELEMENTS, "true"))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            // If we have test data then load relevant schemas and ID manager
            if (graphData != null) {
                configuration.put(GafferPopGraph.GRAPH_ID, graphData.name());
                configuration.put(GafferPopGraph.ID_MANAGER, GafferPopGraph.DefaultIdManager.INTEGER);
                configuration.put(
                        GafferPopGraph.SCHEMAS,
                        GafferPopFeatureTest.class.getClassLoader().getResource("tinkerpop/schema/modern-int").getPath());
            }

            return configuration;
        }
    }

}
