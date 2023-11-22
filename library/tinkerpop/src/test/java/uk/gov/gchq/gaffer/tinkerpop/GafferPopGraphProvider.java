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

package uk.gov.gchq.gaffer.tinkerpop;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Provides the GafferPop graph implementation of the Tinkerpop interface to use for running the
 * standard Tinkerpop test suites.
 */
public class GafferPopGraphProvider extends AbstractGraphProvider {

    public static final String TEST_GRAPH_ID = "tinkerpopTestGraph";
    public static final String TEST_USER_ID = "tinkerpopTestUser";
    public static final String[] TEST_OP_OPTIONS = new String[] {"key1:value1", "key2:value2"};
    public static final String TEST_STORE_PROPS = GafferPopGraphProvider.class.getClassLoader().getResource("gaffer/map-store.properties").getPath();
    public static final String TEST_SCHEMAS = GafferPopGraphProvider.class.getClassLoader().getResource("gaffer/schema").getPath();

    private static final Set<Class> IMPLEMENTATION = Stream.of(
        GafferPopEdge.class,
        GafferPopElement.class,
        GafferPopGraph.class,
        GafferPopGraphVariables.class,
        GafferPopProperty.class,
        GafferPopVertex.class,
        GafferPopVertexProperty.class)
        .collect(Collectors.toCollection(HashSet::new));

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        if (graph != null) {
            graph.close();
        }
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName,
            GraphData loadGraphWith) {
        // TODO: Look at modifying the configuration based on the GraphData that will be loaded e.g. schema

        return Stream.of(
            new SimpleEntry<>(GafferPopGraph.GRAPH, GafferPopGraph.class.getName()),
            new SimpleEntry<>(GafferPopGraph.GRAPH_ID, TEST_GRAPH_ID),
            new SimpleEntry<>(GafferPopGraph.OP_OPTIONS, TEST_OP_OPTIONS),
            new SimpleEntry<>(GafferPopGraph.USER_ID, TEST_USER_ID),
            new SimpleEntry<>(GafferPopGraph.STORE_PROPERTIES, TEST_STORE_PROPS),
            new SimpleEntry<>(GafferPopGraph.SCHEMAS, TEST_SCHEMAS))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public void loadGraphData(final Graph graph, final LoadGraphWith loadGraphWith, final Class testClass, final String testName) {
        // TODO: Add gaffer specific way of loading the data
    }
}
