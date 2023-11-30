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

    public static final String TEST_USER_ID = "tinkerpopTestUser";
    public static final String[] TEST_OP_OPTIONS = new String[] {"key1:value1", "key2:value2"};
    public static final String TEST_STORE_PROPS = GafferPopGraphProvider.class.getClassLoader().getResource("gaffer/map-store.properties").getPath();
    public static final String TEST_TYPES_SCHEMA = GafferPopGraphProvider.class.getClassLoader().getResource("tinkerpop/schema/types").getPath();

    private static final Set<Class> IMPLEMENTATION = Stream.of(
        GafferPopEdge.class,
        GafferPopElement.class,
        GafferPopGraph.class,
        GafferPopGraphVariables.class,
        GafferPopProperty.class,
        GafferPopVertex.class,
        GafferPopVertexProperty.class)
        .collect(Collectors.toCollection(HashSet::new));

    private static final Set<String> TESTS_THAT_NEED_UUID_IDS = Stream.of(
        "shouldIterateVerticesWithUuidIdSupportUsingStringRepresentation",
        "shouldIterateVerticesWithUuidIdSupportUsingStringRepresentations",
        "shouldIterateEdgesWithUuidIdSupportUsingStringRepresentation",
        "shouldIterateEdgesWithUuidIdSupportUsingStringRepresentations",
        "shouldIterateEdgesWithUuidIdSupportUsingEdgeIds",
        "shouldIterateVerticesWithUuidIdSupportUsingVertexIds",
        "shouldIterateVerticesWithUuidIdSupportUsingVertex",
        "shouldIterateVerticesWithUuidIdSupportUsingStarVertex",
        "shouldAddVertexWithUserSuppliedUuidId",
        "shouldIterateEdgesWithUuidIdSupportUsingEdges",
        "shouldIterateVerticesWithUuidIdSupportUsingReferenceVertex",
        "shouldIterateVerticesWithUuidIdSupportUsingDetachedVertex",
        "shouldIterateVerticesWithUuidIdSupportUsingVertexId",
        "shouldIterateVerticesWithUuidIdSupportUsingVertices",
        "shouldAddVertexWithUserSuppliedAnyIdUsingUuid")
        .collect(Collectors.toCollection(HashSet::new));

    // TODO: Review this list based on tests
    private static final Set<String> TESTS_THAT_NEED_STRING_IDS = Stream.of(
        "shouldIterateEdgesWithCustomIdSupportUsingStringRepresentations",
        "shouldIterateVerticesWithStringIdSupportUsingStringRepresentations",
        "shouldAddVertexWithUserSuppliedAnyIdUsingString",
        "shouldIterateVerticesWithStringSupportUsingStarVertex",
        "shouldIterateVerticesWithStringSupportUsingReferenceVertex",
        "shouldIterateVerticesWithStringSupportUsingDetachedVertex",
        "shouldAddVertexWithUserSuppliedStringId",
        "shouldIterateEdgesWithStringIdSupportUsingStringRepresentation",
        "shouldHaveExceptionConsistencyWhenAssigningSameIdOnVertex",
        "shouldIterateVerticesWithStringIdSupportUsingVertexId",
        "shouldIterateVerticesWithStringIdSupportUsingVertices",
        "shouldIterateVerticesWithStringIdSupportUsingVertexIds",
        "shouldEvaluateConnectivityPatterns",
        "shouldIterateEdgesWithStringIdSupportUsingEdgeIds",
        "shouldIterateEdgesWithStringIdSupportUsingStringRepresentations",
        "shouldIterateVerticesWithStringIdSupportUsingStringRepresentation",
        "shouldIterateVerticesWithStringIdSupportUsingVertex",
        "shouldProperlySerializeCustomIdWithGraphSON",
        "shouldReadGraphML")
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

        Map<String, Object> configuration = Stream.of(
            new SimpleEntry<>(GafferPopGraph.GRAPH, GafferPopGraph.class.getName()),
            new SimpleEntry<>(GafferPopGraph.GRAPH_ID, graphName),
            new SimpleEntry<>(GafferPopGraph.OP_OPTIONS, TEST_OP_OPTIONS),
            new SimpleEntry<>(GafferPopGraph.USER_ID, TEST_USER_ID),
            new SimpleEntry<>(GafferPopGraph.STORE_PROPERTIES, TEST_STORE_PROPS),
            new SimpleEntry<>(GafferPopGraph.TYPES_SCHEMA, TEST_TYPES_SCHEMA))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // Some tests require different type of ID used by default.
        if (TESTS_THAT_NEED_UUID_IDS.contains(testMethodName)) {
            configuration.put(GafferPopGraph.ID_MANAGER, GafferPopGraph.DefaultIdManager.UUID);
            configuration.put(
                GafferPopGraph.ELEMENTS_SCHEMA,
                GafferPopGraphProvider.class.getClassLoader().getResource("tinkerpop/schema/elements-uuid-id").getPath());
        } else if (TESTS_THAT_NEED_STRING_IDS.contains(testMethodName)) {
            configuration.put(GafferPopGraph.ID_MANAGER, GafferPopGraph.DefaultIdManager.STRING);
            configuration.put(
                GafferPopGraph.ELEMENTS_SCHEMA,
                GafferPopGraphProvider.class.getClassLoader().getResource("tinkerpop/schema/elements-string-id").getPath());
        } else {
            configuration.put(GafferPopGraph.ID_MANAGER, GafferPopGraph.DefaultIdManager.LONG);
            configuration.put(
                GafferPopGraph.ELEMENTS_SCHEMA,
                GafferPopGraphProvider.class.getClassLoader().getResource("tinkerpop/schema/elements-long-id").getPath());
        }
        // The types of test data can also affect the ID manager we need to use
        if (loadGraphWith != null) {
            if ((loadGraphWith.equals(LoadGraphWith.GraphData.CLASSIC))
                    || (loadGraphWith.equals(LoadGraphWith.GraphData.MODERN))
                    || (loadGraphWith.equals(LoadGraphWith.GraphData.CREW))
                    || (loadGraphWith.equals(LoadGraphWith.GraphData.GRATEFUL))
                    || (loadGraphWith.equals(LoadGraphWith.GraphData.SINK))) {
                configuration.put(GafferPopGraph.ID_MANAGER, GafferPopGraph.DefaultIdManager.INTEGER);
                configuration.put(
                    GafferPopGraph.ELEMENTS_SCHEMA,
                    GafferPopGraphProvider.class.getClassLoader().getResource("tinkerpop/schema/elements-int-id").getPath());
            }
        }
        return configuration;
    }

}
