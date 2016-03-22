/*
 * Copyright 2016 Crown Copyright
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

package gaffer.rest;

import gaffer.data.elementdefinition.exception.SchemaException;
import gaffer.graph.Graph;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * A <code>GraphFactory</code> creates instances of {@link gaffer.graph.Graph} to be reused for all queries.
 */
public class GraphFactory {
    private static Graph graph;

    private final boolean singletonGraph;

    public GraphFactory(final boolean singletonGraph) {
        this.singletonGraph = singletonGraph;
    }

    public Graph getGraph() {
        if (singletonGraph) {
            if (null == graph) {
                setGraph(createGraph());
            }
            return graph;
        }

        return createGraph();
    }

    private static Graph createGraph() {
        final Path storePropertiesPath = Paths.get(System.getProperty(SystemProperty.STORE_PROPERTIES_PATH));
        if (null == storePropertiesPath) {
            throw new SchemaException("The path to the Store Properties was not found in system properties for key: " + SystemProperty.STORE_PROPERTIES_PATH);
        }

        final Graph.Builder builder = new Graph.Builder();
        builder.storeProperties(storePropertiesPath);
        for (Path path : getSchemaPaths()) {
            builder.addSchema(path);
        }

        return builder.build();
    }

    private static void setGraph(final Graph graph) {
        GraphFactory.graph = graph;
    }

    private static Path[] getSchemaPaths() {
        final String schemaPaths = System.getProperty(SystemProperty.SCHEMA_PATHS);
        if (null == schemaPaths) {
            throw new SchemaException("The path to the schema was not found in system properties for key: " + SystemProperty.SCHEMA_PATHS);
        }

        final String[] schemaPathsArray = schemaPaths.split(",");
        final Path[] paths = new Path[schemaPathsArray.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = Paths.get(schemaPathsArray[i]);
        }

        return paths;
    }
}
