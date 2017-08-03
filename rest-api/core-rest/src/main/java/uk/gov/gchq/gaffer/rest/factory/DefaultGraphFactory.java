/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.rest.factory;

import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.hook.AddOperationsToChain;
import uk.gov.gchq.gaffer.graph.hook.OperationAuthoriser;
import uk.gov.gchq.gaffer.graph.hook.OperationChainLimiter;
import uk.gov.gchq.gaffer.rest.SystemProperty;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DefaultGraphFactory implements GraphFactory {
    private static Graph graph;

    private static final OperationChainLimiter OPERATION_CHAIN_LIMITER = createStaticChainLimiter();

    /**
     * Set to true by default - so the same instance of {@link Graph} will be
     * returned.
     */
    private boolean singletonGraph = true;

    public DefaultGraphFactory() {
        // Graph factories should be constructed via the createGraphFactory static method,
        // public constructor is required only by HK2
    }

    public static GraphFactory createGraphFactory() {
        final String graphFactoryClass = System.getProperty(SystemProperty.GRAPH_FACTORY_CLASS,
                SystemProperty.GRAPH_FACTORY_CLASS_DEFAULT);

        try {
            return Class.forName(graphFactoryClass)
                    .asSubclass(GraphFactory.class)
                    .newInstance();
        } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException("Unable to create graph factory from class: " + graphFactoryClass, e);
        }
    }


    private static OperationChainLimiter createStaticChainLimiter() {
        if (isChainLimiterEnabled()) {
            if (null == System.getProperty(SystemProperty.OPERATION_SCORES_FILE, null)) {
                throw new IllegalArgumentException("Required property has not been set: " + SystemProperty.OPERATION_SCORES_FILE);
            }

            if (null == System.getProperty(SystemProperty.AUTH_SCORES_FILE, null)) {
                throw new IllegalArgumentException("Required property has not been set: " + SystemProperty.AUTH_SCORES_FILE);
            }

            return new OperationChainLimiter(Paths.get(System.getProperty(SystemProperty.OPERATION_SCORES_FILE)), Paths.get(System.getProperty(SystemProperty.AUTH_SCORES_FILE)));
        }

        return null;
    }

    private static boolean isChainLimiterEnabled() {
        return Boolean.parseBoolean(System.getProperty(SystemProperty.ENABLE_CHAIN_LIMITER, "false"));
    }

    protected static String getGraphId() {
        return System.getProperty(SystemProperty.GRAPH_ID);
    }

    protected static Path[] getSchemaPaths() {
        final String schemaPaths = System.getProperty(SystemProperty.SCHEMA_PATHS);
        if (null == schemaPaths) {
            return new Path[0];
        }

        final String[] schemaPathsArray = schemaPaths.split(",");
        final Path[] paths = new Path[schemaPathsArray.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = Paths.get(schemaPathsArray[i]);
        }

        return paths;
    }

    @Override
    public Graph getGraph() {
        if (singletonGraph) {
            if (null == graph) {
                setGraph(createGraph());
            }
            return graph;
        }

        return createGraph();
    }

    public static void setGraph(final Graph graph) {
        DefaultGraphFactory.graph = graph;
    }

    public boolean isSingletonGraph() {
        return singletonGraph;
    }

    public void setSingletonGraph(final boolean singletonGraph) {
        this.singletonGraph = singletonGraph;
    }

    @Override
    public Graph.Builder createGraphBuilder() {
        final Path storePropertiesPath = Paths.get(System.getProperty(SystemProperty.STORE_PROPERTIES_PATH));
        if (null == storePropertiesPath) {
            throw new SchemaException("The path to the Store Properties was not found in system properties for key: " + SystemProperty.STORE_PROPERTIES_PATH);
        }
        final StoreProperties storeProperties = StoreProperties.loadStoreProperties(storePropertiesPath);

        // Disable any operations required
        storeProperties.addOperationDeclarationPaths("disableOperations.json");

        final Graph.Builder builder = new Graph.Builder();
        builder.storeProperties(storeProperties);
        builder.graphId(getGraphId());

        String graphLibraryClassName = System.getProperty(SystemProperty.GRAPH_LIBRARY_CLASS);
        if (null != graphLibraryClassName) {
            GraphLibrary library;
            try {
                library = Class.forName(graphLibraryClassName).asSubclass(GraphLibrary.class).newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new RuntimeException("Error creating GraphLibrary class: + " + e);
            }
            library.initialise(System.getProperty(SystemProperty.GRAPH_LIBRARY_CONFIG));
            builder.library(library);
        }

        for (final Path path : getSchemaPaths()) {
            builder.addSchema(path);
        }

        final OperationAuthoriser opAuthoriser = createOpAuthoriser();
        if (null != opAuthoriser) {
            builder.addHook(opAuthoriser);
        }

        if (null != OPERATION_CHAIN_LIMITER) {
            builder.addHook(OPERATION_CHAIN_LIMITER);
        }

        final AddOperationsToChain addOperationsToChain = createAddOperationsToChain();
        if (null != addOperationsToChain) {
            builder.addHook(addOperationsToChain);
        }

        return builder;
    }

    @Override
    public OperationAuthoriser createOpAuthoriser() {
        OperationAuthoriser opAuthoriser = null;

        final String opAuthsPathStr = System.getProperty(SystemProperty.OP_AUTHS_PATH);
        if (null != opAuthsPathStr) {
            final Path opAuthsPath = Paths.get(opAuthsPathStr);
            if (opAuthsPath.toFile().exists()) {
                opAuthoriser = new OperationAuthoriser(opAuthsPath);
            } else {
                throw new IllegalArgumentException("Could not find operation authorisation properties from path: " + opAuthsPathStr);
            }
        }

        return opAuthoriser;
    }

    @Override
    public AddOperationsToChain createAddOperationsToChain() {
        AddOperationsToChain addOperationsToChain = null;

        final String path = System.getProperty(SystemProperty.ADD_OPERATIONS_TO_CHAIN_PATH);
        if (null != path) {
            try {
                addOperationsToChain = new AddOperationsToChain(path);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Could not load " + AddOperationsToChain.class.getName() + " graph hook from file: " + path);
            }
        }

        return addOperationsToChain;
    }

    @Override
    public Graph createGraph() {
        return createGraphBuilder().build();
    }
}
