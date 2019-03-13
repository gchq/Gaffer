/*
 * Copyright 2017-2019 Crown Copyright
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
import uk.gov.gchq.gaffer.graph.library.GraphLibrary;
import uk.gov.gchq.gaffer.graph.util.GraphConfig;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.rest.SystemProperty;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Default implementation of the {@link StoreFactory} interface, used by HK2 to
 * instantiate default {@link Graph} instances.
 */
public class DefaultStoreFactory implements StoreFactory {
    private static Store store;

    /**
     * Set to true by default - so the same instance of {@link Graph} will be
     * returned.
     */
    private boolean singletonStore = true;

    public DefaultStoreFactory() {
        // Store factories should be constructed via the createStoreFactory
        // static method,
        // public constructor is required only by HK2
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
    public Store getStore() {
        if (singletonStore) {
            if (null == store) {
                setStore(createStore());
            }
            return store;
        }
        return createStore();
    }

    public static void setStore(final Store store) {
        DefaultStoreFactory.store = store;
    }

    public boolean isSingletonStore() {
        return singletonStore;
    }

    public void setSingletonStore(final boolean singletonStore) {
        this.singletonStore = singletonStore;
    }

    @Override
    public Store createStore() {
        final String storePropertiesPath = System.getProperty(SystemProperty.STORE_PROPERTIES_PATH);
        if (null == storePropertiesPath) {
            throw new SchemaException("The path to the Store Properties was not found in system properties for key: " + SystemProperty.STORE_PROPERTIES_PATH);
        }
        final StoreProperties storeProperties = StoreProperties.loadStoreProperties(storePropertiesPath);

        // Disable any operations required
        storeProperties.addOperationDeclarationPaths("disableOperations.json");

        final GraphConfig.Builder builder = new GraphConfig.Builder();

        builder.storeProperties(storeProperties);

        final String graphConfigPath = System.getProperty(SystemProperty.GRAPH_CONFIG_PATH);
        if (null != graphConfigPath) {
            Paths.get(graphConfigPath);
            try {
                builder.merge(JSONSerialiser.deserialise(null != Paths.get(graphConfigPath) ?
                        Files.readAllBytes(Paths.get(graphConfigPath)) : null, GraphConfig.class));
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to read graph config from path: " + Paths.get(graphConfigPath), e);
            }
        }

        for (final Path path : getSchemaPaths()) {
            builder.addSchema(path);
        }

        final String graphId = System.getProperty(SystemProperty.GRAPH_ID);
        if (null != graphId) {
            builder.graphId(graphId);
        }

        String graphLibraryClassName = System.getProperty(SystemProperty.GRAPH_LIBRARY_CLASS);
        if (null != graphLibraryClassName) {
            GraphLibrary library;
            try {
                library = Class.forName(graphLibraryClassName).asSubclass(GraphLibrary.class).newInstance();
            } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new RuntimeException("Error creating GraphLibrary class: + " + e);
            }
            library.initialise(System.getProperty(SystemProperty.GRAPH_LIBRARY_CONFIG));
            builder.library(library);
        }

        final String graphHooksPath = System.getProperty(SystemProperty.GRAPH_HOOKS_PATH);
        if (null != graphHooksPath) {
            builder.addHooks(Paths.get(graphHooksPath));
        }

        return Store.createStore(builder.build());
    }
}
