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

package gaffer.graph;


import gaffer.data.elementdefinition.exception.SchemaException;
import gaffer.data.elementdefinition.view.View;
import gaffer.graph.hook.GraphHook;
import gaffer.operation.Operation;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.store.Store;
import gaffer.store.StoreException;
import gaffer.store.StoreProperties;
import gaffer.store.StoreTrait;
import gaffer.store.schema.Schema;
import gaffer.user.User;
import org.apache.commons.io.IOUtils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * The Graph separates the user from the {@link Store}. It holds an instance of the {@link Store} and
 * acts as a proxy for the store, delegating {@link Operation}s to the store.
 * <p>
 * The Graph provides users with a single point of entry for executing operations on a store.
 * This allows the underlying store to be swapped and the same operations can still be applied.
 * <p>
 * Graphs also provides a view of the data with a instance of {@link View}. The view filters out unwanted information
 * and can transform {@link gaffer.data.element.Properties} into transient properties such as averages.
 * <p>
 * When executing operations on a graph, an operation view would override the graph view.
 *
 * @see gaffer.graph.Graph.Builder
 */
public final class Graph {
    /**
     * The instance of the store.
     */
    private final Store store;

    /**
     * The {@link gaffer.data.elementdefinition.view.View} - by default this will just contain all the groups
     * in the graph's {@link Schema}, however it can be set to a subview to
     * allow multiple operations to be performed on the same subview.
     */
    private final View view;

    /**
     * List of {@link GraphHook}s to be triggered before and after operations are
     * executed on the graph.
     */
    private List<GraphHook> graphHooks;

    /**
     * Constructs a <code>Graph</code> with the given {@link gaffer.store.Store} and
     * {@link gaffer.data.elementdefinition.view.View}.
     *
     * @param store      a {@link Store} used to store the elements and handle operations.
     * @param view       a {@link View} defining the view of the data for the graph.
     * @param graphHooks a list of {@link GraphHook}s
     */
    private Graph(final Store store, final View view, final List<GraphHook> graphHooks) {
        this.store = store;
        this.view = view;
        this.graphHooks = graphHooks;
    }

    /**
     * Performs the given operation on the store.
     * If the operation does not have a view then the graph view is used.
     *
     * @param operation the operation to be executed.
     * @param user      the user executing the operation.
     * @param <OUTPUT>  the operation output type.
     * @return the operation result.
     * @throws OperationException if an operation fails
     */
    public <OUTPUT> OUTPUT execute(final Operation<?, OUTPUT> operation, final User user) throws OperationException {
        return execute(new OperationChain<>(operation), user);
    }

    /**
     * Performs the given operation on the store.
     * If the operation does not have a view then the graph view is used.
     *
     * @param operationChain the operation chain to be executed.
     * @param user           the user executing the operation chain.
     * @param <OUTPUT>       the operation chain output type.
     * @return the operation result.
     * @throws OperationException if an operation fails
     */
    public <OUTPUT> OUTPUT execute(final OperationChain<OUTPUT> operationChain, final User user) throws OperationException {
        for (Operation operation : operationChain.getOperations()) {
            if (null == operation.getView()) {
                operation.setView(view);
            }
        }

        for (GraphHook graphHook : graphHooks) {
            graphHook.preExecute(operationChain, user);
        }

        OUTPUT result = store.execute(operationChain, user);

        for (GraphHook graphHook : graphHooks) {
            graphHook.postExecute(result, operationChain, user);
        }

        return result;
    }

    /**
     * @param operationClass the operation class to check
     * @return true if the provided operation is supported.
     */
    public boolean isSupported(final Class<? extends Operation> operationClass) {
        return store.isSupported(operationClass);
    }

    /**
     * @return a collection of all the supported {@link Operation}s.
     */
    public Set<Class<? extends Operation>> getSupportedOperations() {
        return store.getSupportedOperations();
    }

    /**
     * Returns the graph view.
     *
     * @return the graph view.
     */
    public View getView() {
        return view;
    }

    /**
     * @return the schema.
     */
    public Schema getSchema() {
        return store.getSchema();
    }

    /**
     * @param storeTrait the store trait to check
     * @return true if the store has the given trait.
     */
    public boolean hasTrait(final StoreTrait storeTrait) {
        return store.hasTrait(storeTrait);
    }

    /**
     * Returns all the {@link StoreTrait}s for the contained {@link Store} implementation
     *
     * @return a {@link Set} of all of the {@link StoreTrait}s that the store has.
     */
    public Set<StoreTrait> getStoreTraits() {
        return store.getTraits();
    }

    /**
     * Builder for {@link Graph}.
     */
    public static class Builder {
        private final List<byte[]> schemaBytesList = new ArrayList<>();
        private Store store;
        private StoreProperties properties;
        private Schema schema;
        private View view;
        private List<GraphHook> graphHooks = new ArrayList<>();

        public Builder view(final View view) {
            this.view = view;
            return this;
        }

        public Builder view(final Path view) {
            return view(View.fromJson(view));
        }

        public Builder view(final InputStream view) {
            return view(View.fromJson(view));
        }

        public Builder view(final byte[] jsonBytes) {
            return view(View.fromJson(jsonBytes));
        }

        public Builder storeProperties(final StoreProperties properties) {
            this.properties = properties;
            return this;
        }

        public Builder storeProperties(final Path propertiesPath) {
            return storeProperties(StoreProperties.loadStoreProperties(propertiesPath));
        }

        public Builder storeProperties(final InputStream propertiesStream) {
            return storeProperties(StoreProperties.loadStoreProperties(propertiesStream));
        }

        public Builder addSchemas(final Schema... schemaModules) {
            if (null != schemaModules) {
                for (Schema schemaModule : schemaModules) {
                    addSchema(schemaModule);
                }
            }

            return this;
        }

        public Builder addSchemas(final InputStream... schemaStreams) {
            if (null != schemaStreams) {
                try {
                    for (InputStream schemaStream : schemaStreams) {
                        addSchema(schemaStream);
                    }
                } finally {
                    for (InputStream schemaModule : schemaStreams) {
                        IOUtils.closeQuietly(schemaModule);
                    }
                }
            }

            return this;
        }

        public Builder addSchemas(final Path... schemaPaths) {
            if (null != schemaPaths) {
                for (Path schemaPath : schemaPaths) {
                    addSchema(schemaPath);
                }
            }

            return this;
        }

        public Builder addSchemas(final byte[]... schemaBytesArray) {
            if (null != schemaBytesArray) {
                for (byte[] schemaBytes : schemaBytesArray) {
                    addSchema(schemaBytes);
                }
            }

            return this;
        }

        public Builder addSchema(final Schema schemaModule) {
            if (null != schema) {
                schema.merge(schemaModule);
            } else {
                this.schema = schemaModule;
            }

            return this;
        }

        public Builder addSchema(final InputStream schemaStream) {
            try {
                return addSchema(sun.misc.IOUtils.readFully(schemaStream, schemaStream.available(), true));
            } catch (IOException e) {
                throw new SchemaException("Unable to read schema from input stream", e);
            } finally {
                IOUtils.closeQuietly(schemaStream);
            }
        }

        public Builder addSchema(final Path schemaPath) {
            try {
                if (Files.isDirectory(schemaPath)) {
                    for (Path path : Files.newDirectoryStream(schemaPath)) {
                        addSchema(path);
                    }
                } else {
                    addSchema(Files.readAllBytes(schemaPath));
                }
            } catch (IOException e) {
                throw new SchemaException("Unable to read schema from path", e);
            }

            return this;
        }

        public Builder addSchema(final byte[] schemaBytes) {
            schemaBytesList.add(schemaBytes);
            return this;
        }

        public Builder store(final Store store) {
            this.store = store;
            return this;
        }

        public Builder addHook(final GraphHook graphHook) {
            this.graphHooks.add(graphHook);
            return this;
        }

        public Graph build() {
            updateSchema();
            updateStore();
            updateView();

            return new Graph(store, view, graphHooks);
        }

        private void updateSchema() {
            if (!schemaBytesList.isEmpty()) {
                if (null == properties) {
                    throw new IllegalArgumentException("To load a schema from json, the store properties must be provided.");
                }

                final Class<? extends Schema> schemaClass = properties.getSchemaClass();
                final Schema newSchema = Schema.fromJson(schemaClass, schemaBytesList.toArray(new byte[schemaBytesList.size()][]));
                if (null != schema) {
                    schema.merge(newSchema);
                } else {
                    schema = newSchema;
                }
            }
        }

        private void updateStore() {
            if (null == store) {
                store = createStore(properties, schema);
            } else if (null != properties || null != schema) {
                if (null == properties || null == schema) {
                    throw new IllegalArgumentException("To initialise a provided store both a schema and store properties are required");
                }
                try {
                    store.initialise(schema, properties);
                } catch (StoreException e) {
                    throw new IllegalArgumentException("Unable to initialise the store with the given schema and properties");
                }
            } else {
                store.optimiseSchemas();
                store.validateSchemas();
            }
        }

        private Store createStore(final StoreProperties storeProperties, final Schema schema) {
            if (null == storeProperties || null == schema) {
                throw new IllegalArgumentException("Store properties and schema are required to create a store");
            }

            final String storeClass = storeProperties.getStoreClass();
            if (null == storeClass) {
                throw new IllegalArgumentException("The Store class name was not found in the store properties for key: " + StoreProperties.STORE_PROPERTIES_CLASS);
            }

            final Store newStore;
            try {
                newStore = Class.forName(storeClass).asSubclass(Store.class).newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new IllegalArgumentException("Could not create store of type: " + storeClass, e);
            }

            try {
                newStore.initialise(schema, storeProperties);
            } catch (StoreException e) {
                throw new IllegalArgumentException("Could not initialise the store with provided arguments.", e);
            }
            return newStore;
        }

        private void updateView() {
            if (null == view) {
                this.view = new View.Builder()
                        .entities(store.getSchema().getEntityGroups())
                        .edges(store.getSchema().getEdgeGroups())
                        .build();
            }
        }
    }
}
