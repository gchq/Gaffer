/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.graph;


import org.apache.commons.io.IOUtils;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.hook.GraphHook;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
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
 * and can transform {@link uk.gov.gchq.gaffer.data.element.Properties} into transient properties such as averages.
 * <p>
 * When executing operations on a graph, an operation view would override the graph view.
 *
 * @see uk.gov.gchq.gaffer.graph.Graph.Builder
 */
public final class Graph {
    /**
     * The instance of the store.
     */
    private final Store store;

    /**
     * The {@link uk.gov.gchq.gaffer.data.elementdefinition.view.View} - by default this will just contain all the groups
     * in the graph's {@link Schema}, however it can be set to a subview to
     * allow multiple operations to be performed on the same subview.
     */
    private final View view;

    /**
     * List of {@link GraphHook}s to be triggered before and after operations are
     * executed on the graph.
     */
    private List<GraphHook> graphHooks;

    private Schema schema;

    /**
     * Constructs a <code>Graph</code> with the given {@link uk.gov.gchq.gaffer.store.Store} and
     * {@link uk.gov.gchq.gaffer.data.elementdefinition.view.View}.
     *
     * @param store      a {@link Store} used to store the elements and handle operations.
     * @param schema     a {@link Schema} that defines the graph. Should be the copy of the schema that the store is initialised with.
     * @param view       a {@link View} defining the view of the data for the graph.
     * @param graphHooks a list of {@link GraphHook}s
     */
    private Graph(final Store store, final Schema schema, final View view, final List<GraphHook> graphHooks) {
        this.store = store;
        this.view = view;
        this.graphHooks = graphHooks;
        this.schema = schema;
    }

    /**
     * Performs the given operation on the store.
     * If the operation does not have a view then the graph view is used.
     * NOTE the operation may be modified/optimised by the store.
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
     * Performs the given operation chain job on the store.
     * If the operation does not have a view then the graph view is used.
     * NOTE the operationChain may be modified/optimised by the store.
     *
     * @param operationChain the operation chain to be executed.
     * @param user           the user executing the job.
     * @return the job details
     * @throws OperationException thrown if the job fails to run.
     */
    public JobDetail executeJob(final OperationChain<?> operationChain, final User user) throws OperationException {
        updateOperationChainView(operationChain);

        for (final GraphHook graphHook : graphHooks) {
            graphHook.preExecute(operationChain, user);
        }

        JobDetail result = store.executeJob(operationChain, user);

        for (final GraphHook graphHook : graphHooks) {
            result = graphHook.postExecute(result, operationChain, user);
        }

        return result;
    }

    /**
     * Performs the given operation chain on the store.
     * If the operation does not have a view then the graph view is used.
     * NOTE the operationChain may be modified/optimised by the store.
     *
     * @param operationChain the operation chain to be executed.
     * @param user           the user executing the operation chain.
     * @param <OUTPUT>       the operation chain output type.
     * @return the operation result.
     * @throws OperationException if an operation fails
     */
    public <OUTPUT> OUTPUT execute(final OperationChain<OUTPUT> operationChain, final User user) throws OperationException {
        updateOperationChainView(operationChain);

        for (final GraphHook graphHook : graphHooks) {
            graphHook.preExecute(operationChain, user);
        }

        OUTPUT result = store.execute(operationChain, user);

        for (final GraphHook graphHook : graphHooks) {
            result = graphHook.postExecute(result, operationChain, user);
        }

        return result;
    }

    private <OUTPUT> void updateOperationChainView(final OperationChain<OUTPUT> operationChain) {
        for (final Operation operation : operationChain.getOperations()) {
            final View opView;
            if (null == operation.getView()) {
                opView = view;
            } else if (!operation.getView().hasGroups()) {
                opView = new View.Builder()
                        .merge(view)
                        .merge(operation.getView())
                        .build();

            } else {
                opView = operation.getView();
            }

            opView.expandGlobalDefinitions();
            operation.setView(opView);
        }
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
        return schema;
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
            return view(new View.Builder().json(view).build());
        }

        public Builder view(final InputStream view) {
            return view(new View.Builder().json(view).build());
        }

        public Builder view(final byte[] jsonBytes) {
            return view(new View.Builder().json(jsonBytes).build());
        }

        public Builder storeProperties(final StoreProperties properties) {
            this.properties = properties;
            return this;
        }

        public Builder storeProperties(final String propertiesPath) {
            return storeProperties(StoreProperties.loadStoreProperties(propertiesPath));
        }

        public Builder storeProperties(final Path propertiesPath) {
            return storeProperties(StoreProperties.loadStoreProperties(propertiesPath));
        }

        public Builder storeProperties(final InputStream propertiesStream) {
            return storeProperties(StoreProperties.loadStoreProperties(propertiesStream));
        }

        public Builder addSchemas(final Schema... schemaModules) {
            if (null != schemaModules) {
                for (final Schema schemaModule : schemaModules) {
                    addSchema(schemaModule);
                }
            }

            return this;
        }

        public Builder addSchemas(final InputStream... schemaStreams) {
            if (null != schemaStreams) {
                try {
                    for (final InputStream schemaStream : schemaStreams) {
                        addSchema(schemaStream);
                    }
                } finally {
                    for (final InputStream schemaModule : schemaStreams) {
                        IOUtils.closeQuietly(schemaModule);
                    }
                }
            }

            return this;
        }

        public Builder addSchemas(final Path... schemaPaths) {
            if (null != schemaPaths) {
                for (final Path schemaPath : schemaPaths) {
                    addSchema(schemaPath);
                }
            }

            return this;
        }

        public Builder addSchemas(final byte[]... schemaBytesArray) {
            if (null != schemaBytesArray) {
                for (final byte[] schemaBytes : schemaBytesArray) {
                    addSchema(schemaBytes);
                }
            }

            return this;
        }

        public Builder addSchema(final Schema schemaModule) {
            if (null != schema) {
                schema = new Schema.Builder()
                        .merge(schema)
                        .merge(schemaModule)
                        .build();
            } else {
                schema = schemaModule;
            }

            return this;
        }

        public Builder addSchema(final InputStream schemaStream) {
            try {
                return addSchema(sun.misc.IOUtils.readFully(schemaStream, schemaStream.available(), true));
            } catch (final IOException e) {
                throw new SchemaException("Unable to read schema from input stream", e);
            } finally {
                IOUtils.closeQuietly(schemaStream);
            }
        }

        public Builder addSchema(final Path schemaPath) {
            try {
                if (Files.isDirectory(schemaPath)) {
                    for (final Path path : Files.newDirectoryStream(schemaPath)) {
                        addSchema(path);
                    }
                } else {
                    addSchema(Files.readAllBytes(schemaPath));
                }
            } catch (final IOException e) {
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

            return new Graph(store, schema, view, graphHooks);
        }

        private void updateSchema() {
            if (!schemaBytesList.isEmpty()) {
                if (null == properties) {
                    throw new IllegalArgumentException("To load a schema from json, the store properties must be provided.");
                }

                final Class<? extends Schema> schemaClass = properties.getSchemaClass();
                final Schema newSchema = new Schema.Builder()
                        .json(schemaClass, schemaBytesList.toArray(new byte[schemaBytesList.size()][]))
                        .build();
                addSchema(newSchema);
            }
        }

        private void updateStore() {
            if (null == store) {
                store = createStore(properties, cloneSchema(schema));
            } else if (null != properties || null != schema) {
                try {
                    if (null == properties) {
                        store.initialise(cloneSchema(schema), store.getProperties());
                    } else if (null == schema) {
                        store.initialise(store.getSchema(), properties);
                    } else {
                        store.initialise(cloneSchema(schema), properties);
                    }
                } catch (StoreException e) {
                    throw new IllegalArgumentException("Unable to initialise the store with the given schema and properties", e);
                }
            } else {
                schema = store.getSchema();
                store.optimiseSchema();
                store.validateSchemas();
            }

            if (null == schema) {
                schema = store.getSchema();
            }
        }

        private Store createStore(final StoreProperties storeProperties, final Schema schema) {
            if (null == storeProperties) {
                throw new IllegalArgumentException("Store properties are required to create a store");
            }

            final String storeClass = storeProperties.getStoreClass();
            if (null == storeClass) {
                throw new IllegalArgumentException("The Store class name was not found in the store properties for key: " + StoreProperties.STORE_CLASS);
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
                        .entities(schema.getEntityGroups())
                        .edges(schema.getEdgeGroups())
                        .build();
            }
        }

        private Schema cloneSchema(final Schema schema) {
            return null != schema ? schema.clone() : null;
        }
    }
}
