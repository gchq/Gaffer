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


import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.hook.GraphHook;
import uk.gov.gchq.gaffer.graph.hook.NamedOperationResolver;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.library.NoGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 * The Graph separates the user from the {@link Store}. It holds an instance of the {@link Store} and
 * acts as a proxy for the store, delegating {@link Operation}s to the store.
 * </p>
 * <p>
 * The Graph provides users with a single point of entry for executing operations on a store.
 * This allows the underlying store to be swapped and the same operations can still be applied.
 * </p>
 * <p>
 * Graphs also provides a view of the data with a instance of {@link View}. The view filters out unwanted information
 * and can transform {@link uk.gov.gchq.gaffer.data.element.Properties} into transient properties such as averages.
 * </p>
 * <p>
 * When executing operations on a graph, an operation view would override the graph view.
 * </p>
 *
 * @see uk.gov.gchq.gaffer.graph.Graph.Builder
 */
public final class Graph {
    private static final JSONSerialiser JSON_SERIALISER = new JSONSerialiser();
    private static final Logger LOGGER = LoggerFactory.getLogger(Graph.class);

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
     * @param schema     a {@link Schema} that defines the graph. Should be the copy of the schema that the store is initialised with.
     * @param store      a {@link Store} used to store the elements and handle operations.
     * @param view       a {@link View} defining the view of the data for the graph.
     * @param graphHooks a list of {@link GraphHook}s
     */
    private Graph(final Schema schema, final Store store, final View view, final List<GraphHook> graphHooks) {
        this.schema = schema;
        this.store = store;
        this.view = view;
        this.graphHooks = graphHooks;
    }

    /**
     * Performs the given operation on the store.
     * If the operation does not have a view then the graph view is used.
     * NOTE the operation may be modified/optimised by the store.
     *
     * @param operation the operation to be executed.
     * @param user      the user executing the operation.
     * @throws OperationException if an operation fails
     */
    public void execute(final Operation operation, final User user) throws OperationException {
        execute(new OperationChain<>(operation), user);
    }

    /**
     * Performs the given output operation on the store.
     * If the operation does not have a view then the graph view is used.
     * NOTE the operation may be modified/optimised by the store.
     *
     * @param operation the output operation to be executed.
     * @param user      the user executing the operation.
     * @param <O>       the operation chain output type.
     * @return the operation result.
     * @throws OperationException if an operation fails
     */
    public <O> O execute(final Output<O> operation, final User user) throws OperationException {
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
        try {
            for (final GraphHook graphHook : graphHooks) {
                graphHook.preExecute(operationChain, user);
            }

            updateOperationChainView(operationChain);

            JobDetail result = store.executeJob(operationChain, user);

            for (final GraphHook graphHook : graphHooks) {
                result = graphHook.postExecute(result, operationChain, user);
            }

            return result;

        } catch (final Exception e) {
            CloseableUtil.close(operationChain);
            throw e;
        }
    }

    /**
     * Performs the given operation chain on the store.
     * If the operation does not have a view then the graph view is used.
     * NOTE the operationChain may be modified/optimised by the store.
     *
     * @param operationChain the operation chain to be executed.
     * @param user           the user executing the operation chain.
     * @param <O>            the operation chain output type.
     * @return the operation result.
     * @throws OperationException if an operation fails
     */
    public <O> O execute(final OperationChain<O> operationChain, final User user) throws OperationException {
        O result = null;
        try {
            for (final GraphHook graphHook : graphHooks) {
                graphHook.preExecute(operationChain, user);
            }

            updateOperationChainView(operationChain);

            result = store.execute(operationChain, user);

            for (final GraphHook graphHook : graphHooks) {
                result = graphHook.postExecute(result, operationChain, user);
            }
        } catch (final Exception e) {
            CloseableUtil.close(operationChain);
            CloseableUtil.close(result);

            throw e;
        }

        return result;
    }

    private <O> void updateOperationChainView(final OperationChain<O> operationChain) {
        for (final Operation operation : operationChain.getOperations()) {

            if (operation instanceof OperationView) {
                final OperationView operationView = (OperationView) operation;
                final View opView;
                if (null == operationView.getView()) {
                    opView = view;
                } else if (!operationView.getView().hasGroups()) {
                    opView = new View.Builder()
                            .merge(view)
                            .merge(operationView.getView())
                            .build();
                } else {
                    opView = operationView.getView();
                }

                opView.expandGlobalDefinitions();
                operationView.setView(opView);
            }
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
     * @param operation the class of the operation to check
     * @return a collection of all the compatible {@link Operation}s that could
     * be added to an operation chain after the provided operation.
     */
    public Set<Class<? extends Operation>> getNextOperations(final Class<? extends Operation> operation) {
        return store.getNextOperations(operation);
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
     * @return the graphId for this Graph.
     */
    public String getGraphId() {
        return store.getGraphId();
    }

    /**
     * @return the StoreProperties for this Graph.
     */
    public StoreProperties getStoreProperties() {
        return store.getProperties();
    }

    public List<Class<? extends GraphHook>> getGraphHooks() {
        if (graphHooks.isEmpty()) {
            return Collections.emptyList();
        }

        return (List) graphHooks.stream().map(GraphHook::getClass).collect(Collectors.toList());
    }

    /**
     * Builder for {@link Graph}.
     */
    public static class Builder {
        public static final String UNABLE_TO_READ_SCHEMA_FROM_URI = "Unable to read schema from URI";
        private final List<byte[]> schemaBytesList = new ArrayList<>();
        private Store store;
        private String graphId;
        private GraphLibrary library;
        private StoreProperties properties;
        private Schema schema;
        private View view;
        private List<GraphHook> graphHooks = new ArrayList<>();
        private String[] parentSchemaIds;
        private String parentStorePropertiesId;

        public Builder graphId(final String graphId) {
            this.graphId = graphId;
            return this;
        }

        public String getGraphId() {
            return graphId;
        }

        public Builder library(final GraphLibrary library) {
            this.library = library;
            return this;
        }

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

        public Builder view(final URI view) {
            try {
                view(StreamUtil.openStream(view));
            } catch (final IOException e) {
                throw new SchemaException("Unable to read view from URI", e);
            }
            return this;
        }

        public Builder view(final byte[] jsonBytes) {
            return view(new View.Builder().json(jsonBytes).build());
        }

        public Builder parentStorePropertiesId(final String parentStorePropertiesId) {
            this.parentStorePropertiesId = parentStorePropertiesId;
            return this;
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

        public Builder storeProperties(final URI propertiesURI) {
            try {
                storeProperties(StreamUtil.openStream(propertiesURI));
            } catch (final IOException e) {
                throw new SchemaException("Unable to read storeProperties from URI", e);
            }

            return this;
        }

        public Builder addParentSchemaIds(final String... parentSchemaIds) {
            this.parentSchemaIds = parentSchemaIds;
            return this;
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
                        CloseableUtil.close(schemaModule);
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
                CloseableUtil.close(schemaStream);
            }
        }

        public Builder addSchema(final URI schemaURI) {
            try {
                addSchema(StreamUtil.openStream(schemaURI));
            } catch (final IOException e) {
                throw new SchemaException(UNABLE_TO_READ_SCHEMA_FROM_URI, e);
            }

            return this;
        }

        public Builder addSchemas(final URI... schemaURI) {
            try {
                addSchemas(StreamUtil.openStreams(schemaURI));
            } catch (final IOException e) {
                throw new SchemaException(UNABLE_TO_READ_SCHEMA_FROM_URI, e);
            }

            return this;
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

        public Builder addHooks(final Path hooksPath) {
            if (null == hooksPath || !hooksPath.toFile().exists()) {
                throw new IllegalArgumentException("Unable to find graph hooks file: " + hooksPath);
            }
            final GraphHook[] hooks;
            try {
                hooks = JSON_SERIALISER.deserialise(FileUtils.readFileToByteArray(hooksPath.toFile()), GraphHook[].class);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to load graph hooks file: " + hooksPath, e);
            }
            return addHooks(hooks);
        }

        public Builder addHook(final Path hookPath) {
            if (null == hookPath || !hookPath.toFile().exists()) {
                throw new IllegalArgumentException("Unable to find graph hook file: " + hookPath);
            }

            final GraphHook hook;
            try {
                hook = JSON_SERIALISER.deserialise(FileUtils.readFileToByteArray(hookPath.toFile()), GraphHook.class);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to load graph hook file: " + hookPath, e);
            }
            return addHook(hook);
        }

        public Builder addHook(final GraphHook graphHook) {
            this.graphHooks.add(graphHook);
            return this;
        }

        public Builder addHooks(final GraphHook... graphHooks) {
            Collections.addAll(this.graphHooks, graphHooks);
            return this;
        }

        public Graph build() {
            if (null == library) {
                library = new NoGraphLibrary();
            }

            if (null == graphId && null != store) {
                graphId = store.getGraphId();
            }

            if (null != graphId) {
                Pair<Schema, StoreProperties> parentGraph = library.get(graphId);

                if (null != parentGraph) {
                    if (null == parentGraph.getSecond()) {
                        throw new IllegalArgumentException("GraphId " + graphId + " found in GraphLibrary, but no store properties are associated with it.");
                    }
                    schema = parentGraph.getFirst();
                    properties = parentGraph.getSecond();

                    LOGGER.debug("Graph ID " + graphId + " found in graph library. Ignoring any other additional schema/properties");
                    parentSchemaIds = null;
                    schemaBytesList.clear();
                    parentStorePropertiesId = null;
                    store = null;
                }
            }

            updateSchema();
            updateStore();
            updateView();

            if (null == graphId) {
                graphId = store.getGraphId();
            }

            if (null == graphId) {
                throw new IllegalArgumentException("graphId is required");
            }

            library.add(graphId, schema, store.getProperties());
            updateGraphHooks();
            return new Graph(schema, store, view, graphHooks);
        }

        private void updateGraphHooks() {
            if (store.isSupported(NamedOperation.class)) {
                boolean hasNamedOpHook = false;
                for (final GraphHook graphHook : graphHooks) {
                    if (NamedOperationResolver.class.isAssignableFrom(graphHook.getClass())) {
                        hasNamedOpHook = true;
                        break;
                    }
                }
                if (!hasNamedOpHook) {
                    graphHooks.add(0, new NamedOperationResolver());
                }
            }
        }

        private void updateSchema() {
            Schema mergedParentSchema = null;

            if (null != parentSchemaIds) {
                for (final String parentSchemaId : parentSchemaIds) {
                    final Schema parentSchema = library.getSchema(parentSchemaId);
                    if (null != parentSchema) {
                        if (null == mergedParentSchema) {
                            mergedParentSchema = parentSchema;
                        } else {
                            mergedParentSchema = new Schema.Builder()
                                    .merge(mergedParentSchema)
                                    .merge(parentSchema)
                                    .build();
                        }
                    }
                }
            }

            if (null != mergedParentSchema) {
                if (null == schema) {
                    schema = mergedParentSchema;
                } else {
                    schema = new Schema.Builder()
                            .merge(mergedParentSchema)
                            .merge(schema)
                            .build();
                }
            }

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
            StoreProperties mergedStoreProperties = null;
            if (null != parentStorePropertiesId) {
                mergedStoreProperties = library.getProperties(parentStorePropertiesId);
            }

            if (null != properties) {
                if (null == mergedStoreProperties) {
                    mergedStoreProperties = properties;
                } else {
                    mergedStoreProperties.getProperties().putAll(properties.getProperties());
                }
            }

            if (null == store) {
                store = Store.createStore(graphId, cloneSchema(schema), mergedStoreProperties);
            } else if ((null != graphId && !graphId.equals(store.getGraphId()))
                    || (null != schema)
                    || (null != mergedStoreProperties && !mergedStoreProperties.equals(store.getProperties()))) {
                if (null == graphId) {
                    graphId = store.getGraphId();
                }
                if (null == schema) {
                    schema = store.getSchema();
                }

                if (null == mergedStoreProperties) {
                    mergedStoreProperties = store.getProperties();
                }

                try {
                    store.initialise(graphId, cloneSchema(schema), mergedStoreProperties);
                } catch (final StoreException e) {
                    throw new IllegalArgumentException("Unable to initialise the store with the given graphId, schema and properties", e);
                }
            }

            store.setGraphLibrary(library);

            if (null == schema) {
                schema = store.getSchema();
            }
        }

        private void updateView() {
            if (null == view) {
                this.view = new View.Builder()
                        .entities(store.getSchema().getEntityGroups())
                        .edges(store.getSchema().getEdgeGroups())
                        .build();
            }
        }

        private Schema cloneSchema(final Schema schema) {
            return null != schema ? schema.clone() : null;
        }
    }
}
