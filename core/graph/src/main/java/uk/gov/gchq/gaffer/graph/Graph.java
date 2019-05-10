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

package uk.gov.gchq.gaffer.graph;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.hook.GraphHook;
import uk.gov.gchq.gaffer.graph.hook.NamedOperationResolver;
import uk.gov.gchq.gaffer.graph.hook.NamedViewResolver;
import uk.gov.gchq.gaffer.graph.hook.UpdateViewHook;
import uk.gov.gchq.gaffer.jobtracker.Job;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.library.NoGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.util.ReflectionUtil;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 * The Graph separates the user from the {@link Store}. It holds an instance of
 * the {@link Store} and
 * acts as a proxy for the store, delegating {@link Operation}s to the store.
 * </p>
 * <p>
 * The Graph provides users with a single point of entry for executing
 * operations on a store.
 * This allows the underlying store to be swapped and the same operations can
 * still be applied.
 * </p>
 * <p>
 * Graphs also provides a view of the data with a instance of {@link View}. The
 * view filters out unwanted information
 * and can transform {@link uk.gov.gchq.gaffer.data.element.Properties} into
 * transient properties such as averages.
 * </p>
 * <p>
 * When executing operations on a graph, an operation view would override the
 * graph view.
 * </p>
 *
 * @see uk.gov.gchq.gaffer.graph.Graph.Builder
 */
public final class Graph {
    private static final Logger LOGGER = LoggerFactory.getLogger(Graph.class);

    /**
     * The instance of the store.
     */
    private final Store store;

    private GraphConfig config;

    /**
     * Constructs a {@code Graph} with the given {@link uk.gov.gchq.gaffer.store.Store}
     * and
     * {@link uk.gov.gchq.gaffer.data.elementdefinition.view.View}.
     *
     * @param config a {@link GraphConfig} used to store the configuration for
     *               a
     *               Graph.
     * @param store  a {@link Store} used to store the elements and handle
     *               operations.
     */
    private Graph(final GraphConfig config, final Store store) {
        this.config = config;
        this.store = store;
    }

    /**
     * Performs the given operation on the store.
     * If the operation does not have a view then the graph view is used.
     *
     * @param operation the operation to be executed.
     * @param user      the user executing the operation.
     * @throws OperationException if an operation fails
     */
    public void execute(final Operation operation, final User user) throws OperationException {
        execute(new GraphRequest<>(operation, user));
    }

    /**
     * Performs the given operation on the store.
     * If the operation does not have a view then the graph view is used.
     * The context will be cloned and a new jobId will be created.
     *
     * @param operation the operation to be executed.
     * @param context   the user context for the execution of the operation
     * @throws OperationException if an operation fails
     */
    public void execute(final Operation operation, final Context context) throws OperationException {
        execute(new GraphRequest<>(operation, context));
    }

    /**
     * Performs the given output operation on the store.
     * If the operation does not have a view then the graph view is used.
     *
     * @param operation the output operation to be executed.
     * @param user      the user executing the operation.
     * @param <O>       the operation chain output type.
     * @return the operation result.
     * @throws OperationException if an operation fails
     */
    public <O> O execute(final Output<O> operation, final User user) throws OperationException {
        return execute(new GraphRequest<>(operation, user)).getResult();
    }

    /**
     * Performs the given output operation on the store.
     * If the operation does not have a view then the graph view is used.
     * The context will be cloned and a new jobId will be created.
     *
     * @param operation the output operation to be executed.
     * @param context   the user context for the execution of the operation.
     * @param <O>       the operation chain output type.
     * @return the operation result.
     * @throws OperationException if an operation fails
     */
    public <O> O execute(final Output<O> operation, final Context context) throws OperationException {
        return execute(new GraphRequest<>(operation, context)).getResult();
    }

    /**
     * Exeutes a {@link GraphRequest} on the graph and returns the {@link GraphResult}.
     *
     * @param request the request to execute
     * @param <O>     the result type
     * @return {@link GraphResult} containing the result and details
     * @throws OperationException if an operation fails
     */
    public <O> GraphResult<O> execute(final GraphRequest<O> request) throws OperationException {
        return _execute(store::execute, request);
    }

    /**
     * Performs the given operation job on the store.
     * If the operation does not have a view then the graph view is used.
     *
     * @param operation the operation to be executed.
     * @param user      the user executing the job.
     * @return the job details
     * @throws OperationException thrown if the job fails to run.
     */
    public JobDetail executeJob(final Operation operation, final User user) throws OperationException {
        return executeJob(new GraphRequest<>(operation, user)).getResult();
    }

    /**
     * Performs the given operation job on the store.
     * If the operation does not have a view then the graph view is used.
     * The context will be cloned and a new jobId will be created.
     *
     * @param operation the operation to be executed.
     * @param context   the user context for the execution of the operation
     * @return the job details
     * @throws OperationException thrown if the job fails to run.
     */
    public JobDetail executeJob(final Operation operation, final Context context) throws OperationException {
        return executeJob(new GraphRequest<>(operation, context)).getResult();
    }

    /**
     * Executes the given {@link GraphRequest} on the graph as an asynchronous job
     * and returns a {@link GraphResult} containing the {@link JobDetail}s.
     * If the operation does not have a view then the graph view is used.
     * The context will be cloned and a new jobId will be created.
     *
     * @param request the request to execute
     * @return {@link GraphResult} containing the job details
     * @throws OperationException thrown if the job fails to run.
     */
    public GraphResult<JobDetail> executeJob(final GraphRequest<?> request) throws OperationException {
        return _execute(store::executeJob, request);
    }

    /**
     * Performs the given Job on the store.
     * This should be used for Scheduled Jobs,
     * although if no repeat is set it
     * will act as a normal Job.
     *
     * @param job  the {@link Job} to execute, which contains the {@link OperationChain} and the {@link uk.gov.gchq.gaffer.jobtracker.Repeat}
     * @param user the user running the Job.
     * @return the job detail.
     * @throws OperationException thrown if the job fails to run.
     */
    public JobDetail executeJob(final Job job, final User user) throws OperationException {
        return executeJob(job, new Context(user));
    }

    /**
     * Performs the given Job on the store.
     * This should be used for Scheduled Jobs,
     * although if no repeat is set it
     * will act as a normal Job.
     *
     * @param job     the {@link Job} to execute, which contains the {@link OperationChain} and the {@link uk.gov.gchq.gaffer.jobtracker.Repeat}
     * @param context the user context for the execution of the operation
     * @return the job detail
     * @throws OperationException thrown if the job fails to run.
     */
    public JobDetail executeJob(final Job job, final Context context) throws OperationException {
        if (null == context) {
            throw new IllegalArgumentException("A context is required");
        }

        if (null == job) {
            throw new IllegalArgumentException("A job is required");
        }

        OperationChain wrappedOriginal = OperationChain.wrap(job.getOperation());

        context.setOriginalOpChain(wrappedOriginal);

        final Context clonedContext = context.shallowClone();
        final OperationChain clonedOpChain = wrappedOriginal.shallowClone();
        JobDetail result = null;
        try {
            updateOperationChainView(clonedOpChain);
            for (final GraphHook graphHook : config.getHooks()) {
                graphHook.preExecute(clonedOpChain, clonedContext);
            }
            updateOperationChainView(clonedOpChain);
            job.setOperation(clonedOpChain);
            result = store.executeJob(job, context);
            for (final GraphHook graphHook : config.getHooks()) {
                graphHook.postExecute(result, clonedOpChain, clonedContext);
            }
        } catch (final Exception e) {
            for (final GraphHook graphHook : config.getHooks()) {
                try {
                    result = graphHook.onFailure(result, clonedOpChain, clonedContext, e);
                } catch (final Exception graphHookE) {
                    LOGGER.warn("Error in graphHook " + graphHook.getClass().getSimpleName() + ": " + graphHookE.getMessage(), graphHookE);
                }
            }
            CloseableUtil.close(clonedOpChain);
            CloseableUtil.close(result);
            throw e;
        }
        return result;
    }

    private <O> GraphResult<O> _execute(final StoreExecuter<O> storeExecuter, final GraphRequest<?> request) throws OperationException {
        if (null == request) {
            throw new IllegalArgumentException("A request is required");
        }

        if (null == request.getContext()) {
            throw new IllegalArgumentException("A context is required");
        }

        request.getContext().setOriginalOpChain(request.getOperationChain());

        final Context clonedContext = request.getContext().shallowClone();
        final OperationChain clonedOpChain = request.getOperationChain().shallowClone();
        O result = null;
        try {
            updateOperationChainView(clonedOpChain);
            for (final GraphHook graphHook : config.getHooks()) {
                graphHook.preExecute(clonedOpChain, clonedContext);
            }
            // TODO - remove in V2
            // This updates the view, used for empty or null views, for
            // example if there is a NamedOperation that has been resolved
            // that contains an empty view
            updateOperationChainView(clonedOpChain);
            // Runs the updateGraphHook instance (if set) or if not runs a
            // new instance
            UpdateViewHook hookInstance = null;
            for (final GraphHook graphHook : config.getHooks()) {
                if (UpdateViewHook.class.isAssignableFrom(graphHook.getClass())) {
                    hookInstance = (UpdateViewHook) graphHook;
                    break;
                }
            }
            if (null == hookInstance) {
                UpdateViewHook updateViewHook = new UpdateViewHook();
                updateViewHook.preExecute(clonedOpChain, clonedContext);
            } else {
                hookInstance.preExecute(clonedOpChain, clonedContext);
            }
            result = (O) storeExecuter.execute(clonedOpChain, clonedContext);
            for (final GraphHook graphHook : config.getHooks()) {
                result = graphHook.postExecute(result, clonedOpChain, clonedContext);
            }
        } catch (final Exception e) {
            for (final GraphHook graphHook : config.getHooks()) {
                try {
                    result = graphHook.onFailure(result, clonedOpChain, clonedContext, e);
                } catch (final Exception graphHookE) {
                    LOGGER.warn("Error in graphHook " + graphHook.getClass().getSimpleName() + ": " + graphHookE.getMessage(), graphHookE);
                }
            }
            CloseableUtil.close(clonedOpChain);
            CloseableUtil.close(result);
            throw e;
        }
        return new GraphResult<>(result, clonedContext);
    }

    private void updateOperationChainView(final Operations<?> operations) {
        for (final Operation operation : operations.getOperations()) {
            if (operation instanceof Operations) {
                updateOperationChainView((Operations) operation);
            } else if (operation instanceof OperationView) {
                View opView = ((OperationView) operation).getView();
                if (null == opView) {
                    opView = config.getView();
                } else if (!(opView instanceof NamedView) && !opView.hasGroups() && !opView.isAllEdges() && !opView.isAllEntities()) {
                    opView = new View.Builder()
                            .merge(config.getView())
                            .merge(opView)
                            .build();
                } else if (opView.isAllEdges() || opView.isAllEntities()) {
                    View.Builder opViewBuilder = new View.Builder()
                            .merge(opView);
                    if (opView.isAllEdges()) {
                        opViewBuilder.edges(getSchema().getEdgeGroups());
                    }
                    if (opView.isAllEntities()) {
                        opViewBuilder.entities(getSchema().getEntityGroups());
                    }
                    opView = opViewBuilder.build();
                }
                opView.expandGlobalDefinitions();
                ((OperationView) operation).setView(opView);
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
        return config.getView();
    }

    /**
     * @return the original schema.
     */
    public Schema getSchema() {
        return store.getOriginalSchema();
    }

    /**
     * @return the description held in the {@link GraphConfig}
     */
    public String getDescription() {
        return config.getDescription();
    }

    /**
     * @param storeTrait the store trait to check
     * @return true if the store has the given trait.
     */
    public boolean hasTrait(final StoreTrait storeTrait) {
        return store.hasTrait(storeTrait);
    }

    /**
     * Returns all the {@link StoreTrait}s for the contained {@link Store}
     * implementation
     *
     * @return a {@link Set} of all of the {@link StoreTrait}s that the store
     * has.
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
        if (config.getHooks().isEmpty()) {
            return Collections.emptyList();
        }

        return (List) config.getHooks().stream().map(GraphHook::getClass).collect(Collectors.toList());
    }

    public GraphLibrary getGraphLibrary() {
        return store.getGraphLibrary();
    }

    protected GraphConfig getConfig() {
        return config;
    }

    @FunctionalInterface
    private interface StoreExecuter<O> {
        O execute(final OperationChain<O> operation, final Context context) throws OperationException;
    }

    /**
     * <p>
     * Builder for {@link Graph}.
     * </p>
     * We recommend instantiating a Graph from a graphConfig.json file, a
     * schema
     * directory and a store.properties file.
     * For example:
     * <pre>
     * new Graph.Builder()
     *     .config(Paths.get("graphConfig.json"))
     *     .addSchemas(Paths.get("schema"))
     *     .storeProperties(Paths.get("store.properties"))
     *     .build();
     * </pre>
     */
    public static class Builder {
        public static final String UNABLE_TO_READ_SCHEMA_FROM_URI = "Unable to read schema from URI";
        private final GraphConfig.Builder configBuilder = new GraphConfig.Builder();
        private final List<byte[]> schemaBytesList = new ArrayList<>();
        private Store store;
        private StoreProperties properties;
        private Schema schema;
        private List<String> parentSchemaIds;
        private String parentStorePropertiesId;
        private boolean addToLibrary = true;

        /**
         * @param graphId the graph id to set
         * @return this Builder
         * @deprecated use Builder.config instead.
         */
        @Deprecated
        public Builder graphId(final String graphId) {
            configBuilder.graphId(graphId);
            return this;
        }

        public Builder config(final Path path) {
            configBuilder.json(path);
            return this;
        }

        public Builder config(final URI uri) {
            configBuilder.json(uri);
            return this;
        }

        public Builder config(final InputStream stream) {
            configBuilder.json(stream);
            return this;
        }

        public Builder config(final byte[] bytes) {
            configBuilder.json(bytes);
            return this;
        }

        public Builder config(final GraphConfig config) {
            configBuilder.merge(config);
            return this;
        }

        public Builder addToLibrary(final boolean addToLibrary) {
            this.addToLibrary = addToLibrary;
            return this;
        }

        /**
         * @param library the graph library to set
         * @return this Builder
         * @deprecated use Builder.config instead.
         */
        @Deprecated
        public Builder library(final GraphLibrary library) {
            configBuilder.library(library);
            return this;
        }

        /**
         * @param view the graph view to set
         * @return this Builder
         * @deprecated use Builder.config instead.
         */
        @Deprecated
        public Builder view(final View view) {
            configBuilder.view(view);
            return this;
        }

        /**
         * @param view the graph view path to set
         * @return this Builder
         * @deprecated use Builder.config instead.
         */
        @Deprecated
        public Builder view(final Path view) {
            configBuilder.view(view);
            return this;
        }

        /**
         * @param view the graph view input stream to set
         * @return this Builder
         * @deprecated use Builder.config instead.
         */
        @Deprecated
        public Builder view(final InputStream view) {
            configBuilder.view(view);
            return this;
        }

        /**
         * @param view the graph view URI to set
         * @return this Builder
         * @deprecated use Builder.config instead.
         */
        @Deprecated
        public Builder view(final URI view) {
            configBuilder.view(view);
            return this;
        }

        /**
         * @param jsonBytes the graph view json bytes to set
         * @return this Builder
         * @deprecated use Builder.config instead.
         */
        @Deprecated
        public Builder view(final byte[] jsonBytes) {
            configBuilder.view(jsonBytes);
            return this;
        }

        public Builder description(final String description) {
            configBuilder.description(description);
            return this;
        }

        public Builder parentStorePropertiesId(final String parentStorePropertiesId) {
            this.parentStorePropertiesId = parentStorePropertiesId;
            return this;
        }

        public Builder storeProperties(final Properties properties) {
            return storeProperties(null != properties ? StoreProperties.loadStoreProperties(properties) : null);
        }

        public Builder storeProperties(final StoreProperties properties) {
            this.properties = properties;
            if (null != properties) {
                ReflectionUtil.addReflectionPackages(properties.getReflectionPackages());
                JSONSerialiser.update(
                        properties.getJsonSerialiserClass(),
                        properties.getJsonSerialiserModules(),
                        properties.getStrictJson()
                );
            }
            return this;
        }

        public Builder storeProperties(final String propertiesPath) {
            return storeProperties(null != propertiesPath ? StoreProperties.loadStoreProperties(propertiesPath) : null);
        }

        public Builder storeProperties(final Path propertiesPath) {
            if (null == propertiesPath) {
                properties = null;
            } else {
                storeProperties(StoreProperties.loadStoreProperties(propertiesPath));
            }
            return this;
        }

        public Builder storeProperties(final InputStream propertiesStream) {
            if (null == propertiesStream) {
                properties = null;
            } else {
                storeProperties(StoreProperties.loadStoreProperties(propertiesStream));
            }
            return this;
        }

        public Builder storeProperties(final URI propertiesURI) {
            if (null != propertiesURI) {
                try {
                    storeProperties(StreamUtil.openStream(propertiesURI));
                } catch (final IOException e) {
                    throw new SchemaException("Unable to read storeProperties from URI: " + propertiesURI, e);
                }
            }

            return this;
        }

        public Builder addStoreProperties(final Properties properties) {
            if (null != properties) {
                addStoreProperties(StoreProperties.loadStoreProperties(properties));
            }
            return this;
        }

        public Builder addStoreProperties(final StoreProperties updateProperties) {
            if (null != updateProperties) {
                if (null == this.properties) {
                    storeProperties(updateProperties);
                } else {
                    this.properties.merge(updateProperties);
                }
            }
            return this;
        }

        public Builder addStoreProperties(final String updatePropertiesPath) {
            if (null != updatePropertiesPath) {
                addStoreProperties(StoreProperties.loadStoreProperties(updatePropertiesPath));
            }
            return this;
        }

        public Builder addStoreProperties(final Path updatePropertiesPath) {
            if (null != updatePropertiesPath) {
                addStoreProperties(StoreProperties.loadStoreProperties(updatePropertiesPath));
            }
            return this;
        }

        public Builder addStoreProperties(final InputStream updatePropertiesStream) {
            if (null != updatePropertiesStream) {
                addStoreProperties(StoreProperties.loadStoreProperties(updatePropertiesStream));
            }
            return this;
        }

        public Builder addStoreProperties(final URI updatePropertiesURI) {
            if (null != updatePropertiesURI) {
                try {
                    addStoreProperties(StreamUtil.openStream(updatePropertiesURI));
                } catch (final IOException e) {
                    throw new SchemaException("Unable to read storeProperties from URI: " + updatePropertiesURI, e);
                }
            }
            return this;
        }

        public Builder addParentSchemaIds(final List<String> parentSchemaIds) {
            if (null != parentSchemaIds) {
                if (null == this.parentSchemaIds) {
                    this.parentSchemaIds = new ArrayList<>(parentSchemaIds);
                } else {
                    this.parentSchemaIds.addAll(parentSchemaIds);
                }
            }
            return this;
        }

        public Builder addParentSchemaIds(final String... parentSchemaIds) {
            if (null != parentSchemaIds) {
                if (null == this.parentSchemaIds) {
                    this.parentSchemaIds = Lists.newArrayList(parentSchemaIds);
                } else {
                    Collections.addAll(this.parentSchemaIds, parentSchemaIds);
                }
            }
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
            if (null != schemaModule) {
                if (null != schema) {
                    schema = new Schema.Builder()
                            .merge(schema)
                            .merge(schemaModule)
                            .build();
                } else {
                    schema = schemaModule;
                }
            }
            return this;
        }

        public Builder addSchema(final InputStream schemaStream) {
            if (null != schemaStream) {
                try {
                    addSchema(IOUtils.toByteArray(schemaStream));
                } catch (final IOException e) {
                    throw new SchemaException("Unable to read schema from input stream", e);
                } finally {
                    CloseableUtil.close(schemaStream);
                }
            }
            return this;
        }

        public Builder addSchema(final URI schemaURI) {
            if (null != schemaURI) {
                try {
                    addSchema(StreamUtil.openStream(schemaURI));
                } catch (final IOException e) {
                    throw new SchemaException(UNABLE_TO_READ_SCHEMA_FROM_URI, e);
                }
            }
            return this;
        }

        public Builder addSchemas(final URI... schemaURI) {
            if (null != schemaURI) {
                try {
                    addSchemas(StreamUtil.openStreams(schemaURI));
                } catch (final IOException e) {
                    throw new SchemaException(UNABLE_TO_READ_SCHEMA_FROM_URI, e);
                }
            }
            return this;
        }

        public Builder addSchema(final Path schemaPath) {
            if (null != schemaPath) {
                try {
                    if (Files.isDirectory(schemaPath)) {
                        for (final Path path : Files.newDirectoryStream(schemaPath)) {
                            addSchema(path);
                        }
                    } else {
                        addSchema(Files.readAllBytes(schemaPath));
                    }
                } catch (final IOException e) {
                    throw new SchemaException("Unable to read schema from path: " + schemaPath, e);
                }
            }
            return this;
        }

        public Builder addSchema(final byte[] schemaBytes) {
            if (null != schemaBytes) {
                schemaBytesList.add(schemaBytes);
            }
            return this;
        }

        public Builder store(final Store store) {
            this.store = store;
            return this;
        }

        /**
         * @param hooksPath the graph hooks path
         * @return this Builder
         * @deprecated use Builder.config instead.
         */
        @Deprecated
        public Builder addHooks(final Path hooksPath) {
            if (null == hooksPath || !hooksPath.toFile().exists()) {
                throw new IllegalArgumentException("Unable to find graph hooks file: " + hooksPath);
            }
            final GraphHook[] hooks;
            try {
                hooks = JSONSerialiser.deserialise(FileUtils.readFileToByteArray(hooksPath.toFile()), GraphHook[].class);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to load graph hooks file: " + hooksPath, e);
            }
            return addHooks(hooks);
        }

        /**
         * @param hookPath the graph hook path
         * @return this Builder
         * @deprecated use Builder.config instead.
         */
        @Deprecated
        public Builder addHook(final Path hookPath) {
            if (null == hookPath || !hookPath.toFile().exists()) {
                throw new IllegalArgumentException("Unable to find graph hook file: " + hookPath);
            }

            final GraphHook hook;
            try {
                hook = JSONSerialiser.deserialise(FileUtils.readFileToByteArray(hookPath.toFile()), GraphHook.class);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Unable to load graph hook file: " + hookPath, e);
            }
            return addHook(hook);
        }

        /**
         * @param graphHook the graph hook to add
         * @return this Builder
         * @deprecated use Builder.config instead.
         */
        @Deprecated
        public Builder addHook(final GraphHook graphHook) {
            configBuilder.addHook(graphHook);
            return this;
        }

        /**
         * @param graphHooks the graph hooks to add
         * @return this Builder
         * @deprecated use Builder.config instead.
         */
        @Deprecated
        public Builder addHooks(final GraphHook... graphHooks) {
            configBuilder.addHooks(graphHooks);
            return this;
        }

        public Graph build() {
            final GraphConfig config = configBuilder.build();
            if (null == config.getLibrary()) {
                config.setLibrary(new NoGraphLibrary());
            }

            if (null == config.getGraphId() && null != store) {
                config.setGraphId(store.getGraphId());
            }

            updateStoreProperties(config);

            updateSchema(config);

            if (null != config.getLibrary() && config.getLibrary().exists(config.getGraphId())) {
                //Set Props & Schema if null.
                final Pair<Schema, StoreProperties> pair = config.getLibrary().get(config.getGraphId());
                properties = (null == properties) ? pair.getSecond() : properties;
                schema = (null == schema) ? pair.getFirst() : schema;
            }

            updateStore(config);

            if (null != config.getGraphId()) {
                config.getLibrary().checkExisting(config.getGraphId(), schema, properties);
            }

            updateView(config);

            if (null == config.getGraphId()) {
                config.setGraphId(store.getGraphId());
            }

            if (null == config.getGraphId()) {
                throw new IllegalArgumentException("graphId is required");
            }

            updateGraphHooks(config);


            if (addToLibrary) {
                config.getLibrary().add(config.getGraphId(), schema, store.getProperties());
            }

            store.setOriginalSchema(schema);

            return new Graph(config, store);
        }

        private void updateGraphHooks(final GraphConfig config) {
            boolean hasNamedOpHook = false;
            boolean hasNamedViewHook = false;
            for (final GraphHook graphHook : config.getHooks()) {
                if (NamedOperationResolver.class.isAssignableFrom(graphHook.getClass())) {
                    hasNamedOpHook = true;
                }
                if (NamedViewResolver.class.isAssignableFrom(graphHook.getClass())) {
                    hasNamedViewHook = true;
                }
            }
            if (!hasNamedViewHook) {
                config.getHooks().add(0, new NamedViewResolver());
            }
            if (!hasNamedOpHook) {
                if (store.isSupported(NamedOperation.class)) {
                    config.getHooks().add(0, new NamedOperationResolver());
                }
            }
        }

        private void updateSchema(final GraphConfig config) {
            Schema mergedParentSchema = null;

            if (null != parentSchemaIds) {
                for (final String parentSchemaId : parentSchemaIds) {
                    if (null != parentSchemaId) {
                        final Schema parentSchema = config.getLibrary().getSchema(parentSchemaId);
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

        private void updateStoreProperties(final GraphConfig config) {
            StoreProperties mergedStoreProperties = null;
            if (null != parentStorePropertiesId) {
                mergedStoreProperties = config.getLibrary().getProperties(parentStorePropertiesId);
            }

            if (null != properties) {
                if (null == mergedStoreProperties) {
                    mergedStoreProperties = properties;
                } else {
                    mergedStoreProperties.merge(properties);
                }
            }
            properties = mergedStoreProperties;
        }

        private void updateStore(final GraphConfig config) {
            if (null == store) {
                store = Store.createStore(config.getGraphId(), cloneSchema(schema), properties);
            } else if ((null != config.getGraphId() && !config.getGraphId().equals(store.getGraphId()))
                    || (null != schema)
                    || (null != properties && !properties.equals(store.getProperties()))) {
                if (null == config.getGraphId()) {
                    config.setGraphId(store.getGraphId());
                }
                if (null == schema || schema.getGroups().isEmpty()) {
                    schema = store.getSchema();
                }

                if (null == properties) {
                    properties = store.getProperties();
                }

                try {
                    store.initialise(config.getGraphId(), cloneSchema(schema), properties);
                } catch (final StoreException e) {
                    throw new IllegalArgumentException("Unable to initialise the store with the given graphId, schema and properties", e);
                }
            }

            store.setGraphLibrary(config.getLibrary());

            if (null == schema || schema.getGroups().isEmpty()) {
                schema = store.getSchema();
            }
        }

        private void updateView(final GraphConfig config) {
            if (null == config.getView()) {
                config.setView(new View.Builder()
                        .entities(store.getSchema().getEntityGroups())
                        .edges(store.getSchema().getEdgeGroups())
                        .build());
            }
        }

        private Schema cloneSchema(final Schema schema) {
            return null != schema ? schema.clone() : null;
        }

    }
}
