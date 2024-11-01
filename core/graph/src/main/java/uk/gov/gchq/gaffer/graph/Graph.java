/*
 * Copyright 2017-2024 Crown Copyright
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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.cache.Cache;
import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.otel.OtelUtil;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.hook.FunctionAuthoriser;
import uk.gov.gchq.gaffer.graph.hook.FunctionAuthoriserUtil;
import uk.gov.gchq.gaffer.graph.hook.GraphHook;
import uk.gov.gchq.gaffer.graph.hook.NamedOperationResolver;
import uk.gov.gchq.gaffer.graph.hook.NamedViewResolver;
import uk.gov.gchq.gaffer.graph.hook.UpdateViewHook;
import uk.gov.gchq.gaffer.jobtracker.Job;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.view.AddNamedView;
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
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.util.ReflectionUtil;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.commons.collections4.CollectionUtils.isEmpty;

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

    private final GraphConfig config;

    /**
     * Constructs a {@code Graph} with the given {@link uk.gov.gchq.gaffer.store.Store}
     * and
     * {@link uk.gov.gchq.gaffer.data.elementdefinition.view.View}.
     *
     * @param config a {@link GraphConfig} used to store the configuration for
     *               a Graph.
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
     * Executes a {@link GraphRequest} on the graph and returns the {@link GraphResult}.
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
                    LOGGER.warn("Error in graphHook {} : {}", graphHook.getClass().getSimpleName(), graphHookE.getMessage(), graphHookE);
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

        // OpenTelemetry hooks
        Span span = OtelUtil.startSpan(
            this.getClass().getName(),
            "Graph Request: " + clonedOpChain.toOverviewString());
        span.setAttribute(OtelUtil.GRAPH_ID_ATTRIBUTE, getGraphId());
        span.setAttribute(OtelUtil.JOB_ID_ATTRIBUTE, clonedContext.getJobId());
        span.setAttribute(OtelUtil.USER_ATTRIBUTE, clonedContext.getUser().getUserId());

        O result = null;
        // Sets the span to current so parent child spans are auto linked
        try (Scope scope = span.makeCurrent()) {
            updateOperationChainView(clonedOpChain);
            for (final GraphHook graphHook : config.getHooks()) {
                graphHook.preExecute(clonedOpChain, clonedContext);
            }
            // This updates the view, used for empty or null views, for
            // example if there is a NamedOperation that has been resolved
            // that contains an empty view
            updateOperationChainView(clonedOpChain);
            // Runs the updateGraphHook instance (if set) or if not runs a
            // new instance
            ArrayList<UpdateViewHook> hookInstances = new ArrayList<>();
            for (final GraphHook graphHook : config.getHooks()) {
                if (UpdateViewHook.class.isAssignableFrom(graphHook.getClass())) {
                    hookInstances.add((UpdateViewHook) graphHook);
                }
            }
            if (hookInstances.isEmpty()) {
                hookInstances.add(new UpdateViewHook());
            }
            for (final UpdateViewHook hook : hookInstances) {
                hook.preExecute(clonedOpChain, clonedContext);
            }
            span.addEvent("Finished Pre-execute");
            result = (O) storeExecuter.execute(clonedOpChain, clonedContext);
            span.addEvent("Operation Chain Complete");
            for (final GraphHook graphHook : config.getHooks()) {
                result = graphHook.postExecute(result, clonedOpChain, clonedContext);
            }
            span.addEvent("Finished Post-execute");
        } catch (final Exception e) {
            for (final GraphHook graphHook : config.getHooks()) {
                try {
                    result = graphHook.onFailure(result, clonedOpChain, clonedContext, e);
                } catch (final Exception graphHookE) {
                    LOGGER.warn("Error in graphHook {} : {}", graphHook.getClass().getSimpleName(), graphHookE.getMessage(), graphHookE);
                }
            }
            CloseableUtil.close(clonedOpChain);
            CloseableUtil.close(result);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw e;
        } finally {
            span.end();
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

                    // If we have either global elements or nothing at all then
                    // merge with both Entities and Edges
                    if (!isEmpty(opView.getGlobalElements()) || (isEmpty(opView.getGlobalEdges()) && isEmpty(opView.getGlobalEntities()))) {
                        opView = new View.Builder().merge(config.getView()).merge(opView).build();
                    } else { // We have either global edges or entities in
                        // opView, but not both
                        final View originalView = opView;
                        final View partialConfigView = new View.Builder()
                                .merge(config.getView())
                                .removeEdges((x -> isEmpty(originalView.getGlobalEdges())))
                                .removeEntities((x -> isEmpty(originalView.getGlobalEntities())))
                                .build();
                        opView = new View.Builder().merge(partialConfigView)
                                .merge(opView)
                                .build();

                    }
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
     * Returns the current {@link GraphConfig} that holds the configuration for
     * the graph.
     *
     * @return The graph config.
     */
    public GraphConfig getConfig() {
        return config;
    }

    /**
     * Returns the graph view from the {@link GraphConfig}.
     *
     * @return the graph view.
     */
    public View getView() {
        return config.getView();
    }

    /**
     * Returns the description held in the {@link GraphConfig}.
     *
     * @return the description
     */
    public String getDescription() {
        return config.getDescription();
    }

    /**
     * Returns the graph hooks from the {@link GraphConfig}
     *
     * @return The list of {@link GraphHook}s
     */
    public List<Class <? extends GraphHook>> getGraphHooks() {
        if (config.getHooks().isEmpty()) {
            return Collections.emptyList();
        }
        return config.getHooks().stream().map(GraphHook::getClass).collect(Collectors.toList());
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
     * Get the Store's original {@link Schema}.
     * <p>
     * This is not the same as the {@link Schema} used internally by
     * the {@link Store}. See {@link Store#getOriginalSchema()} and
     * {@link Store#getSchema()} for more details.
     *
     * @return the original {@link Schema} used to create this graph
     */
    public Schema getSchema() {
        return store.getOriginalSchema();
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

    public GraphLibrary getGraphLibrary() {
        return store.getGraphLibrary();
    }

    public List<Cache<?, ?>> getCaches() {
        return store.getCaches();
    }

    public String getCreatedTime() {
        return store.getCreatedTime();
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
     *
     * <pre>
     * new Graph.Builder()
     *         .config(Paths.get("graphConfig.json"))
     *         .addSchemas(Paths.get("schema"))
     *         .storeProperties(Paths.get("store.properties"))
     *         .build();
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
                        properties.getStrictJson());
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

        @SuppressWarnings("PMD.UseTryWithResources")
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

        @SuppressWarnings("PMD.UseTryWithResources")
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
            try {
                // If directory load all schemas from it
                if (Files.isDirectory(schemaPath)) {
                    try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(schemaPath)) {
                        directoryStream.forEach(this::addSchema);
                    }
                } else {
                    // Just load the file
                    addSchema(Files.readAllBytes(schemaPath));
                }
            } catch (final IOException e) {
                throw new SchemaException("Unable to read schema from path: " + schemaPath, e);
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

        public Graph build() {
            // Initialise GraphConfig (with default library of NoGraphLibrary)
            final GraphConfig config = configBuilder.build();

            // Enable opentelemetry if configured to be active
            if (Boolean.TRUE.equals(config.getOtelActive())) {
                AutoConfiguredOpenTelemetrySdk.initialize();
                OtelUtil.setOpenTelemetryActive(true);
            }

            // Make sure parent store properties are applied
            if (parentStorePropertiesId != null) {
                final StoreProperties parentStoreProperties = config.getLibrary().getProperties(parentStorePropertiesId);
                properties = applyParentStoreProperties(properties, parentStoreProperties);
            }

            // Make sure parent schemas are applied
            if (parentSchemaIds != null) {
                final List<Schema> parentSchemas = parentSchemaIds.stream().map(id -> config.getLibrary().getSchema(id)).collect(Collectors.toList());
                schema = applyParentSchemas(schema, parentSchemas);
            }

            loadSchemaFromJson();

            if (null != config.getLibrary() && config.getLibrary().exists(config.getGraphId())) {
                // Set Props & Schema if null.
                final Pair<Schema, StoreProperties> pair = config.getLibrary().get(config.getGraphId());
                properties = (null == properties) ? pair.getSecond() : properties;
                schema = (null == schema) ? pair.getFirst() : schema;
            }

            // Initialise the store
            initStore(config);

            // Check this graph does not conflict with an existing graph library graph
            config.getLibrary().checkExisting(config.getGraphId(), schema, properties);

            // Initialise the view
            config.initView(schema);

            // Validate and set up the graph hooks
            validateAndUpdateHooks(config);

            // Add this graph to the graph library (true by default)
            if (addToLibrary) {
                config.getLibrary().add(config.getGraphId(), schema, store.getProperties());
            }

            // Set the original schema used to create the graph.
            // This is stored inside the Store but is primarily
            // used by this class.
            store.setOriginalSchema(schema);

            return new Graph(config, store);
        }

        private void validateAndUpdateHooks(final GraphConfig config) {
            config.validateAndUpdateGetFromCacheHook(
                store,
                AddNamedView.class,
                NamedViewResolver.class,
                properties != null ? properties.getCacheServiceNamedViewSuffix(config.getGraphId()) : null);

            config.validateAndUpdateGetFromCacheHook(
                store,
                AddNamedOperation.class,
                NamedOperationResolver.class,
                properties != null ? properties.getCacheServiceNamedOperationSuffix(config.getGraphId()) : null);

            if (!config.hasHook(FunctionAuthoriser.class)) {
                LOGGER.info("No FunctionAuthoriser hook was supplied, adding default hook.");
                config.addHook(new FunctionAuthoriser(FunctionAuthoriserUtil.DEFAULT_UNAUTHORISED_FUNCTIONS));
            }
        }

        /**
         * Loads the current schema list of byte arrays as json.
         */
        private void loadSchemaFromJson() {
            if (schemaBytesList.isEmpty()) {
                LOGGER.debug("No schema bytes have been supplied unable to load as JSON");
                return;
            }
            if (properties == null) {
                throw new IllegalArgumentException("To load a schema from json, the store properties must be provided.");
            }

            // Get the schema class to load with
            final Class<? extends Schema> schemaClass = properties.getSchemaClass();

            // Add the schemas
            schemaBytesList.forEach(schemaBytes -> {
                Schema newSchema = new Schema.Builder()
                    .json(schemaClass, schemaBytes)
                    .build();
                addSchema(newSchema);
            });
        }

        /**
         * Merges the supplied properties with the properties from the parent store
         * and returns the result. The parent store properties are applied first so
         * the properties supplied can override them during the merge.
         *
         * @param properties The current properties.
         * @param parentStoreProperties The properties the parent store to apply
         * @return Merged properties of the parent store and the supplied properties.
         */
        private StoreProperties applyParentStoreProperties(final StoreProperties properties, final StoreProperties parentStoreProperties) {
            // Start with the parent store properties
            StoreProperties mergedProperties = parentStoreProperties;

            // Apply rest of properties
            if (properties != null) {
                mergedProperties.merge(properties);
            }
            return mergedProperties;
        }

        /**
         * Merges the supplied schema with any parent schemas and returns the
         * result.
         *
         * @param currentSchema The current schema.
         * @param parentSchemas The list of parent schemas to merge.
         * @return Merged schema of any parents and the supplied schema.
         */
        private Schema applyParentSchemas(final Schema currentSchema, final List<Schema> parentSchemas) {
            Schema mergedSchema = null;

            // Apply parent schemas
            for (final Schema parentSchema : parentSchemas) {
                // If the merged result is still null init with the current schema else merge
                if (mergedSchema == null) {
                    mergedSchema = parentSchema;
                } else {
                    mergedSchema = new Schema.Builder()
                        .merge(mergedSchema)
                        .merge(parentSchema)
                        .build();
                }
            }
            // Merge parent schemas with current schema
            if (mergedSchema != null && currentSchema != null) {
                mergedSchema = new Schema.Builder()
                    .merge(mergedSchema)
                    .merge(currentSchema)
                    .build();
            }

            return mergedSchema;
        }

        /**
         * Initialises the {@link Store} class instance with a given graph configuration.
         * Will only initialise the store if it is currently null or relevant parameters
         * are not null e.g. schema/graph ID.
         * <p>
         * Will also attempt to extract some graph configuration from the current store if
         * not currently configured.
         *
         * @param config The graph config
         */
        private void initStore(final GraphConfig config) {
            // If store was not supplied then create it
            // This also checks the GraphId is valid
            if (store == null) {
                LOGGER.debug("Store currently null initialising with Id: {} and existing schema/properties", config.getGraphId());
                store = Store.createStore(config.getGraphId(), cloneSchema(schema), properties);
            }

            // Only init if some configuration has already been set up and is different to what the store already has
            if ((config.getGraphId() != null && !config.getGraphId().equals(store.getGraphId())) ||
                (schema != null) ||
                (properties != null && !properties.equals(store.getProperties()))) {
                try {
                    store.initialise(config.getGraphId(), cloneSchema(schema), properties);
                } catch (final StoreException e) {
                    throw new IllegalArgumentException("Unable to initialise the store with the given graphId, schema and properties", e);
                }
            }

            // Use the store's graph Id if we don't have one configured already
            if (config.getGraphId() == null) {
                config.setGraphId(store.getGraphId());
            }

            // Use the store's schema if not already set
            if (schema == null) {
                schema = store.getSchema();
            }

            // Use the store's properties if not already set
            if (properties == null) {
                properties = store.getProperties();
            }

            store.setGraphLibrary(config.getLibrary());
        }

        /**
         * Null safe clone of the supplied schema.
         *
         * @param schema The schema to clone.
         * @return Clone of the schema.
         */
        private Schema cloneSchema(final Schema schema) {
            return schema != null ? schema.clone() : null;
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Graph graph = (Graph) o;

        return new EqualsBuilder()
                .append(new GraphSerialisable.Builder(this).build(), new GraphSerialisable.Builder(graph).build())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(store)
                .append(config)
                .toHashCode();
    }
}
