/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federated.simple;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.cache.Cache;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.federated.simple.access.GraphAccess;
import uk.gov.gchq.gaffer.federated.simple.operation.AddGraph;
import uk.gov.gchq.gaffer.federated.simple.operation.ChangeGraphId;
import uk.gov.gchq.gaffer.federated.simple.operation.FederatedOperationChainValidator;
import uk.gov.gchq.gaffer.federated.simple.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.federated.simple.operation.GetAllGraphInfo;
import uk.gov.gchq.gaffer.federated.simple.operation.RemoveGraph;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.EitherOperationHandler;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.FederatedOperationHandler;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.FederatedOutputHandler;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.add.AddGraphHandler;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.get.GetAllGraphIdsHandler;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.get.GetAllGraphInfoHandler;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.get.GetSchemaHandler;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.misc.ChangeGraphIdHandler;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.misc.RemoveGraphHandler;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.DeleteNamedOperation;
import uk.gov.gchq.gaffer.named.operation.GetAllNamedOperations;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.view.AddNamedView;
import uk.gov.gchq.gaffer.named.view.DeleteNamedView;
import uk.gov.gchq.gaffer.named.view.GetAllNamedViews;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.delete.DeleteElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetGraphCreatedTime;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.DeleteAllData;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.operation.handler.GetGraphCreatedTimeHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OperationChainHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.AddNamedOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.AddNamedViewHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.DeleteNamedOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.DeleteNamedViewHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.GetAllNamedOperationsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.GetAllNamedViewsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.NamedOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static uk.gov.gchq.gaffer.accumulostore.utils.TableUtils.renameTable;
import static uk.gov.gchq.gaffer.cache.CacheServiceLoader.DEFAULT_SERVICE_NAME;
import static uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties.PROP_DEFAULT_GRAPH_IDS;
import static uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties.PROP_GRAPH_CACHE_NAME;

/**
 * The federated store implementation. Provides the set up and required
 * methods to enable a {@link Store} that will delegate {@link Operation}s
 * to sub graphs then merge the result.
 */
public class FederatedStore extends Store {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederatedStore.class);
    /**
     * The system user account name, this user is essentially an admin for this store.
     */
    public static final String FEDERATED_STORE_SYSTEM_USER = "FederatedStoreSystemUser";

    private static final String DEFAULT_CACHE_CLASS_FALLBACK = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
    private static final String GRAPH_ID_ERROR = "Graph with Graph ID: %s is not available to this federated store";

    // Default graph IDs to execute on
    private List<String> defaultGraphIds = new LinkedList<>();

    // Gaffer cache of available graphs
    private Cache<String, Pair<GraphSerialisable, GraphAccess>> graphCache;

    // Store specific handlers
    public final Map<Class<? extends Operation>, OperationHandler<?>> storeHandlers = Stream.of(
            new SimpleEntry<>(AddGraph.class, new AddGraphHandler()),
            new SimpleEntry<>(GetAllGraphIds.class, new GetAllGraphIdsHandler()),
            new SimpleEntry<>(GetSchema.class, new GetSchemaHandler()),
            new SimpleEntry<>(ChangeGraphId.class, new ChangeGraphIdHandler()),
            new SimpleEntry<>(GetAllGraphInfo.class, new GetAllGraphInfoHandler()),
            new SimpleEntry<>(RemoveGraph.class, new RemoveGraphHandler()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    /**
     * Add a new graph so that it is available to this federated store.
     *
     * @param graph The serialisable instance of the graph.
     * @param graphAccess The graph access.
     *
     * @throws IllegalArgumentException If there is already a graph with the supplied ID
     */
    public void addGraph(final GraphSerialisable graph, final GraphAccess graphAccess) {
        // Pair the graph with its access in the cache
        Pair<GraphSerialisable, GraphAccess> graphAndAccessPair = new ImmutablePair<>(graph, graphAccess);
        try {
            // Add safely to the cache
            graphCache.getCache().putSafe(graph.getGraphId(), graphAndAccessPair);
        } catch (final CacheOperationException e) {
            // Unknown issue adding to cache
            throw new GafferRuntimeException(e.getMessage(), e);
        } catch (final OverwritingException e) {
            // Notify that the graph ID is already in use
            throw new IllegalArgumentException(
                "A graph with Graph ID: '" + graph.getGraphId() + "' has already been added to this store", e);
        }
    }

    /**
     * Remove a graph from the scope of this store.
     *
     * @param graphId The graph ID of the graph to remove.
     * @throws IllegalArgumentException If graph does not exist.
     */
    public void removeGraph(final String graphId) {
        if (!graphCache.contains(graphId)) {
            throw new IllegalArgumentException(
                String.format(GRAPH_ID_ERROR, graphId));
        }
        graphCache.deleteFromCache(graphId);
    }

    /**
     * Get the {@link GraphSerialisable} from a given graph ID. The graph
     * must obviously be known to this federated store to be returned.
     *
     * @param graphId The graph ID
     * @return The {@link GraphSerialisable} relating to the ID.
     * @throws CacheOperationException If issue getting from cache.
     * @throws IllegalArgumentException If graph not found.
     */
    public GraphSerialisable getGraph(final String graphId) throws CacheOperationException {
        return getGraphAccessPair(graphId).getLeft();
    }

    /**
     * Get the {@link GraphAccess} from the given graph ID. The graph
     * must obviously be known to this federated store to be returned.
     *
     * @param graphId The graph ID
     * @return The {@link GraphAccess} relating to the ID.
     * @throws CacheOperationException  If issue getting from cache.
     * @throws IllegalArgumentException If graph not found.
     */
    public GraphAccess getGraphAccess(final String graphId) throws CacheOperationException {
        return getGraphAccessPair(graphId).getRight();
    }

    /**
     * Gets the {@link Pair} of {@link GraphSerialisable} and
     * {@link GraphAccess} from a given graph ID.
     *
     * @param graphId The graph ID
     * @return The {@link Pair} relating to the graph ID.
     * @throws CacheOperationException  If issue getting from cache.
     * @throws IllegalArgumentException If graph not found.
     */
    public Pair<GraphSerialisable, GraphAccess> getGraphAccessPair(final String graphId) throws CacheOperationException {
        Pair<GraphSerialisable, GraphAccess> graphAndAccess = graphCache.getFromCache(graphId);
        if (graphAndAccess == null) {
            throw new IllegalArgumentException(
                String.format(GRAPH_ID_ERROR, graphId));
        }
        return graphAndAccess;
    }

    /**
     * Returns all the graphs and their access available to this store.
     *
     * @return Iterable of {@link Pair}s containing the {@link GraphSerialisable}
     *         and {@link GraphAccess}
     */
    public Iterable<Pair<GraphSerialisable, GraphAccess>> getAllGraphsAndAccess() {
        return graphCache.getCache().getAllValues();
    }

    /**
     * Get the default list of graph IDs for this federated store.
     *
     * @return The default list.
     */
    public List<String> getDefaultGraphIds() {
        // Only return the graphs that actually exist
        return defaultGraphIds.stream().filter(id -> {
            try {
                return Objects.nonNull(getGraph(id));
            } catch (final IllegalArgumentException | CacheOperationException e) {
                LOGGER.warn("Default Graph was not found: {}", id);
                return false;
            }
        }).collect(Collectors.toList());
    }

    /**
     * Set the default list of graph IDs for this federated store.
     *
     * @param defaultGraphIds Default list to set.
     */
    public void setDefaultGraphIds(final List<String> defaultGraphIds) {
        this.defaultGraphIds = defaultGraphIds;
    }

    /**
     * Change the graph's ID for the specified graph.
     *
     * @param graphToUpdateId The graph that is to have its ID updated.
     * @param newGraphId The new graph ID.
     * @throws StoreException If the accumulo table cannot be renamed.
     * @throws CacheOperationException If issue getting existing graph from cache.
     */
    public void changeGraphId(final String graphToUpdateId, final String newGraphId) throws StoreException, CacheOperationException {
        // Get existing graph and access
        final Pair<GraphSerialisable, GraphAccess> graphPairToUpdate = getGraphAccessPair(graphToUpdateId);
        final GraphSerialisable graphToUpdate = graphPairToUpdate.getLeft();
        final GraphAccess graphAccess = graphPairToUpdate.getRight();

        // Remove from cache
        removeGraph(graphToUpdateId);

        // For accumulo update the table with the new graph ID
        if (graphToUpdate.getStoreProperties().getStoreClass().startsWith(AccumuloStore.class.getPackage().getName())) {

            renameTable((AccumuloProperties) graphToUpdate.getStoreProperties(), graphToUpdateId, newGraphId);

            // Update id in the original graph
            graphToUpdate.getConfig().setGraphId(newGraphId);
            GraphSerialisable updatedGraphSerialisable = new GraphSerialisable.Builder(graphToUpdate)
                .config(graphToUpdate.getConfig())
                .build();
            // Add graph with new id back to cache
            addGraph(updatedGraphSerialisable, graphAccess);
        } else {
            // For other stores just re-add with new graph ID
            graphToUpdate.getConfig().setGraphId(newGraphId);
            addGraph(graphToUpdate, graphAccess);
        }
    }

    /**
     * Gets a merged schema based on the graphs specified.
     *
     * @param graphs The graphs to get the schemas from.
     * @return A merged {@link Schema}
     */
    public Schema getSchema(final List<GraphSerialisable> graphs) {
        try {
            // Get the graph IDs we care about
            List<String> graphIds = new ArrayList<>();
            graphs.forEach(gs -> graphIds.add(gs.getGraphId()));

            // Create and run a get schema operation with relevant IDs
            GetSchema operation = new GetSchema.Builder()
                    .option(FederatedOperationHandler.OPT_SHORT_GRAPH_IDS, String.join(",", graphIds))
                    .build();
            return (Schema) this.handleOperation(operation, new Context());

        } catch (final OperationException e) {
            throw new GafferRuntimeException(e.getMessage(), e);
        }
    }

    /**
     * Access to getting the operations that have handlers specific to this
     * store e.g. operations that should always be handled specially.
     *
     * @return The Operation classes handled by this store.
     */
    public Set<Class<? extends Operation>> getStoreSpecificOperations() {
        return storeHandlers.keySet();
    }

    @Override
    public void initialise(final String graphId, final Schema unused, final StoreProperties properties) throws StoreException {
        super.initialise(graphId, new Schema(), properties);

        // Init the cache for graphs
        graphCache = new Cache<>(properties.get(PROP_GRAPH_CACHE_NAME, "federatedGraphCache_" + graphId));

        // Get and set default graph IDs from properties
        if (properties.containsKey(PROP_DEFAULT_GRAPH_IDS)) {
            // Parse as comma separated list
            setDefaultGraphIds(Arrays.asList(properties.get(PROP_DEFAULT_GRAPH_IDS).split(",")));
        }
    }

    @Override
    public Schema getSchema() {
        // Return a blank schema if we have no default graphs
        if (defaultGraphIds.isEmpty()) {
            return new Schema();
        }

        try {
            return (Schema) this.handleOperation(new GetSchema(), new Context());
        } catch (final OperationException e) {
            throw new GafferRuntimeException(e.getMessage(), e);
        }
    }

    @Override
    protected Object doUnhandledOperation(final Operation operation, final Context context) {
        try {
            // Use a federated handler so operations are forwarded to graphs
            return new FederatedOperationHandler<>().doOperation(operation, context, this);
        } catch (final OperationException e) {
            throw new GafferRuntimeException(e.getMessage(), e);
        }
    }

    @Override
    protected void addAdditionalOperationHandlers() {
        storeHandlers.forEach(this::addOperationHandler);

        final String namedOpCacheSuffix = getProperties().getCacheServiceNamedOperationSuffix(getGraphId());
        final String namedViewCacheSuffix = getProperties().getCacheServiceNamedViewSuffix(getGraphId());
        final Boolean nestedNamedOpsAllowed = getProperties().isNestedNamedOperationAllow();

        // Add overrides as cache operations could be run locally or on sub graphs
        if (getProperties().getNamedOperationEnabled()) {
            addOperationHandler(NamedOperation.class, new EitherOperationHandler<>(new NamedOperationHandler()));
            addOperationHandler(AddNamedOperation.class, new EitherOperationHandler<>(
                    new AddNamedOperationHandler(namedOpCacheSuffix, nestedNamedOpsAllowed)));
            addOperationHandler(GetAllNamedOperations.class, new EitherOperationHandler<>(new GetAllNamedOperationsHandler(namedOpCacheSuffix)));
            addOperationHandler(DeleteNamedOperation.class, new EitherOperationHandler<>(new DeleteNamedOperationHandler(namedOpCacheSuffix)));
        }

        // Named Views could be either
        if (getProperties().getNamedViewEnabled()) {
            addOperationHandler(AddNamedView.class, new EitherOperationHandler<>(new AddNamedViewHandler(namedViewCacheSuffix)));
            addOperationHandler(GetAllNamedViews.class, new EitherOperationHandler<>(new GetAllNamedViewsHandler(namedViewCacheSuffix)));
            addOperationHandler(DeleteNamedView.class, new EitherOperationHandler<>(new DeleteNamedViewHandler(namedViewCacheSuffix)));
        }

        // Misc operations that could be for sub graphs or not
        addOperationHandler(GetGraphCreatedTime.class, new EitherOperationHandler<>(new GetGraphCreatedTimeHandler()));
    }

    @Override
    protected OperationHandler<? extends OperationChain<?>> getOperationChainHandler() {
        return new EitherOperationHandler<>(new OperationChainHandler<>(getOperationChainValidator(), getOperationChainOptimisers()));
    }

    @Override
    protected OperationChainValidator createOperationChainValidator() {
        return new FederatedOperationChainValidator(new ViewValidator());
    }

    @Override
    protected OutputOperationHandler<GetElements, Iterable<? extends Element>> getGetElementsHandler() {
        return new FederatedOutputHandler<>();
    }

    @Override
    protected OutputOperationHandler<GetAllElements, Iterable<? extends Element>> getGetAllElementsHandler() {
        return new FederatedOutputHandler<>();
    }

    @Override
    protected OutputOperationHandler<? extends GetAdjacentIds, Iterable<? extends EntityId>> getAdjacentIdsHandler() {
        return new FederatedOutputHandler<>();
    }

    @Override
    protected OperationHandler<? extends AddElements> getAddElementsHandler() {
        return new FederatedOperationHandler<>();
    }

    @Override
    protected OperationHandler<? extends DeleteElements> getDeleteElementsHandler() {
        return new FederatedOperationHandler<>();
    }

    @Override
    protected OperationHandler<DeleteAllData> getDeleteAllDataHandler() {
        return new FederatedOperationHandler<>();
    }

    @Override
    protected OutputOperationHandler<GetTraits, Set<StoreTrait>> getGetTraitsHandler() {
        return new FederatedOutputHandler<>();
    }

    @Override
    protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
        return ToBytesSerialiser.class;
    }

    @Override
    protected Class<FederatedStoreProperties> getPropertiesClass() {
        return FederatedStoreProperties.class;
    }

    @Override
    protected void startCacheServiceLoader(final StoreProperties properties) {
        super.startCacheServiceLoader(properties);
        // If default not setup then initialise cache as its needed for storing graphs
        if (!CacheServiceLoader.isDefaultEnabled()) {
            CacheServiceLoader.initialise(
                DEFAULT_SERVICE_NAME,
                DEFAULT_CACHE_CLASS_FALLBACK,
                properties.getProperties());
        }
    }
}
