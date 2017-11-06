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

package uk.gov.gchq.gaffer.federatedstore;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.federatedstore.exception.StorageException;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperationChain;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedAggregateHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedFilterHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedGetSchemaHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedOperationHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedTransformHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedValidateHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedAddGraphHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetAdjacentIdsHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetAllElementsHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetAllGraphIDHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetElementsHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedOperationChainHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedRemoveGraphHandler;
import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.Validate;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.function.Aggregate;
import uk.gov.gchq.gaffer.operation.impl.function.Filter;
import uk.gov.gchq.gaffer.operation.impl.function.Transform;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties.IS_PUBLIC_ACCESS_ALLOWED_DEFAULT;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getCleanStrings;

/**
 * <p>
 * A Store that encapsulates a collection of sub-graphs and executes operations
 * against them and returns results as though it was a single graph.
 * <p>
 * To create a FederatedStore you need to initialise the store with a
 * graphId and  (if graphId is not known by the {@link uk.gov.gchq.gaffer.store.library.GraphLibrary})
 * the {@link Schema} and {@link StoreProperties}.
 *
 * @see #initialise(String, Schema, StoreProperties)
 * @see Store
 * @see Graph
 */
public class FederatedStore extends Store {
    private FederatedGraphStorage graphStorage = new FederatedGraphStorage();
    private Set<String> customPropertiesAuths;
    private Boolean isPublicAccessAllowed = Boolean.valueOf(IS_PUBLIC_ACCESS_ALLOWED_DEFAULT);

    /**
     * Initialise this FederatedStore with any sub-graphs defined within the
     * properties.
     *
     * @param graphId    the graphId to label this FederatedStore.
     * @param unused     unused
     * @param properties properties to initialise this FederatedStore with, can
     *                   contain details on graphs to add to scope.
     * @throws StoreException if no cache has been set
     */
    @Override
    public void initialise(final String graphId, final Schema unused, final StoreProperties properties) throws StoreException {
        super.initialise(graphId, new Schema(), properties);
        customPropertiesAuths = getCustomPropertiesAuths();
        isPublicAccessAllowed = Boolean.valueOf(getProperties().getIsPublicAccessAllowed());
    }

    @Override
    public void setGraphLibrary(final GraphLibrary library) {
        super.setGraphLibrary(library);
        graphStorage.setGraphLibrary(library);
    }

    /**
     * Get this Store's {@link uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties}.
     *
     * @return the instance of {@link uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties},
     * this may contain details such as database connection details.
     */
    @Override
    public FederatedStoreProperties getProperties() {
        return (FederatedStoreProperties) super.getProperties();
    }

    /**
     * <p>
     * Within FederatedStore an {@link Operation} is executed against a
     * collection of many graphs.
     * </p>
     * <p>
     * Problem: When an Operation contains View information about an Element
     * which is not known by the Graph; It will fail validation when executed.
     * </p>
     * <p>
     * Solution: For each operation, remove all elements from the View that is
     * unknown to the graph.
     * </p>
     *
     * @param operation current operation
     * @param graph     current graph
     * @param <OP>      Operation type
     * @return cloned operation with modified View for the given graph.
     * @deprecated see {@link FederatedStoreUtil#updateOperationForGraph(Operation, Graph)}
     */
    @Deprecated
    public static <OP extends Operation> OP updateOperationForGraph(final OP operation, final Graph graph) {
        return FederatedStoreUtil.updateOperationForGraph(operation, graph);
    }

    /**
     * Adds graphs to the scope of FederatedStore.
     * <p>
     * To be used by the FederatedStore and Handlers only. Users should add
     * graphs via the {@link AddGraph} operation.
     * public access will be ignored if the FederatedStore denies this action
     * at initialisation, will default to usual access with addingUserId and
     * graphAuths
     * </p>
     *
     * @param addingUserId the adding userId
     * @param graphs       the graph to add
     * @param isPublic     if this class should have public access.
     * @param graphAuths   the access auths for the graph being added
     * @throws StorageException if unable to put graph into storage
     */
    public void addGraphs(final Set<String> graphAuths, final String addingUserId, final boolean isPublic, final Graph... graphs) throws StorageException {
        FederatedAccess access = new FederatedAccess(graphAuths, addingUserId, isPublicAccessAllowed && isPublic);

        addGraphs(access, graphs);
    }

    public void addGraphs(final FederatedAccess access, final Graph... graphs) throws StorageException {
        for (final Graph graph : graphs) {
            _add(graph, access);
        }
    }

    @Deprecated
    public void addGraphs(final Set<String> graphAuths, final String addingUserId, final Graph... graphs) throws StorageException {
        addGraphs(graphAuths, addingUserId, false, graphs);
    }

    /**
     * <p>
     * Removes graphs from the scope of FederatedStore.
     * </p>
     * <p>
     * To be used by the FederatedStore and Handlers only. Users should remove
     * graphs via the {@link RemoveGraph} operation.
     * </p>
     *
     * @param graphId to be removed from scope
     * @param user    to match visibility against
     */
    public void remove(final String graphId, final User user) {
        graphStorage.remove(graphId, user);
    }

    /**
     * @param user the visibility to use for getting graphIds
     * @return All the graphId(s) within scope of this FederatedStore and within
     * visibility for the given user.
     */
    public Collection<String> getAllGraphIds(final User user) {
        return graphStorage.getAllIds(user);
    }

    @Override
    public Schema getSchema() {
        return getSchema((Map<String, String>) null, (User) null);
    }

    public Schema getSchema(final GetSchema operation, final Context context) {
        if (null == operation) {
            return getSchema((Map<String, String>) null, context);
        }

        return graphStorage.getSchema(operation, context);
    }

    public Schema getSchema(final Operation operation, final Context context) {
        if (null == operation) {
            return getSchema((Map<String, String>) null, context);
        }

        return getSchema(operation.getOptions(), context);
    }

    public Schema getSchema(final Map<String, String> config, final Context context) {
        return graphStorage.getSchema(config, context);
    }

    public Schema getSchema(final Operation operation, final User user) {
        if (null == operation) {
            return getSchema((Map<String, String>) null, user);
        }

        return getSchema(operation.getOptions(), user);
    }

    public Schema getSchema(final Map<String, String> config, final User user) {
        return graphStorage.getSchema(config, user);
    }

    /**
     * @return {@link Store#getTraits()}
     */
    @Override
    public Set<StoreTrait> getTraits() {
        return StoreTrait.ALL_TRAITS;
    }

    /**
     * <p>
     * Gets a collection of graph objects within FederatedStore scope from the
     * given csv of graphIds, with visibility of the given user.
     * </p>
     * <p>
     * if graphIdsCsv is null then all graph objects within FederatedStore
     * scope are returned.
     * </p>
     *
     * @param user        the users scope to get graphs for.
     * @param graphIdsCsv the csv of graphIds to get, null returns all graphs.
     * @return the graph collection.
     */
    public Collection<Graph> getGraphs(final User user, final String graphIdsCsv) {
        return graphStorage.get(user, getCleanStrings(graphIdsCsv));
    }

    /**
     * The FederatedStore at time of initialisation, can set the auths required
     * to allow users to use custom {@link StoreProperties} outside the
     * scope of the {@link uk.gov.gchq.gaffer.store.library.GraphLibrary}.
     *
     * @param user the user needing validation for custom property usage.
     * @return boolean permission
     */
    public boolean isLimitedToLibraryProperties(final User user) {
        return (null != this.customPropertiesAuths) && Collections.disjoint(user.getOpAuths(), this.customPropertiesAuths);
    }

    @Override
    protected Class<FederatedStoreProperties> getPropertiesClass() {
        return FederatedStoreProperties.class;
    }

    @Override
    protected void addAdditionalOperationHandlers() {
        // Override the Operations that don't have an output
        getSupportedOperations()
                .stream()
                .filter(op -> !Output.class.isAssignableFrom(op) && !AddElements.class.equals(op))
                .forEach(op -> addOperationHandler(op, new FederatedOperationHandler()));

        addOperationHandler(GetSchema.class, new FederatedGetSchemaHandler());

        addOperationHandler(Filter.class, new FederatedFilterHandler());
        addOperationHandler(Aggregate.class, new FederatedAggregateHandler());
        addOperationHandler(Transform.class, new FederatedTransformHandler());

        addOperationHandler(Validate.class, new FederatedValidateHandler());

        addOperationHandler(GetAllGraphIds.class, new FederatedGetAllGraphIDHandler());
        addOperationHandler(AddGraph.class, new FederatedAddGraphHandler());
        addOperationHandler(RemoveGraph.class, new FederatedRemoveGraphHandler());

        addOperationHandler(FederatedOperationChain.class, new FederatedOperationChainHandler());
    }

    @Override
    protected OperationChainValidator createOperationChainValidator() {
        return new FederatedOperationChainValidator(new ViewValidator());
    }

    @Override
    protected OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> getGetElementsHandler() {
        return new FederatedGetElementsHandler();
    }

    @Override
    protected OutputOperationHandler<GetAllElements, CloseableIterable<? extends Element>> getGetAllElementsHandler() {
        return new FederatedGetAllElementsHandler();
    }

    @Override
    protected OutputOperationHandler<? extends GetAdjacentIds, CloseableIterable<? extends EntityId>> getAdjacentIdsHandler() {
        return new FederatedGetAdjacentIdsHandler();
    }

    @Override
    protected OperationHandler<? extends AddElements> getAddElementsHandler() {
        return (OperationHandler) new FederatedOperationHandler();
    }

    @Override
    protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
        return Serialiser.class;
    }

    @Override
    protected Object doUnhandledOperation(final Operation operation,
                                          final Context context) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void startCacheServiceLoader(final StoreProperties properties) {
        super.startCacheServiceLoader(properties);
        try {
            graphStorage.startCacheServiceLoader();
        } catch (final StorageException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private Set<String> getCustomPropertiesAuths() {
        final String value = getProperties().getCustomPropsValue();
        return (Strings.isNullOrEmpty(value)) ? null : Sets.newHashSet(getCleanStrings(value));
    }

    private void _add(final Graph newGraph, final FederatedAccess access) throws StorageException {
        graphStorage.put(newGraph, access);
    }
}
