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

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties.GraphConfigEnum;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties.LocationEnum;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperationChain;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedAggregateHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedFilterHandler;
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
import uk.gov.gchq.gaffer.graph.Graph.Builder;
import uk.gov.gchq.gaffer.graph.GraphConfig;
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
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.gaffer.user.User;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties.IS_PUBLIC_ACCESS_ALLOWED_DEFAULT;

/**
 * <p>
 * A Store that encapsulates a collection of sub-graphs and executes operations
 * against them and returns results as though it was a single graph.
 * </p>
 * <p>
 * To create a FederatedStore you need to initialise the store with a
 * graphId and  (if graphId is not known by the {@link GraphLibrary}) the
 * {@link
 * Schema} and  {@link StoreProperties}.
 * </p>
 *
 * @see #initialise(String, Schema, StoreProperties)
 * @see Store
 * @see Graph
 */
public class FederatedStore extends Store {
    protected static final String S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2 = "%s was not able to be created with the supplied properties.%n%s";
    public static final GraphConfigEnum SCHEMA = GraphConfigEnum.SCHEMA;
    public static final GraphConfigEnum PROPERTIES = GraphConfigEnum.PROPERTIES;
    public static final LocationEnum ID = LocationEnum.ID;
    public static final LocationEnum FILE = LocationEnum.FILE;

    private FederatedGraphStorage graphStorage = new FederatedGraphStorage();
    private Set<String> customPropertiesAuths;
    public Boolean isPublicAccessAllowed = Boolean.valueOf(IS_PUBLIC_ACCESS_ALLOWED_DEFAULT);

    /**
     * Initialise this FederatedStore with any sub-graphs defined within the
     * properties.
     *
     * @param graphId    the graphId to label this FederatedStore.
     * @param unused     unused
     * @param properties properties to initialise this FederatedStore with, can
     *                   contain details on graphs to add to scope.
     * @throws StoreException exception
     */
    @Override
    public void initialise(final String graphId, final Schema unused, final StoreProperties properties) throws StoreException {
        super.initialise(graphId, new Schema(), properties);
        loadCustomPropertiesAuths();

        isPublicAccessAllowed = Boolean.valueOf(getProperties().getIsPublicAccessAllowed());

        loadGraphs();
    }

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
     * at
     * initialisation, will default to usual access with addingUserId and
     * graphAuths
     * </p>
     *
     * @param addingUserId the adding userId
     * @param graphs       the graph to add
     * @param isPublic     if this class should have public access.
     * @param graphAuths   the access auths for the graph being added
     */
    public void addGraphs(final Set<String> graphAuths, final String addingUserId, final boolean isPublic, final Graph... graphs) {
        FederatedAccess access = new FederatedAccess(graphAuths, addingUserId, isPublicAccessAllowed && isPublic);

        for (final Graph graph : graphs) {
            _add(graph, access);
        }
    }

    public void addGraphs(final Set<String> graphAuths, final String addingUserId, final Graph... graphs) {
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
        return graphStorage.get(user, FederatedStoreUtil.getCleanStrings(graphIdsCsv));
    }

    /**
     * The FederatedStore at time of initialisation, can set the auths required
     * to allow users to use custom {@link StoreProperties} outside the
     * scope of the {@link GraphLibrary}.
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

    private void loadCustomPropertiesAuths() {
        final String value = getProperties().getCustomPropsValue();
        if (!Strings.isNullOrEmpty(value)) {
            customPropertiesAuths = Sets.newHashSet(FederatedStoreUtil.getCleanStrings(value));
        }
    }

    private void loadGraphs() {
        final HashSet<String> graphIds = getGraphIds();
        for (final String graphId : graphIds) {

            final Builder builder = new Builder().config(new GraphConfig.Builder()
                    .graphId(graphId)
                    .library(getGraphLibrary())
                    .build());

            resolveConfiguration(graphId, builder);

            final Set<String> auths = resolveAuths(graphId);
            final boolean isPublic = resolveIsPublic(graphId);
            addGraphs(auths, null, isPublic, builder);
        }
    }

    private boolean resolveIsPublic(final String graphId) {
        return Boolean.valueOf(getProperties().getGraphIsPublicValue(graphId));
    }

    private void resolveConfiguration(final String graphId, final Builder builder) {
        resolveSchema(graphId, builder);

        resolveProperties(graphId, builder);
    }

    private Set<String> resolveAuths(final String graphId) {
        final String value = getProperties().getGraphAuthsValue(graphId);
        return Strings.isNullOrEmpty(value) ? null : Sets.newHashSet(FederatedStoreUtil.getCleanStrings(value));
    }

    private void resolveProperties(final String graphId, final Builder builder) {
        addPropertiesFromLibrary(graphId, builder);

        //this method is allowed to override properties from file
        addPropertiesFromFile(graphId, builder);
    }

    private void resolveSchema(final String graphId, final Builder builder) {
        addSchemaFromLibrary(graphId, builder);

        //this method is allowed to override schema from file
        addSchemaFromFile(graphId, builder);
    }

    private void addGraphs(final Set<String> graphAuths, final String userId, final boolean isPublic, final Builder... builders) {
        for (final Builder builder : builders) {
            final Graph graph;
            try {
                graph = builder.build();
            } catch (final Exception e) {
                throw new IllegalArgumentException(String.format(S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2, "Graph", ""), e);
            }
            addGraphs(graphAuths, userId, isPublic, graph);
        }
    }

    private void addSchemaFromLibrary(final String graphId, final Builder builder) {
        final String schemaIdValue = getValueOf(graphId, SCHEMA, ID);
        if (!Strings.isNullOrEmpty(schemaIdValue)) {
            final GraphLibrary graphLibrary = getGraphLibrary();
            if (null != graphLibrary) {
                try {
                    builder.addSchema(graphLibrary.getSchema(schemaIdValue));
                } catch (final Exception e) {
                    throw new IllegalArgumentException(String.format(S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2, "Schema", "graphId: " + graphId + " schemaId: " + schemaIdValue), e);
                }
            }
        }
    }

    private void addSchemaFromFile(final String graphId, final Builder builder) {
        final String schemaFileValue = getValueOf(graphId, SCHEMA, FILE);
        if (!Strings.isNullOrEmpty(schemaFileValue)) {
            List<String> schemas = FederatedStoreUtil.getCleanStrings(schemaFileValue);
            for (final String schemaPath : schemas) {
                try {
                    if (new File(schemaPath).exists()) {
                        builder.addSchema(Paths.get(schemaPath));
                    } else {
                        builder.addSchema(StreamUtil.openStream(FederatedStore.class, schemaPath));
                    }
                } catch (final Exception e) {
                    throw new IllegalArgumentException(String.format(S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2, "Schema", "graphId: " + graphId + " schemaPath: " + schemaPath), e);
                }
            }
        }
    }

    private void addPropertiesFromLibrary(final String graphId, final Builder builder) {
        final String propIdValue = getValueOf(graphId, PROPERTIES, ID);
        if (!Strings.isNullOrEmpty(propIdValue)) {
            try {
                builder.addStoreProperties(getGraphLibrary().getProperties(propIdValue));
            } catch (final Exception e) {
                throw new IllegalArgumentException(String.format(S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2, "Property", "graphId: " + graphId + " propertyId: " + propIdValue), e);
            }
        }
    }

    private void addPropertiesFromFile(final String graphId, final Builder builder) {
        final String propFileValue = getValueOf(graphId, PROPERTIES, FILE);
        if (!Strings.isNullOrEmpty(propFileValue)) {
            try {
                builder.addStoreProperties(propFileValue);
            } catch (final Exception e) {
                throw new IllegalArgumentException(String.format(S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2, "Property", "graphId: " + graphId + " propertyPath: " + propFileValue), e);
            }
        }
    }

    private String getValueOf(final String graphId, final GraphConfigEnum graphConfigEnum, final LocationEnum locationEnum) {
        return getProperties().getValueOf(graphId, graphConfigEnum, locationEnum);
    }

    private HashSet<String> getGraphIds() {
        final HashSet<String> graphIds = Sets.newHashSet();
        final String graphIdValue = getProperties().getGraphIdsValue();
        if (!Strings.isNullOrEmpty(graphIdValue)) {
            graphIds.addAll(FederatedStoreUtil.getCleanStrings(graphIdValue));
        }
        return graphIds;
    }

    private void _add(final Graph newGraph, final FederatedAccess access) {
        graphStorage.put(newGraph, access);

        if (null != getGraphLibrary()) {
            getGraphLibrary().add(newGraph.getGraphId(), newGraph.getSchema(), newGraph.getStoreProperties());
        }
    }
}
