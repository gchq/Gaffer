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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties.GraphConfigEnum;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties.LocationEnum;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedOperationAddElementsHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedOperationHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedAddGraphHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetAdjacentIdsHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetAllElementsHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetAllGraphIDHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetElementsHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedRemoveGraphHandler;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.Graph.Builder;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
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
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties.IS_PUBLIC_ACCESS_ALLOWED_DEFAULT;

/**
 * A Store that encapsulates a collection of sub-graphs and executes operations
 * against them and returns results as though it was a single graph.
 * <p>
 * To create a FederatedStore you need to initialise the store with a
 * graphId and  (if graphId is not known by the {@link uk.gov.gchq.gaffer.store.library.GraphLibrary})
 * the
 * {@link Schema} and {@link StoreProperties}.
 *
 * @see #initialise(String, Schema, StoreProperties)
 * @see Store
 * @see Graph
 */
public class FederatedStore extends Store {
    public static final String ERROR_ADDING_GRAPH_TO_CACHE = "Error adding graph, GraphId is known within the cache, but %s is different. GraphId: %s";
    protected static final String S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2 = "%s was not able to be created with the supplied properties.%n%s";
    private static final String SCHEMA_DEL_REGEX = Pattern.quote(",");
    private static final GraphConfigEnum SCHEMA = GraphConfigEnum.SCHEMA;
    private static final GraphConfigEnum PROPERTIES = GraphConfigEnum.PROPERTIES;
    private static final LocationEnum ID = LocationEnum.ID;
    private static final LocationEnum FILE = LocationEnum.FILE;
    private FederatedStoreCache federatedStoreCache = new FederatedStoreCache();
    private FederatedGraphStorage graphStorage = new FederatedGraphStorage();
    private Set<String> customPropertiesAuths;
    private Boolean isPublicAccessAllowed = Boolean.valueOf(IS_PUBLIC_ACCESS_ALLOWED_DEFAULT);
    private Boolean isCacheEnabled = false;
    private boolean isInitialised = false;

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
        isInitialised = true;
        setProperties(properties);
        validateCacheProperties();
        super.initialise(graphId, new Schema(), properties);
        loadCustomPropertiesAuths();

        isPublicAccessAllowed = Boolean.valueOf(getProperties().getIsPublicAccessAllowed());

        loadGraphs();
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
     * Within FederatedStore an {@link Operation} is executed against a
     * collection of many graphs.
     * <p>
     * Problem: When an Operation contains View information about an Element
     * which is not known by the Graph; It will fail validation when executed.
     * <p>
     * Solution: For each operation, remove all elements from the View that is
     * unknown to the graph.
     *
     * @param operation current operation
     * @param graph     current graph
     * @param <OP>      Operation type
     * @return cloned operation with modified View for the given graph.
     */
    public static <OP extends Operation> OP updateOperationForGraph(final OP operation, final Graph graph) {
        OP resultOp = operation;

        if (operation instanceof OperationView) {
            final View view = ((OperationView) operation).getView();
            if (null != view && view.hasGroups()) {
                resultOp = (OP) operation.shallowClone();
                final View validView = createValidView(view, graph.getSchema());
                if (validView.hasGroups()) {
                    ((OperationView) resultOp).setView(validView);
                } else {
                    resultOp = null;
                }
            }
        } else if (operation instanceof AddElements) {
            resultOp = (OP) operation.shallowClone();
        }

        return resultOp;
    }

    /**
     * Adds graphs to the scope of FederatedStore.
     * <p>
     * To be used by the FederatedStore and Handlers only. Users should add
     * graphs via the {@link AddGraph} operation.
     * public access will be ignored if the FederatedStore denies this action
     * at initialisation, will default to usual access with addingUserId and
     * graphAuths
     *
     * @param addingUserId the adding userId
     * @param graphs       the graph to add
     * @param isPublic     if this class should have public access.
     * @param graphAuths   the access auths for the graph being added
     */

    public void addGraphs(final Set<String> graphAuths, final String addingUserId, final boolean isPublic, final Graph... graphs) {
        FederatedAccess access = new FederatedAccess(graphAuths, addingUserId, isPublicAccessAllowed && isPublic);

        addGraphs(access, graphs);
    }

    public void addGraphs(final FederatedAccess access, final Graph... graphs) {
        if (isCacheEnabled || !isInitialised) {
            validateCache();
        }

        for (final Graph graph : graphs) {
            _add(graph, access);
        }
    }

    @Deprecated
    public void addGraphs(final Set<String> graphAuths, final String addingUserId, final Graph... graphs) {
        addGraphs(graphAuths, addingUserId, false, graphs);
    }

    /**
     * Removes graphs from the scope of FederatedStore.
     * <p>
     * To be used by the FederatedStore and Handlers only. Users should remove
     * graphs via the {@link RemoveGraph} operation.
     *
     * @param graphId to be removed from scope
     * @param user    to match visibility against
     */
    public void remove(final String graphId, final User user) {
        if (isCacheEnabled) {
            validateCache();
            federatedStoreCache.deleteFromCache(graphId);
        }
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

    /**
     * @return {@link Store#getTraits()}
     */
    @Override
    public Set<StoreTrait> getTraits() {
        return graphStorage.getTraits();
    }

    /**
     * Gets a collection of graph objects within FederatedStore scope from the
     * given csv of graphIds, with visibility of the given user.
     * <p>
     * if graphIdsCsv is null then all graph objects within FederatedStore
     * scope are returned.
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

        addOperationHandler(GetAllGraphIds.class, new FederatedGetAllGraphIDHandler());
        addOperationHandler(AddGraph.class, new FederatedAddGraphHandler());
        addOperationHandler(RemoveGraph.class, new FederatedRemoveGraphHandler());
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
        return new FederatedOperationAddElementsHandler();
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
        if (isCacheEnabled) {
            if (federatedStoreCache.getCache() == null) {
                CacheServiceLoader.initialise(properties.getProperties());
            }
        }
    }

    @Override
    public Schema getSchema() {
        return graphStorage.getMergedSchema();
    }

    private static View createValidView(final View view, final Schema delegateGraphSchema) {
        View newView;
        if (view.hasGroups()) {
            final View.Builder viewBuilder = new View.Builder().merge(view);
            viewBuilder.entities(new LinkedHashMap<>());
            viewBuilder.edges(new LinkedHashMap<>());

            final Set<String> validEntities = new HashSet<>(view.getEntityGroups());
            final Set<String> validEdges = new HashSet<>(view.getEdgeGroups());
            validEntities.retainAll(delegateGraphSchema.getEntityGroups());
            validEdges.retainAll(delegateGraphSchema.getEdgeGroups());

            for (final String entity : validEntities) {
                viewBuilder.entity(entity, view.getEntity(entity));
            }

            for (final String edge : validEdges) {
                viewBuilder.edge(edge, view.getEdge(edge));
            }

            newView = viewBuilder.build();
        } else {
            newView = view;
        }
        return newView;
    }

    private static List<String> getCleanStrings(final String value) {
        final List<String> values;
        if (value != null) {
            values = Lists.newArrayList(StringUtils.stripAll(value.split(SCHEMA_DEL_REGEX)));
            values.remove("");
            values.remove(null);
        } else {
            values = null;
        }
        return values;
    }

    private void loadCustomPropertiesAuths() {
        final String value = getProperties().getCustomPropsValue();
        if (!Strings.isNullOrEmpty(value)) {
            customPropertiesAuths = Sets.newHashSet(getCleanStrings(value));
        }
    }

    private void loadGraphs() throws StoreException {
        final Set<String> graphIds = getGraphIds();
        for (final String graphId : graphIds) {
            if (isCacheEnabled && federatedStoreCache.contains(graphId)) {
                makeGraphFromCache(graphId);
            } else {
                makeGraphFromProperties(graphId, resolveAuths(graphId), resolveIsPublic(graphId));
            }
        }
        if (isCacheEnabled) {
            makeGraphsRemainingInCache(graphIds);
        }
    }

    private void makeGraphFromCache(final String graphId) {
        Graph graph = federatedStoreCache.getFromCache(graphId);
        final FederatedAccess accessFromCache = federatedStoreCache.getAccessFromCache(graphId);
        addGraphs(accessFromCache, graph);
    }

    private void makeGraphFromProperties(final String graphId, final Set<String> graphAuths, final boolean isPublic) throws StoreException {
        final Builder builder = new Builder()
                .config(new GraphConfig.Builder()
                        .graphId(graphId)
                        .library(getGraphLibrary())
                        .build())
                .addParentSchemaIds(getValueOf(graphId, SCHEMA, ID))
                .parentStorePropertiesId(getValueOf(graphId, PROPERTIES, ID));

        addPropertiesFromFile(graphId, builder);
        addSchemaFromFile(graphId, builder);

        addGraphs(graphAuths, null, isPublic, builder);
    }

    private void makeGraphsRemainingInCache(final Set<String> graphIds) {
        if (isCacheEnabled) {
            final Set<String> allGraphIds = Sets.newHashSet(federatedStoreCache.getAllGraphIds());
            allGraphIds.removeAll(graphIds);
            for (String graphId : allGraphIds) {
                makeGraphFromCache(graphId);
            }
        }
    }

    private boolean resolveIsPublic(final String graphId) {
        return Boolean.valueOf(getProperties().getGraphIsPublicValue(graphId));
    }

    private Set<String> resolveAuths(final String graphId) {
        final String value = getProperties().getGraphAuthsValue(graphId);
        return Strings.isNullOrEmpty(value) ? null : Sets.newHashSet(getCleanStrings(value));
    }

    private void addGraphs(final Set<String> graphAuths, final String addingUserId, final boolean isPublic, final Builder... builders) throws StoreException {
        for (final Builder builder : builders) {
            final Graph graph;
            try {
                graph = builder.build();
            } catch (final Exception e) {
                throw new IllegalArgumentException(String.format(S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2, "Graph", ""), e);
            }
            addGraphs(graphAuths, addingUserId, isPublic, graph);
        }
    }

    private void addSchemaFromFile(final String graphId, final Builder builder) {
        final String schemaFileValue = getValueOf(graphId, SCHEMA, FILE);
        if (!Strings.isNullOrEmpty(schemaFileValue)) {
            List<String> schemas = getCleanStrings(schemaFileValue);
            for (final String schemaPath : schemas) {
                try {
                    if (new File(schemaPath).exists()) {
                        builder.addSchema(Paths.get(schemaPath));
                    } else {
                        builder.addSchema(StreamUtil.openStream(FederatedStore.class, schemaPath));
                    }
                } catch (final Exception e) {
                    throw new IllegalArgumentException(String.format(S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2, "Schema", "GraphId: " + graphId + " schemaPath: " + schemaPath), e);
                }
            }
        }
    }

    private void addPropertiesFromFile(final String graphId, final Builder builder) {
        final String propFileValue = getValueOf(graphId, PROPERTIES, FILE);
        if (!Strings.isNullOrEmpty(propFileValue)) {
            try {
                builder.addStoreProperties(propFileValue);
            } catch (final Exception e) {
                throw new IllegalArgumentException(String.format(S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2, "Property", "GraphId: " + graphId + " propertyPath: " + propFileValue), e);
            }
        }
    }

    private String getValueOf(final String graphId, final GraphConfigEnum graphConfigEnum, final LocationEnum locationEnum) {
        return getProperties().getValueOf(graphId, graphConfigEnum, locationEnum);
    }

    private Set<String> getGraphIds() {
        final HashSet<String> graphIds = Sets.newHashSet();
        final String graphIdValue = getProperties().getGraphIdsValue();
        if (!Strings.isNullOrEmpty(graphIdValue)) {
            graphIds.addAll(getCleanStrings(graphIdValue));
        }
        return graphIds;
    }

    private void _add(final Graph newGraph, final FederatedAccess access) {
        if (isCacheEnabled) {
            addToCache(newGraph, access);
        }

        graphStorage.put(newGraph, access);

        if (null != getGraphLibrary()) {
            getGraphLibrary().add(newGraph.getGraphId(), newGraph.getSchema(), newGraph.getStoreProperties());
        }
    }

    private void addToCache(final Graph newGraph, final FederatedAccess access) {
        final String graphId = newGraph.getGraphId();
        if (federatedStoreCache.contains(graphId)) {
            validateSameAsFromCache(newGraph, graphId);
        } else {
            try {
                federatedStoreCache.addGraphToCache(newGraph, access, false);
            } catch (final OverwritingException e) {
                throw new OverwritingException((String.format("User is attempting to overwrite a graph within the cacheService. GraphId: %s", graphId)));
            } catch (CacheOperationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void validateSameAsFromCache(final Graph newGraph, final String graphId) {
        final Graph fromCache = federatedStoreCache.getFromCache(graphId);
        if (!newGraph.getStoreProperties().getProperties().equals(fromCache.getStoreProperties().getProperties())) {
            throw new RuntimeException(String.format(ERROR_ADDING_GRAPH_TO_CACHE, GraphConfigEnum.PROPERTIES.toString(), graphId));
        } else {
            if (!JsonUtil.equals(newGraph.getSchema().toJson(false), fromCache.getSchema().toJson(false))) {
                throw new RuntimeException(String.format(ERROR_ADDING_GRAPH_TO_CACHE, GraphConfigEnum.SCHEMA.toString(), graphId));
            } else {
                if (!newGraph.getGraphId().equals(fromCache.getGraphId())) {
                    throw new RuntimeException(String.format(ERROR_ADDING_GRAPH_TO_CACHE, "GraphId", graphId));
                }
            }
        }
    }

    private void validateCacheProperties() {
        if (getProperties().getCacheProperties() != null) {
            isCacheEnabled = true;
        }
    }

    private void validateCache() {
        if (federatedStoreCache.getCache() == null) {
            throw new RuntimeException("No cache has been set, please initialise the FederatedStore instance");
        }
    }
}
