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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphID;
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
import uk.gov.gchq.gaffer.store.exception.OverwritingException;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A Store that encapsulates a collection of sub-graphs
 * and executes operations against them and returns
 * results as though it was a single graph.
 * <p>
 * To create a FederatedStore you need to initialise the store with a graph name and a properties file.
 * <p>
 * To load a graph into the FederatedStore you need to provide either.
 * <ul>
 * <li>GraphID
 * <li>Graph Schema
 * <li>Graph Properties file
 * </ul>
 * or
 * <ul>
 * <li>GraphID
 * <li>GraphLibrary (containing GraphId)
 * </ul>
 * <p>
 * To remove a graph from FederatedStore scope you just need the GraphID.
 *
 * @see Store
 * @see Graph
 * @see StoreProperties
 * @see Schema
 */
public class FederatedStore extends Store {
    public static final String USER_IS_ATTEMPTING_TO_OVERWRITE_A_GRAPH_WITHIN_FEDERATED_STORE_GRAPH_ID_S = "User is attempting to overwrite a graph within FederatedStore. GraphId: %s";
    protected static final String S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2 = "%s was not able to be created with the supplied properties.%n%s";
    private static final String GAFFER_FEDERATED_STORE = "gaffer.federatedstore.";
    private static final String GRAPH_IDS = "graphIds";
    private static final String SCHEMA_DEL_REGEX = Pattern.quote(",");
    private static final String DOT = ".";
    private static final String FILE = "file";
    private static final String SCHEMA = "schema";
    private static final String PROPERTIES = "properties";
    private static final String ID = "id";

    private final Map<String, Graph> graphs = Maps.newHashMap();
    private Set<StoreTrait> traits = new HashSet<>();

    private static List<String> getCleanStrings(final String value) {
        final List<String> values = Arrays.asList(StringUtils.stripAll(value.split(SCHEMA_DEL_REGEX)));
        values.remove("");
        values.remove(null);
        return values;
    }

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

    /**
     * Initialise this FederatedStore with any sub-graphs defined within properties
     *
     * @param graphId      the graphId to label this FederatedStore.
     * @param properties   properties to initialise this FederatedStore, can contain details on graphs to add to scope.
     * @param graphLibrary the source for extracting sub-graphs from.
     * @throws StoreException exception
     */
    public void initialise(final String graphId, final StoreProperties properties, final GraphLibrary graphLibrary) throws StoreException {
        setGraphLibrary(graphLibrary);
        this.initialise(graphId, properties);
    }

    /**
     * Initialise this FederatedStore with any sub-graphs defined within properties.
     *
     * @param graphId    the graphId to label this FederatedStore.
     * @param properties properties to initialise this FederatedStore, can contain details on graphs to add to scope.
     * @throws StoreException exception
     * @see #initialise(String, Schema, StoreProperties)
     */
    public void initialise(final String graphId, final StoreProperties properties) throws StoreException {
        this.initialise(graphId, null, properties);
    }

    /**
     * Initialise this FederatedStore with any sub-graphs defined within properties.
     *
     * @param graphId    the graphId to label this FederatedStore.
     * @param unused     unused
     * @param properties properties to initialise this FederatedStore with, can contain details on graphs to add to scope.
     * @throws StoreException exception
     */
    @Override
    public void initialise(final String graphId, final Schema unused, final StoreProperties properties) throws StoreException {
        super.initialise(graphId, new Schema(), properties);
        loadGraphs();
    }

    private void loadGraphs() {
        final HashSet<String> graphIds = getGraphIds();
        for (final String graphId : graphIds) {

            final Builder builder = makeBuilder(graphId);

            final boolean isFoundInLibrary = addConfigFromLibrary(graphId, builder);
            if (isFoundInLibrary) {
                final Builder overriddenBuilder = makeBuilder(graphId);
                final String before = overriddenBuilder.toString();

                resolveSchemaAndProps(graphId, overriddenBuilder);

                final String overridden = overriddenBuilder.toString();
                final String original = builder.toString();
                if (!overridden.equals(before) && !original.equals(overridden)) {
                    throw new IllegalArgumentException(String.format("User is attempting to override a known graph in library.%nOriginal: %s%nOverridden: %s", original, overridden));
                }
            } else {
                resolveSchemaAndProps(graphId, builder);
            }
            addGraphs(builder);
        }
    }

    private void resolveSchemaAndProps(final String graphId, final Builder builder) {
        resolveSchema(graphId, builder);

        resolveProperties(graphId, builder);
    }

    private void resolveProperties(final String graphId, final Builder builder) {
        addPropertiesFromLibrary(graphId, builder);

        //may override with properties from from
        addPropertiesFromFile(graphId, builder);
    }

    private void resolveSchema(final String graphId, final Builder builder) {
        addSchemaFromLibrary(graphId, builder);

        //may override with schema from file
        addSchemaFromFile(graphId, builder);
    }

    private Builder makeBuilder(final String graphId) {
        return new Builder().config(new GraphConfig.Builder()
                .graphId(graphId)
                .library(getGraphLibrary())
                .build());
    }

    public void addGraphs(final Builder... builders) {
        for (final Builder builder : builders) {
            final Graph build;
            try {
                build = builder.build();
            } catch (final Exception e) {
                throw new IllegalArgumentException(String.format(S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2, "Graph", builder.toString()), e);
            }
            addGraphs(build);
        }
    }

    private boolean addConfigFromLibrary(final String graphId, final Builder builder) {
        boolean rtn = false;
        final GraphLibrary graphLibrary = getGraphLibrary();
        if (graphLibrary != null) {
            final Pair<Schema, StoreProperties> pair = graphLibrary.get(graphId);
            if (pair != null) {
                builder.addSchema(pair.getFirst())
                        .addStoreProperties(pair.getSecond());
                rtn = true;
            }
        }
        return rtn;
    }

    private void addSchemaFromLibrary(final String graphId, final Builder builder) {
        final String schemaIdValue = getValueOf(graphId, SCHEMA, ID);
        if (!Strings.isNullOrEmpty(schemaIdValue)) {
            final GraphLibrary graphLibrary = getGraphLibrary();
            if (graphLibrary != null) {
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
            List<String> schemas = getCleanStrings(schemaFileValue);
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

    private String getValueOf(final String graphId, final String properties, final String location) {
        final String key = GAFFER_FEDERATED_STORE + graphId + DOT + properties + DOT + location;
        return getProperties().get(key);
    }

    private HashSet<String> getGraphIds() {
        final HashSet<String> graphIds = Sets.newHashSet();
        final String idKey = GAFFER_FEDERATED_STORE + GRAPH_IDS;
        final String graphIDValue = getProperties().get(idKey);
        if (!Strings.isNullOrEmpty(graphIDValue)) {
            final List<String> graphIDs = getCleanStrings(graphIDValue);
            graphIds.addAll(graphIDs);
        }
        return graphIds;
    }

    public void addGraphs(final Graph... graphs) {
        for (final Graph graph : graphs) {
            _add(graph);
        }
        updateMergedGraphConfig();
    }

    private void _add(final Graph graph) {
        final String graphId = graph.getGraphId();
        if (graphs.containsKey(graphId)) {
            throw new OverwritingException((String.format(USER_IS_ATTEMPTING_TO_OVERWRITE_A_GRAPH_WITHIN_FEDERATED_STORE_GRAPH_ID_S, graphId)));
        }
        graphs.put(graphId, graph);
    }

    public void addGraphs(final String... graphId) {
        for (final String id : graphId) {
            final Builder builder = new Builder().config(new GraphConfig.Builder()
                    .graphId(id)
                    .library(getGraphLibrary())
                    .build());
            addConfigFromLibrary(id, builder);
            addGraphs(builder);
        }
    }

    private void updateMergedGraphConfig() {
        Schema.Builder schemaBuilder = new Schema.Builder();
        final Set<StoreTrait> newTraits = Sets.newHashSet(StoreTrait.values());
        for (final Graph graph : graphs.values()) {
            schemaBuilder = schemaBuilder.merge(graph.getSchema());
            newTraits.retainAll(graph.getStoreTraits());
        }

        schema = schemaBuilder.build();
        traits = Collections.unmodifiableSet(newTraits);
    }

    @Override
    protected void addAdditionalOperationHandlers() {
        // Override the Operations that don't have an output
        getSupportedOperations()
                .stream()
                .filter(op -> !Output.class.isAssignableFrom(op) && !AddElements.class.equals(op))
                .forEach(op -> addOperationHandler(op, new FederatedOperationHandler()));

        addOperationHandler(GetAllGraphID.class, new FederatedGetAllGraphIDHandler());
        addOperationHandler(AddGraph.class, new FederatedAddGraphHandler());
        addOperationHandler(RemoveGraph.class, new FederatedRemoveGraphHandler());
    }

    public void remove(final String graphId) {
        graphs.remove(graphId);
        updateMergedGraphConfig();
    }


    public Set<String> getAllGraphIds() {
        return graphs.keySet();
    }

    @Override
    public Set<StoreTrait> getTraits() {
        return traits;
    }

    public Collection<Graph> getGraphs() {
        return graphs.values();
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

    public Collection<Graph> getGraphs(final String[] split) {
        Map<String, Graph> filteredGraphs = Maps.newHashMap();
        if (null != split && split.length > 0) {
            for (final String s : split) {
                if (graphs.containsKey(s)) {
                    filteredGraphs.put(s, graphs.get(s));
                }
            }
        } else {
            filteredGraphs = graphs;
        }
        return filteredGraphs.values();
    }
}
