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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedOperationAddElementsHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedOperationHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedAddGraphHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetAdjacentIdsHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetAllElementsHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetElementsHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedRemoveGraphHandler;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.Graph.Builder;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
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
import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

public class FederatedStore extends Store {
    public static final String GAFFER_FEDERATEDSTORE = "gaffer.federatedstore.";
    public static final String PROPERTIES = "properties";
    public static final String SCHEMA = "schema";
    public static final String SCHEMA_DEL_REGEX = Pattern.quote(",");
    public static final String KEY_DEL_REGEX = Pattern.quote(".");
    public static final String USER_IS_ATTEMPTING_TO_OVERWRITE_A_GRAPH_WITHIN_FEDERATED_STORE_GRAPH_ID_S = "User is attempting to overwrite a graph within FederatedStore. GraphId: %s";
    public static final String GRAPH_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_USER_SUPPLIED_PROPERTIES_GRAPH_ID_S = "Graph was not able to be created with the user supplied properties. GraphId: %s";
    private final Map<String, Graph> graphs = Maps.newHashMap();
    private Schema schema = new Schema();
    private Set<StoreTrait> traits = new HashSet<>();
    public static final JSONSerialiser JSON_SERIALISER = new JSONSerialiser();

    @Override
    public void initialise(final String graphId, final Schema unused, final StoreProperties properties) throws StoreException {
        super.initialise(graphId, new Schema(), properties);
        loadGraphs();
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    public void add(final Graph... graphs) {
        for (final Graph graph : graphs) {
            _add(graph);
        }
        updateMergedGraphConfig();
    }

    public void add(final Collection<Graph> graphs) {
        graphs.forEach(this::_add);
        updateMergedGraphConfig();
    }

    private void _add(final Graph graph) {
        if (graphs.containsKey(graph.getGraphId())) {
            throw new IllegalStateException((String.format(USER_IS_ATTEMPTING_TO_OVERWRITE_A_GRAPH_WITHIN_FEDERATED_STORE_GRAPH_ID_S, graph.getGraphId())));
        }
        graphs.put(graph.getGraphId(), graph);
    }

    public void remove(final String graphId) {
        graphs.remove(graphId);
        updateMergedGraphConfig();
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

    public static View createValidView(final View view, final Schema delegateGraphSchema) {
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

    private void loadGraphs() {
        //val = graphID: (propertyValue, schemaValue)
        Map<String, Graph.Builder> graphIdData = getGraphIdData(getProperties());
        add(getGraphsFrom(graphIdData.values()));
        updateMergedGraphConfig();
    }

    private ArrayList<Graph> getGraphsFrom(final Iterable<Graph.Builder> graphIdData) {
        ArrayList<Graph> graphs = Lists.newArrayList();
        for (final Graph.Builder builder : graphIdData) {
            try {
                graphs.add(builder.build());
            } catch (final Exception e) {
                throw new IllegalArgumentException(String.format(GRAPH_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_USER_SUPPLIED_PROPERTIES_GRAPH_ID_S, builder.getGraphId()), e);
            }
        }

        return graphs;
    }

    private Map<String, Graph.Builder> getGraphIdData(final StoreProperties properties) {
        Map<String, Graph.Builder> graphIdData = Maps.newHashMap();

        for (final Entry<Object, Object> entry : properties.getProperties().entrySet()) {
            String[] splitKey = splitKey(entry.getKey().toString());
            if (validateSplitKey(splitKey)) {
                String graphId = splitKey[0];
                String graphIdOption = splitKey[1];
                String value = entry.getValue().toString();
                switch (graphIdOption) {
                    case PROPERTIES:
                        addPropertiesValue(graphIdData, graphId, value);
                        break;
                    case SCHEMA:
                        addSchemaValue(graphIdData, graphId, value);
                        break;
                    default:
                        break;
                }
            }
        }
        return graphIdData;
    }

    private void addSchemaValue(final Map<String, Graph.Builder> graphIdData,
                                final String graphId,
                                final String value) {

        Graph.Builder graphBuilder = graphIdData.get(graphId);
        if (graphBuilder == null) {
            graphBuilder = new Builder().graphId(graphId);
        }

        List<String> schemas = Lists.newArrayList(Arrays.asList(StringUtils.stripAll(value.split(SCHEMA_DEL_REGEX))));
        schemas.remove("");
        schemas.remove(null);
        for (final String schemaPath : schemas) {
            try {
                if (new File(schemaPath).exists()) {
                    graphBuilder.addSchema(Paths.get(schemaPath));
                } else {
                    graphBuilder.addSchema(StreamUtil.openStream(FederatedStore.class, schemaPath));
                }
            } catch (final Exception e) {
                throw new IllegalArgumentException(String.format(GRAPH_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_USER_SUPPLIED_PROPERTIES_GRAPH_ID_S, graphId), e);
            }
        }

        graphIdData.put(graphId, graphBuilder);
    }

    private void addPropertiesValue(final Map<String, Graph.Builder> graphIdData,
                                    final String graphId,
                                    final String value) {

        Graph.Builder graphBuilder = graphIdData.get(graphId);
        if (graphBuilder == null) {
            graphBuilder = new Graph.Builder().graphId(graphId);
        }
        try {
            graphBuilder.storeProperties(value);
        } catch (final Exception e) {
            throw new IllegalArgumentException(String.format(GRAPH_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_USER_SUPPLIED_PROPERTIES_GRAPH_ID_S, graphId), e);
        }
        graphIdData.put(graphId, graphBuilder);
    }

    private String[] splitKey(final String key) {
        return key.replace(GAFFER_FEDERATEDSTORE, "").split(KEY_DEL_REGEX);
    }

    private boolean validateSplitKey(final String[] splitKey) {
        return splitKey.length == 2 && splitKey[0].length() > 0 && splitKey[1].length() > 0;
    }

    @Override
    public Set<StoreTrait> getTraits() {
        return traits;
    }

    public Collection<Graph> getGraphs() {
        return graphs.values();
    }

    @Override
    protected void addAdditionalOperationHandlers() {
        // Override the Operations that don't have an output
        getSupportedOperations()
                .stream()
                .filter(op -> !Output.class.isAssignableFrom(op) && !AddElements.class.equals(op))
                .forEach(op -> addOperationHandler(op, new FederatedOperationHandler()));

        // Override the Output operations
//        addOperationHandler(Min.class, new FederatedMinHandler());
//        addOperationHandler(Max.class, new FederatedMaxHandler());
//        addOperationHandler(Sort.class, new FederatedSortHandler());

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
}
