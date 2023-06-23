/*
 * Copyright 2016-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.service.ServiceRegistry;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationChain.Builder;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.tinkerpop.generator.GafferEdgeGenerator;
import uk.gov.gchq.gaffer.tinkerpop.generator.GafferEntityGenerator;
import uk.gov.gchq.gaffer.tinkerpop.generator.GafferPopEdgeGenerator;
import uk.gov.gchq.gaffer.tinkerpop.generator.GafferPopVertexGenerator;
import uk.gov.gchq.gaffer.tinkerpop.service.GafferPopNamedOperationServiceFactory;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.iterable.ChainedIterable;
import uk.gov.gchq.koryphe.iterable.MappedIterable;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A <code>GafferPopGraph</code> is an implementation of
 * {@link org.apache.tinkerpop.gremlin.structure.Graph}.
 * It wraps a Gaffer {@link Graph} and delegates all operations to it.
 * In addition to the tinkerpop methods required there are methods to add edges
 * query for adjacent vertices and to provide a {@link View} to filter out results.
 */
public class GafferPopGraph implements org.apache.tinkerpop.gremlin.structure.Graph {
    public static final String GRAPH_ID = "gaffer.graphId";

    /**
     * Configuration key for a path to Gaffer store properties.
     *
     * @see uk.gov.gchq.gaffer.store.StoreProperties
     */
    public static final String STORE_PROPERTIES = "gaffer.storeproperties";

    /**
     * Configuration key for a string array of path to Gaffer schemas.
     *
     * @see Schema
     */
    public static final String SCHEMAS = "gaffer.schemas";

    /**
     * Configuration key for a string array of operation options.
     * Each option should in the form: key:value
     */
    public static final String OP_OPTIONS = "gaffer.operation.options";

    public static final String USER_ID = "gaffer.userId";

    public static final String DATA_AUTHS = "gaffer.dataAuths";

    /**
     * The vertex label for vertex IDs. These are {@link GafferPopVertex}s that
     * don't have any properties, just an ID value and a label of 'id'.
     */
    public static final String ID_LABEL = "id";

    private final Graph graph;
    private final Configuration configuration;
    private final GafferPopGraphVariables variables;
    private final GafferPopGraphFeatures features;
    private final Map<String, String> opOptions;
    private final User user;
    private final ServiceRegistry serviceRegistry;

    private static final Logger LOGGER = LoggerFactory.getLogger(GafferPopGraph.class);

    public GafferPopGraph(final Configuration configuration) {
        this(configuration, createGraph(configuration));
    }

    public GafferPopGraph(final Configuration configuration, final Graph graph) {
        this.configuration = configuration;
        this.graph = graph;
        features = new GafferPopGraphFeatures();
        opOptions = new HashMap<>();
        if (configuration().containsKey(OP_OPTIONS)) {
            for (final String option : configuration().getStringArray(OP_OPTIONS)) {
                final String[] parts = option.split(":");
                opOptions.put(parts[0], parts[1]);
            }
        }

        user = new User.Builder()
                .userId(configuration().getString(USER_ID, User.UNKNOWN_USER_ID))
                .dataAuths(configuration().getStringArray(DATA_AUTHS))
                .build();

        variables = createVariables();

        serviceRegistry = new ServiceRegistry();
        serviceRegistry.registerService(new GafferPopNamedOperationServiceFactory(this));
    }

    private static Graph createGraph(final Configuration configuration) {
        final String graphId = configuration.getString(GRAPH_ID);
        if (null == graphId) {
            throw new IllegalArgumentException(GRAPH_ID + " property is required");
        }

        final Path storeProps = Paths.get(configuration.getString(STORE_PROPERTIES));
        final Schema.Builder schemaBuilder = new Schema.Builder();
        for (final String schemaPath : configuration.getStringArray(SCHEMAS)) {
            schemaBuilder.merge(Schema.fromJson(Paths.get(schemaPath)));
        }

        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(graphId)
                        .build())
                .storeProperties(storeProps)
                .addSchema(schemaBuilder.build())
                .build();
    }

    public static GafferPopGraph open(final String configurationFile) {
        return (GafferPopGraph) GraphFactory.open(configurationFile);
    }

    public static GafferPopGraph open(final Configuration configuration) {
        return new GafferPopGraph(configuration);
    }

    public static GafferPopGraph open(final Configuration configuration, final Graph graph) {
        return new GafferPopGraph(configuration, graph);
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        final Object idValue = ElementHelper.getIdValue(keyValues).orElseThrow(() -> new IllegalArgumentException("ID is required"));
        final String label = ElementHelper.getLabelValue(keyValues).orElseThrow(() -> new IllegalArgumentException("Label is required"));

        final GafferPopVertex vertex = new GafferPopVertex(label, idValue, this);
        ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues);
        addVertex(vertex);
        return vertex;
    }

    public void addVertex(final GafferPopVertex vertex) {
        execute(new OperationChain.Builder()
                .first(new GenerateElements.Builder<GafferPopVertex>()
                        .input(vertex)
                        .generator(new GafferEntityGenerator())
                        .build())
                .then(new AddElements())
                .build());

        vertex.setReadOnly();
    }

    public void addEdge(final GafferPopEdge edge) {
        execute(new OperationChain.Builder()
                .first(new GenerateElements.Builder<GafferPopEdge>()
                        .input(edge)
                        .generator(new GafferEdgeGenerator())
                        .build())
                .then(new AddElements())
                .build());
    }

    /**
     * This performs getEntitiesBySeed operation on Gaffer.
     * At least 1 vertexId must be provided. Gaffer does not support unseeded
     * queries.
     * All provided vertexIds will also be returned as {@link GafferPopVertex}s with
     * the label 'id', in order to allow Gaffer graphs with no entities to still be traversed.
     *
     * @param vertexIds vertices ids to query for
     * @return iterator of {@link GafferPopVertex}s, each vertex represents
     * an {@link uk.gov.gchq.gaffer.data.element.Entity} in Gaffer
     * @see org.apache.tinkerpop.gremlin.structure.Graph#vertices(Object...)
     */
    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        final boolean getAll = null == vertexIds || 0 == vertexIds.length;

        final Output<Iterable<? extends Element>> getOperation;
        final List<Vertex> idVertices = new LinkedList<>();
        if (getAll) {
            getOperation = new GetAllElements.Builder()
                    .view(new View.Builder()
                            .entities(graph.getSchema().getEntityGroups())
                            .build())
                    .build();
        } else {
            final List<EntitySeed> entitySeeds = getEntitySeeds(Arrays.asList(vertexIds));
            getOperation = new GetElements.Builder()
                    .input(entitySeeds)
                    .view(new View.Builder()
                            .entities(graph.getSchema().getEntityGroups())
                            .build())
                    .build();
        }

        final Iterable<? extends GafferPopVertex> result = execute(new Builder()
                .first(getOperation)
                .then(new GenerateObjects.Builder<GafferPopVertex>()
                        .generator(new GafferPopVertexGenerator(this))
                        .build())
                .build());

        return new ChainedIterable<Vertex>(result, idVertices).iterator();
    }

    /**
     * This performs getRelatedEntities operation on Gaffer.
     * At least 1 id must be provided. Gaffer does not support unseeded
     * queries.
     * All provided vertex IDs will also be returned as {@link GafferPopVertex}s with
     * the label 'id', in order to allow Gaffer graphs with no entities to still be traversed.
     *
     * @param ids    vertex IDs and edge IDs to be queried for.
     *               You can use {@link Vertex}s, {@link GafferPopEdge}s,
     *               EdgeId or just vertex ID values
     * @param labels labels of Entities to filter for.
     * @return iterator of {@link GafferPopVertex}s, each vertex represents
     * an {@link uk.gov.gchq.gaffer.data.element.Entity} in Gaffer
     */
    public Iterator<GafferPopVertex> vertices(final Iterable<Object> ids, final String... labels) {
        return verticesWithView(ids, createViewWithEntities(labels));
    }

    /**
     * This performs getRelatedEntities operation on Gaffer.
     * At least 1 id must be provided. Gaffer does not support unseeded
     * queries.
     * All provided vertex IDs will also be returned as {@link GafferPopVertex}s with
     * the label 'id', in order to allow Gaffer graphs with no entities to still be traversed.
     *
     * @param ids  vertex IDs and edge IDs to be queried for.
     *             You can use {@link Vertex}s, {@link GafferPopEdge}s,
     *             EdgeIds or just vertex ID values
     * @param view a Gaffer {@link View} to filter the vertices
     * @return iterator of {@link GafferPopVertex}s, each vertex represents
     * an {@link uk.gov.gchq.gaffer.data.element.Entity} in Gaffer
     * @see #vertices(Iterable, String...)
     */
    public Iterator<GafferPopVertex> verticesWithView(final Iterable<Object> ids, final View view) {
        return verticesWithSeedsAndView(getElementSeeds(ids), view);
    }


    /**
     * This performs getAdjacentEntitySeeds then getEntityBySeed operation chain
     * on Gaffer.
     * Given a vertex id, adjacent vertices will be returned.
     * If you provide any optional labels then you must provide edge labels and
     * the vertex labels - any missing labels will cause the elements to be filtered out.
     * This method will not return 'id' vertices, only vertices that exist as entities in Gaffer.
     *
     * @param vertexId  the vertex id to start at.
     * @param direction the direction along edges to travel
     * @param labels    labels of vertices and edges. Alternatively you can supply a Gaffer View serialised into JSON.
     * @return iterator of {@link GafferPopVertex}
     */
    public Iterator<GafferPopVertex> adjVertices(final Object vertexId, final Direction direction, final String... labels) {
        return adjVerticesWithView(vertexId, direction, createView(labels));
    }

    /**
     * This performs getAdjacentEntitySeeds then getEntityBySeed operation chain
     * on Gaffer.
     * Given an iterable of vertex ids, adjacent vertices will be returned.
     * If you provide any optional labels then you must provide edge labels and
     * the vertex labels - any missing labels will cause the elements to be filtered out.
     * This method will not return 'id' vertices, only vertices that exist as entities in Gaffer.
     *
     * @param vertexIds the iterable of vertex ids to start at.
     * @param direction the direction along edges to travel
     * @param labels    labels of vertices and edges. Alternatively you can supply a Gaffer View serialised into JSON.
     * @return iterator of {@link GafferPopVertex}
     */
    public Iterator<GafferPopVertex> adjVertices(final Iterable<Object> vertexIds, final Direction direction, final String... labels) {
        return adjVerticesWithView(vertexIds, direction, createView(labels));
    }

    /**
     * This performs getAdjacentEntitySeeds then getEntityBySeed operation chain
     * on Gaffer.
     * Given a vertex id, adjacent vertices will be returned. If you provide
     * any optional labels then you must provide edge labels and the vertex
     * labels - any missing labels will cause the elements to be filtered out.
     * This method will not return 'id' vertices, only vertices that exist as entities in Gaffer.
     *
     * @param vertexId  the vertex id to start at.
     * @param direction the direction along edges to travel
     * @param view      a Gaffer {@link View} containing edge and entity groups.
     * @return iterator of {@link GafferPopVertex}
     */
    public Iterator<GafferPopVertex> adjVerticesWithView(final Object vertexId, final Direction direction, final View view) {
        return adjVerticesWithView(Collections.singletonList(vertexId), direction, view);
    }

    /**
     * This performs getAdjacentEntitySeeds then getEntityBySeed operation chain
     * on Gaffer.
     * Given an iterable of vertex ids, adjacent vertices will be returned.
     * If you provide any optional labels then you must provide edge labels and the vertex
     * labels - any missing labels will cause the elements to be filtered out.
     * This method will not return 'id' vertices, only vertices that exist as entities in Gaffer.
     *
     * @param vertexIds the iterable of vertex ids to start at.
     * @param direction the direction along edges to travel
     * @param view      a Gaffer {@link View} containing edge and entity groups.
     * @return iterator of {@link GafferPopVertex}
     */
    public Iterator<GafferPopVertex> adjVerticesWithView(final Iterable<Object> vertexIds, final Direction direction, final View view) {
        return adjVerticesWithSeedsAndView(getEntitySeeds(vertexIds), direction, view);
    }

    /**
     * This performs a getEdgesBySeed operation on Gaffer.
     * At least 1 edgeIds must be provided. Gaffer does not support unseeded
     * queries.
     *
     * @param edgeIds EdgeIds or {@link GafferPopEdge}s to query for
     * @return iterator of {@link GafferPopEdge}s.
     * @see org.apache.tinkerpop.gremlin.structure.Graph#edges(Object...)
     */
    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        final boolean getAll = null == edgeIds || 0 == edgeIds.length;

        final Output<Iterable<? extends Element>> getOperation;
        if (getAll) {
            getOperation = new GetAllElements.Builder()
                    .view(new View.Builder()
                            .edges(graph.getSchema().getEdgeGroups())
                            .build())
                    .build();
        } else {
            getOperation = new GetElements.Builder()
                    .input(getEdgeSeeds(Arrays.asList(edgeIds)))
                    .view(new View.Builder()
                            .edges(graph.getSchema().getEdgeGroups())
                            .build())
                    .build();
        }

        return (Iterator) execute(new OperationChain.Builder()
                .first(getOperation)
                .then(new GenerateObjects.Builder<GafferPopEdge>()
                        .generator(new GafferPopEdgeGenerator(this))
                        .build())
                .build()).iterator();
    }

    /**
     * This performs a getRelatedEdges operation on Gaffer.
     *
     * @param id        vertex ID or edge ID to be queried for.
     *                  You can use {@link Vertex}, {@link GafferPopEdge},
     *                  EdgeId or just a vertex ID value.
     * @param direction {@link Direction} of edges to return.
     * @param labels    labels of edges. Alternatively you can supply a Gaffer View serialised into JSON.
     * @return iterator of {@link GafferPopEdge}
     */
    public Iterator<GafferPopEdge> edges(final Object id, final Direction direction, final String... labels) {
        return edgesWithView(id, direction, createView(labels));
    }

    /**
     * This performs a getRelatedEdges operation on Gaffer.
     *
     * @param ids       vertex IDs and edge IDs to be queried for.
     *                  You can use {@link Vertex}s, {@link GafferPopEdge}s,
     *                  EdgeIds or just vertex ID values.
     * @param direction {@link Direction} of edges to return.
     * @param labels    labels of edges. Alternatively you can supply a Gaffer View serialised into JSON.
     * @return iterator of {@link GafferPopEdge}
     */
    public Iterator<GafferPopEdge> edges(final Iterable<Object> ids, final Direction direction, final String... labels) {
        return edgesWithView(ids, direction, createView(labels));
    }

    /**
     * This performs a getRelatedEdges operation on Gaffer.
     *
     * @param id        vertex ID or edge ID to be queried for.
     *                  You can use {@link Vertex}, {@link GafferPopEdge},
     *                  EdgeId or just a vertex ID value.
     * @param direction {@link Direction} of edges to return.
     * @param view      labels of edges. Alternatively you can supply a Gaffer View serialised into JSON.
     * @return iterator of {@link GafferPopEdge}
     */
    public Iterator<GafferPopEdge> edgesWithView(final Object id, final Direction direction, final View view) {
        return edgesWithView(Collections.singletonList(id), direction, view);
    }

    /**
     * This performs a getRelatedEdges operation on Gaffer.
     *
     * @param ids       vertex IDs and edge IDs to be queried for.
     *                  You can use {@link Vertex}s, {@link GafferPopEdge}s,
     *                  EdgeIds or just vertex ID values.
     * @param direction {@link Direction} of edges to return.
     * @param view      a Gaffer {@link View} containing edge groups.
     * @return iterator of {@link GafferPopEdge}
     */
    public Iterator<GafferPopEdge> edgesWithView(final Iterable<Object> ids, final Direction direction, final View view) {
        return edgesWithSeedsAndView(getElementSeeds(ids), direction, view);
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        throw Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Exceptions.graphComputerNotSupported();
    }

    @Override
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    @Override
    public Variables variables() {
        return this.variables;
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    @Override
    public void close() throws Exception {
        serviceRegistry.close();
    }

    @Override
    public ServiceRegistry getServiceRegistry() {
        return serviceRegistry;
    }

    @Override
    public Features features() {
        return features;
    }

    public <T> T execute(final OperationChain<T> opChain) {
        for (final Operation operation : opChain.getOperations()) {
            operation.setOptions(opOptions);

            if (operation instanceof Input) {
                Object input = ((Input) operation).getInput();
                if (input != null) {
                    if (input instanceof MappedIterable) {
                        ((MappedIterable) input).forEach(item -> { LOGGER.info("GafferPop operation input: " + item.toString()); });
                    } else {
                        LOGGER.info("GafferPop operation input: " + input.toString());
                    }
                }
            }

        }

        try {
            LOGGER.info("GafferPop operation chain called: " + opChain.toString());
            return graph.execute(opChain, user);
        } catch (final OperationException e) {
            throw new RuntimeException(e);
        }
    }

    private Iterator<GafferPopVertex> verticesWithSeedsAndView(final List<ElementSeed> seeds, final View view) {
        final boolean getAll = null == seeds || seeds.isEmpty();
        final LinkedList<GafferPopVertex> idVertices = new LinkedList<>();

        View entitiesView = view;
        if (null == entitiesView) {
            entitiesView = new View.Builder()
                    .entities(graph.getSchema().getEntityGroups())
                    .build();
        } else if (entitiesView.hasEdges()) {
            entitiesView = new View.Builder()
                    .merge(entitiesView)
                    .edges(Collections.emptyMap())
                    .build();
        }

        final Output<Iterable<? extends Element>> getOperation;
        if (getAll) {
            getOperation = new GetAllElements.Builder()
                    .view(entitiesView)
                    .build();
        } else {
            getOperation = new GetElements.Builder()
                    .input(seeds)
                    .view(entitiesView)
                    .build();

            if (null == entitiesView || entitiesView.getEntityGroups().contains(ID_LABEL)) {
                for (final ElementSeed elementSeed : seeds) {
                    if (elementSeed instanceof EntitySeed) {
                        idVertices.add(new GafferPopVertex(ID_LABEL, ((EntitySeed) elementSeed).getVertex(), this));
                    }
                }
            }
        }

        final Iterable<? extends GafferPopVertex> result = execute(new OperationChain.Builder()
                .first(getOperation)
                .then(new GenerateObjects.Builder<GafferPopVertex>()
                        .generator(new GafferPopVertexGenerator(this))
                        .build())
                .build());

        if (idVertices.isEmpty()) {
            return new ChainedIterable<GafferPopVertex>(result, idVertices).iterator();
        } else {
            return (Iterator<GafferPopVertex>) result.iterator();
        }
    }

    private Iterator<GafferPopVertex> adjVerticesWithSeedsAndView(final List<EntitySeed> seeds, final Direction direction, final View view) {
        if (null == seeds || seeds.isEmpty()) {
            throw new UnsupportedOperationException("There could be a lot of vertices, so please add some seeds");
        }

        View processedView = view == null ? createAllEntitiesView() : view;

        return (Iterator) execute(new OperationChain.Builder()
                .first(new GetAdjacentIds.Builder()
                        .input(seeds)
                        .view(processedView)
                        .inOutType(getInOutType(direction))
                        .build())
                .then(new GetElements.Builder()
                        .view(processedView)
                        .build())
                .then(new GenerateObjects.Builder<GafferPopVertex>()
                        .generator(new GafferPopVertexGenerator(this))
                        .build())
                .build()).iterator();
    }

    private Iterator<GafferPopEdge> edgesWithSeedsAndView(final List<ElementSeed> seeds, final Direction direction, final View view) {
        final boolean getAll = null == seeds || seeds.isEmpty();

        View edgesView = view;
        if (null == edgesView) {
            edgesView = new View.Builder()
                    .edges(graph.getSchema().getEdgeGroups())
                    .build();
        } else if (edgesView.hasEntities()) {
            edgesView = new View.Builder()
                    .merge(edgesView)
                    .entities(Collections.emptyMap())
                    .build();
        }

        final Output<Iterable<? extends Element>> getOperation;
        if (getAll) {
            getOperation = new GetAllElements.Builder()
                    .view(edgesView)
                    .build();
        } else {
            getOperation = new GetElements.Builder()
                    .input(seeds)
                    .view(edgesView)
                    .inOutType(getInOutType(direction))
                    .build();
        }

        return (Iterator) execute(new OperationChain.Builder()
                .first(getOperation)
                .then(new GenerateObjects.Builder<GafferPopEdge>()
                        .generator(new GafferPopEdgeGenerator(this, true))
                        .build())
                .build()).iterator();
    }

    private View createViewWithEntities(final String... labels) {
        View view = null;
        if (null != labels && 0 < labels.length) {
            if (1 == labels.length && labels[0].startsWith("View{")) {
                // Allows a view to be passed in as a label
                view = View.fromJson(labels[0].substring(4).getBytes(StandardCharsets.UTF_8));
            } else {
                final View.Builder viewBuilder = new View.Builder();
                for (final String label : labels) {
                    viewBuilder.entity(label);
                }
                view = viewBuilder.build();
            }
        }
        return view;
    }

    private View createView(final String... labels) {
        View view = null;
        if (null != labels && 0 < labels.length) {
            final View.Builder viewBuilder = new View.Builder();
            final Schema schema = ((Schema) variables().get(GafferPopGraphVariables.SCHEMA).get());
            for (final String label : labels) {
                if (schema.isEntity(label)) {
                    viewBuilder.entity(label);
                } else if (schema.isEdge(label)) {
                    viewBuilder.edge(label);
                } else if (!ID_LABEL.equals(label)) {
                    throw new IllegalArgumentException("Label/Group was found in the schema: " + label);
                }
            }
            view = viewBuilder.build();
        }
        return view;
    }

    private View createAllEntitiesView() {
        final View.Builder viewBuilder = new View.Builder();
        final Schema schema = ((Schema) variables().get(GafferPopGraphVariables.SCHEMA).get());
        for (final String group : schema.getEntityGroups()) {
            viewBuilder.entity(group);
        }
        return viewBuilder.build();
    }

    private List<ElementSeed> getElementSeeds(final Iterable<Object> ids) {
        List<ElementSeed> seeds = null;
        if (null != ids) {
            seeds = new LinkedList<>();
            for (final Object id : ids) {
                if (id instanceof Vertex) {
                    seeds.add(new EntitySeed(((Vertex) id).id()));
                } else if (id instanceof GafferPopEdge) {
                    seeds.add(new EdgeSeed(((GafferPopEdge) id).outVertex().id(), ((GafferPopEdge) id).inVertex().id(), true));
                } else if (id instanceof List) {
                    final List edgeIdList = (List) id;
                    if (edgeIdList.size() == 2) {
                        seeds.add(new EdgeSeed(edgeIdList.get(0), edgeIdList.get(1), true));
                    }
                } else {
                    seeds.add(new EntitySeed(id));
                }
            }
        }

        return seeds;
    }

    private List<EntitySeed> getEntitySeeds(final Iterable<Object> vertexIds) {
        List<EntitySeed> seeds = null;
        if (null != vertexIds) {
            seeds = new LinkedList<>();
            for (final Object vertexId : vertexIds) {
                if (vertexId instanceof Vertex) {
                    seeds.add(new EntitySeed(((Vertex) vertexId).id()));
                } else {
                    seeds.add(new EntitySeed(vertexId));
                }
            }
        }

        return seeds;
    }

    private List<EdgeSeed> getEdgeSeeds(final Iterable<Object> edgeIds) {
        List<EdgeSeed> seeds = null;
        if (null != edgeIds) {
            seeds = new LinkedList<>();
            for (final Object edgeIdObj : edgeIds) {
                final List edgeIdList;
                if (edgeIdObj instanceof GafferPopEdge) {
                    edgeIdList = Arrays.asList(((GafferPopEdge) edgeIdObj).outVertex().id(), ((GafferPopEdge) edgeIdObj).inVertex().id());
                } else if (edgeIdObj instanceof List) {
                    edgeIdList = ((List) edgeIdObj);
                } else {
                    final String className = null != edgeIdObj ? edgeIdObj.getClass().getName() : " a null object";
                    throw new IllegalArgumentException("Edge IDs must be either a EdgeId list or a GafferPopEdge. Not " + className);
                }
                seeds.add(new EdgeSeed(edgeIdList.get(0), edgeIdList.get(1), true));
            }
        }

        return seeds;
    }

    private IncludeIncomingOutgoingType getInOutType(final Direction direction) {
        final IncludeIncomingOutgoingType inOutType;
        if (Direction.OUT == direction) {
            inOutType = IncludeIncomingOutgoingType.OUTGOING;
        } else if (Direction.IN == direction) {
            inOutType = IncludeIncomingOutgoingType.INCOMING;
        } else {
            inOutType = IncludeIncomingOutgoingType.EITHER;
        }

        return inOutType;
    }

    private GafferPopGraphVariables createVariables() {
        final ConcurrentHashMap<String, Object> variablesMap = new ConcurrentHashMap<>();
        variablesMap.put(GafferPopGraphVariables.OP_OPTIONS, Collections.unmodifiableMap(opOptions));
        variablesMap.put(GafferPopGraphVariables.USER, user);
        variablesMap.put(GafferPopGraphVariables.SCHEMA, graph.getSchema());
        return new GafferPopGraphVariables(variablesMap);
    }
}
