/*
 * Copyright 2016 Crown Copyright
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
package gaffer.gafferpop;

import gaffer.commonutil.CommonConstants;
import gaffer.commonutil.iterable.ChainedIterable;
import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.commonutil.iterable.CloseableIterator;
import gaffer.commonutil.iterable.WrappedCloseableIterable;
import gaffer.data.element.Entity;
import gaffer.data.elementdefinition.view.View;
import gaffer.gafferpop.generator.GafferPopEdgeGenerator;
import gaffer.gafferpop.generator.GafferPopVertexGenerator;
import gaffer.graph.Graph;
import gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import gaffer.operation.Operation;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationChain.Builder;
import gaffer.operation.OperationException;
import gaffer.operation.data.EdgeSeed;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.generate.GenerateElements;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.operation.impl.get.GetAllEdges;
import gaffer.operation.impl.get.GetAllEntities;
import gaffer.operation.impl.get.GetEdgesBySeed;
import gaffer.operation.impl.get.GetElements;
import gaffer.operation.impl.get.GetEntitiesBySeed;
import gaffer.operation.impl.get.GetRelatedEdges;
import gaffer.operation.impl.get.GetRelatedEntities;
import gaffer.store.schema.Schema;
import gaffer.user.User;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import java.io.UnsupportedEncodingException;
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
    /**
     * Configuration key for a path to Gaffer store properties.
     *
     * @see gaffer.store.StoreProperties
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

    public GafferPopGraph(final Configuration configuration) {
        this(configuration, createGraph(configuration));
    }

    public GafferPopGraph(final Configuration configuration, final Graph graph) {
        this.configuration = configuration;
        this.graph = graph;
        features = new GafferPopGraphFeatures();
        opOptions = new HashMap<>();
        if (configuration().containsKey(OP_OPTIONS)) {
            for (String option : configuration().getStringArray(OP_OPTIONS)) {
                final String[] parts = option.split(":");
                opOptions.put(parts[0], parts[1]);
            }
        }

        user = new User.Builder()
                .userId(configuration().getString(USER_ID, User.UNKNOWN_USER_ID))
                .dataAuths(configuration().getStringArray(DATA_AUTHS))
                .build();

        variables = createVariables();
    }

    private static Graph createGraph(final Configuration configuration) {
        final Path storeProps = Paths.get(configuration.getString(STORE_PROPERTIES));
        final Schema schema = new Schema();
        for (String schemaPath : configuration.getStringArray(SCHEMAS)) {
            schema.merge(Schema.fromJson(Paths.get(schemaPath)));
        }

        return new Graph.Builder()
                .storeProperties(storeProps)
                .addSchema(schema)
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
                        .objects(Collections.singletonList(vertex))
                        .generator(new GafferPopVertexGenerator(this))
                        .build())
                .then(new AddElements())
                .build());

        vertex.setReadOnly();
    }

    public void addEdge(final GafferPopEdge edge) {
        execute(new OperationChain.Builder()
                .first(new GenerateElements.Builder<GafferPopEdge>()
                        .objects(Collections.singletonList(edge))
                        .generator(new GafferPopEdgeGenerator(this))
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
     * an {@link Entity} in Gaffer
     * @see org.apache.tinkerpop.gremlin.structure.Graph#vertices(Object...)
     */
    @Override
    public CloseableIterator<Vertex> vertices(final Object... vertexIds) {
        final boolean getAll = null == vertexIds || 0 == vertexIds.length;

        final GetElements<? extends ElementSeed, Entity> getOperation;
        final List<Vertex> idVertices = new LinkedList<>();
        if (getAll) {
            getOperation = new GetAllEntities();
        } else {
            final List<EntitySeed> entitySeeds = getEntitySeeds(Arrays.asList(vertexIds));
            getOperation = new GetEntitiesBySeed.Builder()
                    .seeds(entitySeeds)
                    .build();
            for (EntitySeed entitySeed : entitySeeds) {
                idVertices.add(new GafferPopVertex(ID_LABEL, entitySeed.getVertex(), this));
            }
        }

        final CloseableIterable<GafferPopVertex> result = execute(new Builder()
                .first(getOperation)
                .then(new GenerateObjects.Builder<Entity, GafferPopVertex>()
                        .generator(new GafferPopVertexGenerator(this))
                        .build())
                .build());

        return new WrappedCloseableIterable<>(new ChainedIterable<Vertex>(result, idVertices)).iterator();
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
     *               {@link EdgeId}s or just vertex ID values
     * @param labels labels of Entities to filter for.
     * @return iterator of {@link GafferPopVertex}s, each vertex represents
     * an {@link Entity} in Gaffer
     */
    public CloseableIterator<GafferPopVertex> vertices(final Iterable<Object> ids, final String... labels) {
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
     *             {@link EdgeId}s or just vertex ID values
     * @param view a Gaffer {@link View} to filter the vertices
     * @return iterator of {@link GafferPopVertex}s, each vertex represents
     * an {@link Entity} in Gaffer
     * @see #vertices(Iterable, String...)
     */
    public CloseableIterator<GafferPopVertex> verticesWithView(final Iterable<Object> ids, final View view) {
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
    public CloseableIterator<GafferPopVertex> adjVertices(final Object vertexId, final Direction direction, final String... labels) {
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
    public CloseableIterator<GafferPopVertex> adjVertices(final Iterable<Object> vertexIds, final Direction direction, final String... labels) {
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
    public CloseableIterator<GafferPopVertex> adjVerticesWithView(final Object vertexId, final Direction direction, final View view) {
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
    public CloseableIterator<GafferPopVertex> adjVerticesWithView(final Iterable<Object> vertexIds, final Direction direction, final View view) {
        return adjVerticesWithSeedsAndView(getEntitySeeds(vertexIds), direction, view);
    }

    /**
     * This performs a getEdgesBySeed operation on Gaffer.
     * At least 1 edgeIds must be provided. Gaffer does not support unseeded
     * queries.
     *
     * @param edgeIds {@link EdgeId}s or {@link GafferPopEdge}s to query for
     * @return iterator of {@link GafferPopEdge}s.
     * @see org.apache.tinkerpop.gremlin.structure.Graph#edges(Object...)
     */
    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        final boolean getAll = null == edgeIds || 0 == edgeIds.length;

        final GetElements<? extends ElementSeed, gaffer.data.element.Edge> getOperation;
        if (getAll) {
            getOperation = new GetAllEdges();
        } else {
            getOperation = new GetEdgesBySeed.Builder()
                    .seeds(getEdgeSeeds(Arrays.asList(edgeIds)))
                    .build();
        }

        return (Iterator) execute(new OperationChain.Builder()
                .first(getOperation)
                .then(new GenerateObjects.Builder<gaffer.data.element.Edge, GafferPopEdge>()
                        .generator(new GafferPopEdgeGenerator(this))
                        .build())
                .build()).iterator();
    }

    /**
     * This performs a getRelatedEdges operation on Gaffer.
     *
     * @param id        vertex ID or edge ID to be queried for.
     *                  You can use {@link Vertex}, {@link GafferPopEdge},
     *                  {@link EdgeId} or just a vertex ID value.
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
     *                  {@link EdgeId}s or just vertex ID values.
     * @param direction {@link Direction} of edges to return.
     * @param labels    labels of edges. Alternatively you can supply a Gaffer View serialised into JSON.
     * @return iterator of {@link GafferPopEdge}
     */
    public CloseableIterator<GafferPopEdge> edges(final Iterable<Object> ids, final Direction direction, final String... labels) {
        return edgesWithView(ids, direction, createView(labels));
    }

    /**
     * This performs a getRelatedEdges operation on Gaffer.
     *
     * @param id        vertex ID or edge ID to be queried for.
     *                  You can use {@link Vertex}, {@link GafferPopEdge},
     *                  {@link EdgeId} or just a vertex ID value.
     * @param direction {@link Direction} of edges to return.
     * @param view      labels of edges. Alternatively you can supply a Gaffer View serialised into JSON.
     * @return iterator of {@link GafferPopEdge}
     */
    public CloseableIterator<GafferPopEdge> edgesWithView(final Object id, final Direction direction, final View view) {
        return edgesWithView(Collections.singletonList(id), direction, view);
    }

    /**
     * This performs a getRelatedEdges operation on Gaffer.
     *
     * @param ids       vertex IDs and edge IDs to be queried for.
     *                  You can use {@link Vertex}s, {@link GafferPopEdge}s,
     *                  {@link EdgeId}s or just vertex ID values.
     * @param direction {@link Direction} of edges to return.
     * @param view      a Gaffer {@link View} containing edge groups.
     * @return iterator of {@link GafferPopEdge}
     */
    public CloseableIterator<GafferPopEdge> edgesWithView(final Iterable<Object> ids, final Direction direction, final View view) {
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
    }

    @Override
    public Features features() {
        return features;
    }

    private <T> T execute(final OperationChain<T> opChain) {
        for (Operation operation : opChain.getOperations()) {
            operation.setOptions(opOptions);
        }

        try {
            return graph.execute(opChain, user);
        } catch (OperationException e) {
            throw new RuntimeException(e);
        }
    }

    private CloseableIterator<GafferPopVertex> verticesWithSeedsAndView(final List<ElementSeed> seeds, final View view) {
        final boolean getAll = null == seeds || seeds.isEmpty();
        final LinkedList<Vertex> idVertices = new LinkedList<>();

        final GetElements<? extends ElementSeed, Entity> getOperation;
        if (getAll) {
            getOperation = new GetAllEntities.Builder()
                    .view(view)
                    .build();
        } else {
            getOperation = new GetRelatedEntities.Builder<>()
                    .seeds(seeds)
                    .view(view)
                    .build();

            if (null == view || view.getEntityGroups().contains(ID_LABEL)) {
                for (ElementSeed elementSeed : seeds) {
                    if (elementSeed instanceof EntitySeed) {
                        idVertices.add(new GafferPopVertex(ID_LABEL, ((EntitySeed) elementSeed).getVertex(), this));
                    }
                }
            }
        }

        final CloseableIterable<GafferPopVertex> result = execute(new Builder()
                .first(getOperation)
                .then(new GenerateObjects.Builder<Entity, GafferPopVertex>()
                        .generator(new GafferPopVertexGenerator(this))
                        .build())
                .build());

        return idVertices.isEmpty()
                ? result.iterator()
                : new WrappedCloseableIterable<>(new ChainedIterable<GafferPopVertex>(result, idVertices)).iterator();
    }

    private CloseableIterator<GafferPopVertex> adjVerticesWithSeedsAndView(final List<EntitySeed> seeds, final Direction direction, final View view) {
        if (null == seeds || seeds.isEmpty()) {
            throw new UnsupportedOperationException("There could be a lot of vertices, so please add some seeds");
        }

        return execute(new OperationChain.Builder()
                .first(new GetAdjacentEntitySeeds.Builder()
                        .seeds(seeds)
                        .view(view)
                        .inOutType(getInOutType(direction))
                        .build())
                .then(new GetEntitiesBySeed.Builder()
                        .view(view)
                        .build())
                .then(new GenerateObjects.Builder<Entity, GafferPopVertex>()
                        .generator(new GafferPopVertexGenerator(this))
                        .build())
                .build()).iterator();
    }

    private CloseableIterator<GafferPopEdge> edgesWithSeedsAndView(final List<ElementSeed> seeds, final Direction direction, final View view) {
        final boolean getAll = null == seeds || seeds.isEmpty();

        final GetElements<? extends ElementSeed, gaffer.data.element.Edge> getOperation;
        if (getAll) {
            getOperation = new GetAllEdges.Builder()
                    .view(view)
                    .build();
        } else {
            getOperation = new GetRelatedEdges.Builder<>()
                    .seeds(seeds)
                    .view(view)
                    .inOutType(getInOutType(direction))
                    .build();
        }

        return execute(new OperationChain.Builder()
                .first(getOperation)
                .then(new GenerateObjects.Builder<gaffer.data.element.Edge, GafferPopEdge>()
                        .generator(new GafferPopEdgeGenerator(this, true))
                        .build())
                .build()).iterator();
    }

    private View createViewWithEntities(final String[] labels) {
        View view = null;
        if (null != labels && 0 < labels.length) {
            if (1 == labels.length && labels[0].startsWith("View{")) {
                // Allows a view to be passed in as a label
                try {
                    view = View.fromJson(labels[0].substring(4).getBytes(CommonConstants.UTF_8));
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            } else {
                final View.Builder viewBuilder = new View.Builder();
                for (String label : labels) {
                    viewBuilder.entity(label);
                }
                view = viewBuilder.build();
            }
        }
        return view;
    }

    private View createView(final String[] labels) {
        View view = null;
        if (null != labels && 0 < labels.length) {
            if (1 == labels.length && labels[0].startsWith("View{")) {
                // Allows a view to be passed in as a label
                try {
                    view = View.fromJson(labels[0].substring(4).getBytes(CommonConstants.UTF_8));
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            } else {
                final View.Builder viewBuilder = new View.Builder();
                final Schema schema = ((Schema) variables().get(GafferPopGraphVariables.SCHEMA).get());
                for (String label : labels) {
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
        }
        return view;
    }

    private List<ElementSeed> getElementSeeds(final Iterable<Object> ids) {
        List<ElementSeed> seeds = null;
        if (null != ids) {
            seeds = new LinkedList<>();
            for (Object id : ids) {
                if (id instanceof Vertex) {
                    seeds.add(new EntitySeed(((Vertex) id).id()));
                } else if (id instanceof GafferPopEdge) {
                    final EdgeId edgeId = ((GafferPopEdge) id).id();
                    seeds.add(new EdgeSeed(edgeId.getSource(), edgeId.getDest(), true));
                } else if (id instanceof EdgeId) {
                    final EdgeId edgeId = ((EdgeId) id);
                    seeds.add(new EdgeSeed(edgeId.getSource(), edgeId.getDest(), true));
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
            for (Object vertexId : vertexIds) {
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
            for (Object edgeIdObj : edgeIds) {
                final EdgeId edgeId;
                if (edgeIdObj instanceof GafferPopEdge) {
                    edgeId = ((GafferPopEdge) edgeIdObj).id();
                } else if (edgeIdObj instanceof EdgeId) {
                    edgeId = ((EdgeId) edgeIdObj);
                } else {
                    final String className = null != edgeIdObj ? edgeIdObj.getClass().getName() : " a null object";
                    throw new IllegalArgumentException("Edge IDs must be either a EdgeId or a GafferPopEdge. Not " + className);
                }
                seeds.add(new EdgeSeed(edgeId.getSource(), edgeId.getDest(), true));
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
            inOutType = IncludeIncomingOutgoingType.BOTH;
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
