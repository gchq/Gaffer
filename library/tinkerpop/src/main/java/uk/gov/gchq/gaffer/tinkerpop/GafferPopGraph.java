/*
 * Copyright 2016-2024 Crown Copyright
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
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies.GlobalCache;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph.OptIn;
import org.apache.tinkerpop.gremlin.structure.Graph.OptOut;
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
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.tinkerpop.generator.GafferEdgeGenerator;
import uk.gov.gchq.gaffer.tinkerpop.generator.GafferEntityGenerator;
import uk.gov.gchq.gaffer.tinkerpop.generator.GafferPopElementGenerator;
import uk.gov.gchq.gaffer.tinkerpop.process.traversal.strategy.optimisation.GafferPopGraphStepStrategy;
import uk.gov.gchq.gaffer.tinkerpop.service.GafferPopNamedOperationServiceFactory;
import uk.gov.gchq.gaffer.user.User;
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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A <code>GafferPopGraph</code> is an implementation of
 * {@link org.apache.tinkerpop.gremlin.structure.Graph}.
 * It wraps a Gaffer {@link Graph} and delegates all operations to it.
 * In addition to the tinkerpop methods required there are methods to add edges
 * query for adjacent vertices and to provide a {@link View} to filter out results.
 */

@OptIn(OptIn.SUITE_STRUCTURE_STANDARD)
@OptIn(OptIn.SUITE_STRUCTURE_INTEGRATE)
@OptIn(OptIn.SUITE_PROCESS_STANDARD)
@OptIn(OptIn.SUITE_PROCESS_LIMITED_STANDARD)
@OptOut(
    test = "org.apache.tinkerpop.gremlin.structure.io.IoCustomTest",
    method = "*",
    reason = "GafferPopGraph does not support Tinkerpop IO test cases")
@OptOut(
    test = "org.apache.tinkerpop.gremlin.structure.io.IoEdgeTest",
    method = "*",
    reason = "GafferPopGraph does not support Tinkerpop IO test cases")
@OptOut(
    test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest",
    method = "*",
    reason = "GafferPopGraph does not support Tinkerpop IO test cases")
@OptOut(
    test = "org.apache.tinkerpop.gremlin.structure.io.IoPropertyTest",
    method = "*",
    reason = "GafferPopGraph does not support Tinkerpop IO test cases")
@OptOut(
    test = "org.apache.tinkerpop.gremlin.structure.io.IoTest",
    method = "*",
    reason = "GafferPopGraph does not support Tinkerpop IO test cases")
@OptOut(
    test = "org.apache.tinkerpop.gremlin.structure.io.IoVertexTest",
    method = "*",
    reason = "GafferPopGraph does not support Tinkerpop IO test cases")
@OptOut(
    test = "org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdgeTest",
    method = "*",
    reason = "GafferPopGraph does not support detached test cases")
@OptOut(
    test = "org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest",
    method = "*",
    reason = "GafferPopGraph does not support Tinkerpop IO test cases")
@OptOut(
    test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
    method = "*",
    reason = "GafferPopGraph does not support Tinkerpop IO test cases")
@OptOut(
    test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.EarlyLimitStrategyProcessTest",
    method = "*",
    reason = "GafferPopGraph does not support Tinkerpop IO test cases")
@OptOut(
    test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.WriteTest$Traversals",
    method = "*",
    reason = "Currently a bug with the WriteTest that creates unwanted files")
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
     * Configuration key for a directory of Gaffer type schemas.
     * Primary use is for when the types and elements schemas are in different
     * directories, if main SCHEMAS key is defined it will be used in
     * preference to this one.
     */
    public static final String TYPES_SCHEMA = "gaffer.schema.types";

    /**
     * Configuration key for a directory of Gaffer element schemas.
     * Primary use is for when the types and elements schemas are in different
     * directories, if main SCHEMAS key is defined it will be used in
     * preference to this one.
     */
    public static final String ELEMENTS_SCHEMA = "gaffer.schema.elements";

    /**
     * Configuration key for a string array of operation options.
     * Each option should in the form: key:value
     */
    public static final String OP_OPTIONS = "gaffer.operation.options";

    public static final String USER_ID = "gaffer.userId";

    public static final String DATA_AUTHS = "gaffer.dataAuths";

    /**
     * Configuration key for stopping the elements added via Gremlin/Tinkerpop
     * from being readonly. If this is set a vertex or edge may have its properties
     * modified via the Tinkerpop interface using the defined ingest aggregation
     * function(s) it has set.
     */
    public static final String NOT_READ_ONLY_ELEMENTS = "gaffer.elements.notreadonly";

    /**
     * The vertex label for vertex IDs. These are {@link GafferPopVertex}s that
     * don't have any properties, just an ID value and a label of 'id'.
     */
    public static final String ID_LABEL = "id";

    /**
     * The type of vertex id manager to use see {@link DefaultIdManager}
     */
    public static final String ID_MANAGER = "vertex.id.manager";

    /**
     * Types of ID managers available for this graph (mainly used for testing).
     */
    public enum DefaultIdManager {
        INTEGER,
        LONG,
        STRING,
        UUID
    }

    // Internal ID tracker for using a number based ID manager
    protected AtomicLong currentId = new AtomicLong(-1L);

    private final Graph graph;
    private final Configuration configuration;
    private final GafferPopGraphVariables variables;
    private final GafferPopGraphFeatures features;
    private final Map<String, String> opOptions;
    private final User defaultUser;
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
        // Default user for operations
        defaultUser = new User.Builder()
                .userId(configuration().getString(USER_ID, User.UNKNOWN_USER_ID))
                .dataAuths(configuration().getStringArray(DATA_AUTHS))
                .build();

        // Set the graph variables to current config
        variables = new GafferPopGraphVariables();
        setDefaultVariables(variables);

        serviceRegistry = new ServiceRegistry();
        serviceRegistry.registerService(new GafferPopNamedOperationServiceFactory(this));

        // Add and register custom traversals
        TraversalStrategies traversalStrategies = GlobalCache.getStrategies(this.getClass()).addStrategies(
                GafferPopGraphStepStrategy.instance());
        GlobalCache.registerStrategies(this.getClass(), traversalStrategies);
    }

    private static Graph createGraph(final Configuration configuration) {
        final String graphId = configuration.getString(GRAPH_ID);
        if (null == graphId) {
            throw new IllegalArgumentException(GRAPH_ID + " property is required");
        }

        final Path storeProps = Paths.get(configuration.getString(STORE_PROPERTIES));
        final Schema.Builder schemaBuilder = new Schema.Builder();
        // Use SCHEMAS key if defined else use separate types and elements keys
        if (configuration.containsKey(SCHEMAS)) {
            Arrays.stream(configuration.getStringArray(SCHEMAS))
                .forEach(path -> schemaBuilder.merge(Schema.fromJson(Paths.get(path))));
        } else {
            Stream.concat(
                Arrays.stream(configuration.getStringArray(TYPES_SCHEMA)),
                Arrays.stream(configuration.getStringArray(ELEMENTS_SCHEMA)))
                    .forEach(path -> schemaBuilder.merge(Schema.fromJson(Paths.get(path))));
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
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        Object idValue;
        // Check if we are using an ID manager
        if (configuration.containsKey(ID_MANAGER)) {
            idValue = ElementHelper.getIdValue(keyValues).orElse(null);
            if (idValue == null) {
                idValue = getNextVertexId();
            }
        } else {
            idValue = ElementHelper.getIdValue(keyValues).orElseThrow(() -> new IllegalArgumentException("ID is required"));
        }

        /*
         * TODO: Check the ID type is relevant for the group (a.k.a label) in the schema and auto convert
         *       as the some Standard tinkerpop tests add data for the same group but with a different
         *       Object type for the ID. Using a String ID manager might be the most flexible for these
         *       tests.
         * Basic idea of auto converting the type is below:
         *
         * String idSchemaType = graph.getSchema().getEntity(label).getVertex();
         * String idTypeName = graph.getSchema().getType(idSchemaType).getFullClassString();
         * if (!idTypeName.equals(idValue.getClass().getName())) {
         *     LOGGER.warn("Vertex ID is not the correct type for the schema: " + idValue);
         *     idValue = graph.getSchema().getType(idSchemaType).getClazz().cast(idValue);
         * }
         */

        final GafferPopVertex vertex = new GafferPopVertex(label, idValue, this);
        ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues);
        addVertex(vertex);
        return vertex;
    }

    public void addVertex(final GafferPopVertex vertex) {
        // Create the entity and add to graph
        execute(new OperationChain.Builder()
                .first(new AddElements.Builder()
                    .input(new GafferEntityGenerator()._apply(vertex))
                    .build())
                .build());

        // Set read only if not told otherwise
        if (!configuration.containsKey(NOT_READ_ONLY_ELEMENTS)) {
            vertex.setReadOnly();
        }
    }

    public void addEdge(final GafferPopEdge edge) {
        // Create the edge and add to graph
        execute(new OperationChain.Builder()
            .first(new AddElements.Builder()
                    .input(new GafferEdgeGenerator()._apply(edge))
                    .build())
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
        if (getAll) {
            getOperation = new GetAllElements.Builder()
                    .view(new View.Builder()
                            .entities(graph.getSchema().getEntityGroups())
                            .build())
                    .build();
        } else {
            getOperation = new GetElements.Builder()
                    .input(getElementSeeds(Arrays.asList(vertexIds)))
                    .view(new View.Builder()
                            .entities(graph.getSchema().getEntityGroups())
                            .build())
                    .build();
        }
        // Run requested chain on the graph
        final Iterable<? extends Element> result = execute(new Builder()
                .first(getOperation)
                .build());

        // Translate results to Gafferpop elements
        final GafferPopElementGenerator generator = new GafferPopElementGenerator(this);
        final Iterable<Vertex> translatedResults = () -> StreamSupport.stream(result.spliterator(), false)
                .map(generator::_apply)
                .filter(Vertex.class::isInstance)
                .map(e -> (Vertex) e)
                .iterator();

        return translatedResults.iterator();
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
    public Iterator<Vertex> adjVertices(final Object vertexId, final Direction direction, final String... labels) {
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
    public Iterator<Vertex> adjVertices(final Iterable<Object> vertexIds, final Direction direction, final String... labels) {
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
    public Iterator<Vertex> adjVerticesWithView(final Object vertexId, final Direction direction, final View view) {
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
    public Iterator<Vertex> adjVerticesWithView(final Iterable<Object> vertexIds, final Direction direction, final View view) {
        return adjVerticesWithSeedsAndView(getElementSeeds(vertexIds), direction, view);
    }

    /**
     * This performs a getEdgesBySeed operation on Gaffer.
     * At least 1 ID must be provided as Gaffer does not support unseeded
     * queries. Supports various forms of IDs to enable passing either edge IDs
     * or Entity/Vertex IDs but will filter to only return edges.
     *
     * @param elementIds EdgeIds, {@link GafferPopEdge}s or Vertex IDs to query for
     * @return iterator of {@link GafferPopEdge}s.
     * @see org.apache.tinkerpop.gremlin.structure.Graph#edges(Object...)
     */
    @Override
    public Iterator<Edge> edges(final Object... elementIds) {
        final boolean getAll = null == elementIds || 0 == elementIds.length;

        final Output<Iterable<? extends Element>> getOperation;
        if (getAll) {
            getOperation = new GetAllElements.Builder()
                    .view(new View.Builder()
                            .edges(graph.getSchema().getEdgeGroups())
                            .build())
                    .build();
        } else {
            getOperation = new GetElements.Builder()
                    .input(getElementSeeds(Arrays.asList(elementIds)))
                    .view(new View.Builder()
                            .edges(graph.getSchema().getEdgeGroups())
                            .build())
                    .build();
        }

        // Run requested chain on the graph
        final Iterable<? extends Element> result = execute(new Builder()
                .first(getOperation)
                .build());

        // Translate results to Gafferpop elements
        final GafferPopElementGenerator generator = new GafferPopElementGenerator(this);
        final Iterable<Edge> translatedResults = () -> StreamSupport.stream(result.spliterator(), false)
                .map(generator::_apply)
                .filter(Edge.class::isInstance)
                .map(e -> (Edge) e)
                .iterator();

        return translatedResults.iterator();
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
    public Iterator<Edge> edges(final Object id, final Direction direction, final String... labels) {
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
    public Iterator<Edge> edges(final Iterable<Object> ids, final Direction direction, final String... labels) {
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
    public Iterator<Edge> edgesWithView(final Object id, final Direction direction, final View view) {
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
    public Iterator<Edge> edgesWithView(final Iterable<Object> ids, final Direction direction, final View view) {
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
            // Set options on operations
            operation.setOptions(variables.getOperationOptions());
            // Debug logging
            if (LOGGER.isDebugEnabled() && operation instanceof Input) {
                Object input = ((Input) operation).getInput();
                if (input instanceof MappedIterable) {
                    ((MappedIterable) input).forEach(item -> LOGGER.debug("GafferPop operation input: {}", item));
                } else {
                    LOGGER.debug("GafferPop operation input: {}", input);
                }
            }
        }
        // Use the requested user based on variables
        User user = new User.Builder()
            .userId(variables.getUserId())
            .dataAuths(variables.getDataAuths())
            .build();

        try {
            LOGGER.info("GafferPop operation chain called: {}", opChain.toOverviewString());
            return graph.execute(opChain, user);
        } catch (final Exception e) {
            LOGGER.error("Operation chain failed: {}", e.getMessage());
            throw new RuntimeException("GafferPop operation failed: " + e.getMessage(), e);
        } finally {
            // Reset the variables to default after running operation as they may have been updated in the query
            setDefaultVariables(variables);
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
                seeds.forEach(seed -> {
                    if (seed instanceof EntitySeed) {
                        idVertices.add(new GafferPopVertex(ID_LABEL, ((EntitySeed) seed).getVertex(), this));
                    }
                });
            }
        }

        // Run operation on graph
        final Iterable<? extends Element> result = execute(new OperationChain.Builder()
                .first(getOperation)
                .build());

        // Translate results to Gafferpop elements
        final GafferPopElementGenerator generator = new GafferPopElementGenerator(this);
        final Iterable<GafferPopVertex> translatedResults = () -> StreamSupport.stream(result.spliterator(), false)
                .map(generator::_apply)
                .filter(GafferPopVertex.class::isInstance)
                .map(e -> (GafferPopVertex) e)
                .iterator();

        return translatedResults.iterator();

    }

    private Iterator<Vertex> adjVerticesWithSeedsAndView(final List<ElementSeed> seeds, final Direction direction, final View view) {
        if (null == seeds || seeds.isEmpty()) {
            throw new UnsupportedOperationException("There could be a lot of vertices, so please add some seeds");
        }

        View processedView = view == null ? createAllEntitiesView() : view;
        final Iterable<? extends Element> result = execute(new OperationChain.Builder()
                .first(new GetAdjacentIds.Builder()
                        .input(seeds)
                        .view(processedView)
                        .inOutType(getInOutType(direction))
                        .build())
                // GetAdjacentIds provides list of entity seeds so run a GetElements to get the actual Entities
                .then(new GetElements())
                .build());

        // Translate results to Gafferpop elements
        final GafferPopElementGenerator generator = new GafferPopElementGenerator(this);
        final Iterable<Vertex> translatedResults = () -> StreamSupport.stream(result.spliterator(), false)
                .map(generator::_apply)
                .filter(Vertex.class::isInstance)
                .map(e -> (Vertex) e)
                .iterator();

        return translatedResults.iterator();
    }

    private Iterator<Edge> edgesWithSeedsAndView(final List<ElementSeed> seeds, final Direction direction, final View view) {
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

        // Run requested chain on the graph
        final Iterable<? extends Element> result = execute(new Builder()
                .first(getOperation)
                .build());

        // Translate results to Gafferpop elements
        final GafferPopElementGenerator generator = new GafferPopElementGenerator(this, true);
        final Iterable<Edge> translatedResults = () -> StreamSupport.stream(result.spliterator(), false)
                .map(generator::_apply)
                .filter(Edge.class::isInstance)
                .map(e -> (Edge) e)
                .iterator();

        return translatedResults.iterator();
    }

    private View createViewWithEntities(final String... labels) {
        View view = null;
        if (labels != null && labels.length > 0) {
            if (labels.length == 1 && labels[0].startsWith("View{")) {
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
            final Schema schema = graph.getSchema();
            for (final String label : labels) {
                if (schema.isEntity(label)) {
                    viewBuilder.entity(label);
                } else if (schema.isEdge(label)) {
                    viewBuilder.edge(label);
                } else if (!ID_LABEL.equals(label)) {
                    throw new IllegalArgumentException("Label/Group was not found in the schema: " + label);
                }
            }
            view = viewBuilder.build();
        }
        return view;
    }

    private View createAllEntitiesView() {
        final View.Builder viewBuilder = new View.Builder();
        for (final String group : graph.getSchema().getEntityGroups()) {
            viewBuilder.entity(group);
        }
        return viewBuilder.build();
    }

    /**
     * Determines the element seeds based on supplied IDs.
     * Supports input as a {@link Vertex}, {@link Edge}, List of Edge IDs or individual Vertex IDs.
     *
     * @param ids The iterable of IDs
     * @return List of {@link ElementSeed}s
     */
    private List<ElementSeed> getElementSeeds(final Iterable<Object> ids) {
        List<ElementSeed> seeds = new LinkedList<>();
        if (null == ids) {
            LOGGER.warn("Gaffer does not support unseeded queries, no results will be returned");
            return seeds;
        }

        ids.forEach(id -> {
            List<Object> edgeIdList = new LinkedList<>();
            // Extract Vertex ID
            if (id instanceof Vertex) {
                seeds.add(new EntitySeed(((Vertex) id).id()));
            // Extract Edge ID
            } else if (id instanceof Edge) {
                seeds.add(new EdgeSeed(((Edge) id).outVertex().id(), ((Edge) id).inVertex().id(), true));
            // Extract source and destination from ID list
            } else if (id instanceof Iterable) {
                ((Iterable<?>) id).forEach(edgeIdList::add);
            // Attempt to extract source and destination IDs from a string form of an array/list
            } else if ((id instanceof String) && (((String) id).matches("^\\[.*,.*\\]$"))) {
                edgeIdList = Arrays.asList(((String) id)
                        .replaceAll("\\s", "")
                        .replace("[", "")
                        .replace("]", "")
                        .split(","));
            // Assume entity ID as fallback
            } else {
                seeds.add(new EntitySeed(id));
            }

            // If found a list verify source and destination
            if (edgeIdList.size() == 2) {
                seeds.add(new EdgeSeed(edgeIdList.get(0), edgeIdList.get(1), true));
            }
        });

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

    /**
     * Sets the {@link GafferPopGraphVariables} to default values for this
     * graph
     *
     * @param variables The variables
     */
    private void setDefaultVariables(final GafferPopGraphVariables variables) {
        variables.set(GafferPopGraphVariables.OP_OPTIONS, Collections.unmodifiableMap(opOptions));
        variables.set(GafferPopGraphVariables.USER_ID, defaultUser.getUserId());
        variables.set(GafferPopGraphVariables.DATA_AUTHS, configuration().getStringArray(DATA_AUTHS));
    }

    /**
     * Gets the next ID to assign to a supplied vertex based on the currently configured
     * ID manager.
     *
     * @return Next ID Object
     */
    private Object getNextVertexId() {
        switch (configuration.getEnum(ID_MANAGER, DefaultIdManager.class)) {
            case INTEGER:
                return ((Long) currentId.incrementAndGet()).intValue();

            case UUID:
                return UUID.randomUUID();

            case STRING:
                return UUID.randomUUID().toString();

            // Use long for default
            default:
                return currentId.incrementAndGet();
        }
    }
}
