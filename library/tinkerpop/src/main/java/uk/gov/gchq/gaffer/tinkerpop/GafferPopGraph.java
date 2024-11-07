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

import org.apache.commons.collections4.IterableUtils;
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
import uk.gov.gchq.gaffer.data.element.id.EntityId;
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
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.tinkerpop.generator.GafferEdgeGenerator;
import uk.gov.gchq.gaffer.tinkerpop.generator.GafferEntityGenerator;
import uk.gov.gchq.gaffer.tinkerpop.generator.GafferPopElementGenerator;
import uk.gov.gchq.gaffer.tinkerpop.process.traversal.strategy.optimisation.GafferPopGraphStepStrategy;
import uk.gov.gchq.gaffer.tinkerpop.process.traversal.strategy.optimisation.GafferPopHasStepStrategy;
import uk.gov.gchq.gaffer.tinkerpop.process.traversal.strategy.optimisation.GafferPopVertexStepStrategy;
import uk.gov.gchq.gaffer.tinkerpop.process.traversal.util.GafferCustomTypeFactory;
import uk.gov.gchq.gaffer.tinkerpop.process.traversal.util.GafferVertexUtils;
import uk.gov.gchq.gaffer.tinkerpop.service.GafferPopNamedOperationServiceFactory;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.iterable.MappedIterable;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
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

    /**
     * Configuration key for the max number of elements returned by a getElements
     */
    public static final String GET_ELEMENTS_LIMIT = "gaffer.elements.getlimit";

    /**
     * Default value for the max number of elements returned by getElements
     */
    public static final int DEFAULT_GET_ELEMENTS_LIMIT = 5000;

    /**
     * Configuration key for when to apply HasStep filtering
     */
    public static final String HAS_STEP_FILTER_STAGE = "gaffer.elements.hasstepfilterstage";

    public enum HasStepFilterStage {
        PRE_AGGREGATION,
        POST_AGGREGATION,
        POST_TRANSFORM
    }

    /**
     * Default to pre-aggregation filtering for HasStep predicates
     */
    public static final HasStepFilterStage DEFAULT_HAS_STEP_FILTER_STAGE = HasStepFilterStage.PRE_AGGREGATION;

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
     * Key for use in the store properties to allow setting the file location of
     * the GafferPop properties file from a store properties file.
     */
    public static final String GAFFERPOP_PROPERTIES = "gaffer.gafferpop.properties";

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
    private static final String GET_DEBUG_MSG = "Requested a GetElements, results will be truncated to: {}.";
    private static final Pattern EDGE_ID_REGEX = Pattern.compile("^\\s*\\[\\s*(?<src>[a-zA-Z0-9|-]*)\\s*(,\\s*(?<label>[a-zA-Z0-9|-]*))?\\s*,\\s*(?<dest>[a-zA-Z0-9|-]*)\\s*\\]\\s*$");

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
                GafferPopGraphStepStrategy.instance(),
                GafferPopHasStepStrategy.instance(),
                GafferPopVertexStepStrategy.instance());
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
         *     as the some Standard tinkerpop tests add data for the same group but with a different
         *     Object type for the ID. Using a String ID manager might be the most flexible for these
         *     tests.
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
     * This performs a GetElements operation on Gaffer.
     * If no vertex ids are provided, it performs a GetAllElements operation instead.
     * The results of GetElements will be truncated to a configured max size.
     *
     * @param vertexIds vertex ids to query for.
     * Supports input as a {@link Vertex}, {@link Edge}, List of Edge IDs or individual Vertex IDs.
     * @return iterator of {@link GafferPopVertex}s, each vertex represents
     * an {@link uk.gov.gchq.gaffer.data.element.Entity} in Gaffer
     * @see org.apache.tinkerpop.gremlin.structure.Graph#vertices(Object...)
     */
    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        final boolean getAll = null == vertexIds || 0 == vertexIds.length;
        final OperationChain<Iterable<? extends Element>> getOperation;
        final Iterable<Vertex> orphanVertices;

        LOGGER.debug(GET_DEBUG_MSG, variables.getElementsLimit());
        if (getAll) {
            getOperation = new Builder()
                    .first(new GetAllElements.Builder()
                            .view(createAllEntitiesView())
                            .build())
                    .then(new Limit<Element>(variables.getElementsLimit(), true))
                    .build();
        } else {
            getOperation = new Builder()
                .first(new GetElements.Builder()
                    .input(getElementSeeds(Arrays.asList(vertexIds)))
                    .build())
                .then(new Limit<Element>(variables.getElementsLimit(), true))
                .build();
        }
        // Run requested chain on the graph
        final Iterable<? extends Element> result = execute(getOperation);

        // Translate results to Gafferpop elements
        final GafferPopElementGenerator generator = new GafferPopElementGenerator(this);
        final Iterable<Vertex> translatedResults = () -> StreamSupport.stream(result.spliterator(), false)
                .map(generator::_apply)
                .filter(Vertex.class::isInstance)
                .map(e -> (Vertex) e)
                .limit(variables.getElementsLimit())
                .iterator();

        if (IterableUtils.size(translatedResults) >= variables.getElementsLimit()) {
            LOGGER.warn(
                "Result size is greater than or equal to configured limit ({}). Results may have been truncated",
                variables.getElementsLimit());
        }

        // Check for seeds that are not entities but are vertices on an edge (orphan vertices)
        orphanVertices = GafferVertexUtils.getOrphanVertices(result, this, vertexIds);
        Iterable<Vertex> chainedIterable = IterableUtils.chainedIterable(translatedResults, orphanVertices);

        return chainedIterable.iterator();
    }

    /**
     * This performs a GetElements operation on Gaffer filtering vertices by labels.
     * If no vertex ids are provided, it performs a GetAllElements operation instead.
     * The results of GetAllElements will be truncated to a configured max size.
     *
     * @param ids vertex ids to query for.
     * Supports input as a {@link Vertex}, {@link Edge}, List of Edge IDs or individual Vertex IDs.
     * @param labels labels of Entities to filter for.
     * Alternatively you can supply a Gaffer View serialised into JSON.
     * @return iterator of {@link GafferPopVertex}s, each vertex represents an
     * {@link uk.gov.gchq.gaffer.data.element.Entity} in Gaffer
     * @see #vertices(Object...)
     */
    public Iterator<GafferPopVertex> vertices(final Iterable<Object> ids, final String... labels) {
        return verticesWithView(ids, createViewWithEntities(labels));
    }

    /**
     * This performs a GetElements operation on Gaffer filtering by a {@link View}.
     *
     * @param ids vertex ids to query for.
     * Supports input as a {@link Vertex}, {@link Edge}, List of Edge IDs or individual Vertex IDs.
     * @param view a Gaffer {@link View} to filter vertices by
     * @return iterator of {@link GafferPopVertex}s, each vertex represents
     * an {@link uk.gov.gchq.gaffer.data.element.Entity} in Gaffer
     * @see #vertices(Object...)
     */
    public Iterator<GafferPopVertex> verticesWithView(final Iterable<Object> ids, final View view) {
        return verticesWithSeedsAndView(getElementSeeds(ids), view);
    }

    /**
     * This performs GetAdjacentIds then GetElements operation chain
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
     * This performs GetAdjacentIds then GetElements operation chain
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
     * This performs GetAdjacentIds then GetElements operation chain
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
     * This performs GetAdjacentIds then GetElements operation chain
     * on Gaffer.
     * Given an iterable of vertex ids, adjacent vertices will be returned.
     * If you provide any optional labels then you must provide edge labels and the vertex
     * labels - any missing labels will cause the elements to be filtered out.
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
     * This performs a GetElements operation on Gaffer.
     * If no element ids are provided, it performs a GetAllElements operation instead.
     * The results of GetAllElements will be truncated to a configured max size.
     *
     * @param elementIds element ids to query for.
     * Supports input as a {@link Vertex}, {@link Edge}, List of Edge IDs or individual Vertex IDs.
     * @return iterator of {@link GafferPopEdge}s.
     * @see org.apache.tinkerpop.gremlin.structure.Graph#edges(Object...)
     */
    @Override
    public Iterator<Edge> edges(final Object... elementIds) {
        final boolean getAll = null == elementIds || 0 == elementIds.length;
        final OperationChain<Iterable<? extends Element>> getOperation;

        LOGGER.debug(GET_DEBUG_MSG, variables.getElementsLimit());
        if (getAll) {
            getOperation = new Builder()
                .first(new GetAllElements.Builder()
                        .view(createAllEdgesView())
                        .build())
                .then(new Limit<>(variables.getElementsLimit(), true))
                .build();
        } else {

            View.Builder builder = new View.Builder();
            Set<String> edgeLabels = getEdgeLabelsFromIds(Arrays.asList(elementIds));
            if (edgeLabels.isEmpty()) {
                // Default to all edges
                builder.allEdges(true);
            } else {
                // Get requested edges
                builder.edges(edgeLabels);
            }

            getOperation = new Builder()
                .first(new GetElements.Builder()
                    .input(getElementSeeds(Arrays.asList(elementIds)))
                    .view(builder.build())
                    .build())
                .then(new Limit<>(variables.getElementsLimit(), true))
                .build();
        }

        // Run requested chain on the graph
        final Iterable<? extends Element> result = execute(getOperation);

        // Translate results to Gafferpop elements
        final GafferPopElementGenerator generator = new GafferPopElementGenerator(this);
        final Iterable<Edge> translatedResults = () -> StreamSupport.stream(result.spliterator(), false)
                .map(generator::_apply)
                .filter(Edge.class::isInstance)
                .map(e -> (Edge) e)
                .limit(variables.getElementsLimit())
                .iterator();

        if (IterableUtils.size(translatedResults) >= variables.getElementsLimit()) {
            LOGGER.warn(
                "Result size is greater than or equal to configured limit ({}). Results may have been truncated",
                variables.getElementsLimit());
        }
        return translatedResults.iterator();
    }

    /**
     * This performs a GetElements operation filtering edges by labels and direction.
     *
     * @param id vertex ID or edge ID to be queried for.
     * Supports input as a {@link Vertex}, {@link Edge}, List of Edge IDs or individual Vertex IDs.
     * @param direction {@link Direction} of edges to return.
     * @param labels labels of edges. Alternatively you can supply a Gaffer View serialised into JSON.
     * @return iterator of {@link GafferPopEdge}s.
     * @see #edges(Object...)
     */
    public Iterator<Edge> edges(final Object id, final Direction direction, final String... labels) {
        return edgesWithView(id, direction, createView(labels));
    }

    /**
     * This performs a GetElements operation filtering edges by labels and direction.
     *
     * @param ids vertex IDs or edge IDs to be queried for.
     * Supports input as a {@link Vertex}, {@link Edge}, List of Edge IDs or individual Vertex IDs.
     * @param direction {@link Direction} of edges to return.
     * @param labels labels of edges. Alternatively you can supply a Gaffer View serialised into JSON.
     * @return iterator of {@link GafferPopEdge}s.
     * @see #edges(Object...)
     */
    public Iterator<Edge> edges(final Iterable<Object> ids, final Direction direction, final String... labels) {
        return edgesWithView(ids, direction, createView(labels));
    }

    /**
     * This performs a GetElements operation filtering edges by direction and view.
     *
     * @param id vertex ID or edge ID to be queried for.
     * Supports input as a {@link Vertex}, {@link Edge}, List of Edge IDs or individual Vertex IDs.
     * @param direction {@link Direction} of edges to return.
     * @param view Gaffer {@link View} to filter edges by
     * @return iterator of {@link GafferPopEdge}s.
     * @see #edges(Object...)
     */
    public Iterator<Edge> edgesWithView(final Object id, final Direction direction, final View view) {
        return edgesWithView(Collections.singletonList(id), direction, view);
    }

    /**
     * This performs a GetElements operation filtering edges by direction and view.
     *
     * @param ids vertex IDs or edge IDs to be queried for.
     * Supports input as a {@link Vertex}, {@link Edge}, List of Edge IDs or individual Vertex IDs.
     * @param direction {@link Direction} of edges to return.
     * @param view Gaffer {@link View} to filter edges by
     * @return iterator of {@link GafferPopEdge}s.
     * @see #edges(Object...)
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
        // Set options at opChain level
        opChain.setOptions(variables.getOperationOptions());
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

        // Add the current chain to the list of chains ran so far for this query (it is reset by the graph step)
        List<Operation> currentChain = variables.getLastOperationChain().getOperations();
        currentChain.add(opChain);
        variables.set(GafferPopGraphVariables.LAST_OPERATION_CHAIN, new OperationChain<>(currentChain));

        try {
            LOGGER.info("GafferPop operation chain called: {}", opChain.toOverviewString());
            return graph.execute(opChain, variables.getUser());
        } catch (final Exception e) {
            LOGGER.error("Operation chain failed: {}", e.getMessage());
            throw new RuntimeException("GafferPop operation failed: " + e.getMessage(), e);
        }
    }

    /**
     * Sets the {@link GafferPopGraphVariables} to default values for this
     * graph
     *
     * @param variables The variables
     */
    public void setDefaultVariables(final GafferPopGraphVariables variables) {
        LOGGER.debug("Resetting graph variables to defaults");
        variables.set(GafferPopGraphVariables.OP_OPTIONS, Collections.unmodifiableMap(opOptions));
        variables.set(GafferPopGraphVariables.USER, defaultUser);
        variables.set(GafferPopGraphVariables.GET_ELEMENTS_LIMIT,
                configuration().getInteger(GET_ELEMENTS_LIMIT, DEFAULT_GET_ELEMENTS_LIMIT));
        variables.set(GafferPopGraphVariables.HAS_STEP_FILTER_STAGE,
                configuration().getString(HAS_STEP_FILTER_STAGE, DEFAULT_HAS_STEP_FILTER_STAGE.toString()));
        variables.set(GafferPopGraphVariables.LAST_OPERATION_CHAIN, new OperationChain<Object>());
    }

    /**
     * Get the underlying Gaffer graph this GafferPop graph is connected to.
     *
     * @return The Gaffer Graph.
     */
    public Graph getGafferGraph() {
        return graph;
    }

    private Iterator<GafferPopVertex> verticesWithSeedsAndView(final List<ElementSeed> seeds, final View view) {
        final boolean getAll = null == seeds || seeds.isEmpty();
        final LinkedList<GafferPopVertex> idVertices = new LinkedList<>();

        View entitiesView = view;
        if (null == entitiesView) {
            entitiesView = createAllEntitiesView();
        } else if (entitiesView.hasEdges()) {
            entitiesView = new View.Builder()
                    .merge(entitiesView)
                    .edges(Collections.emptyMap())
                    .build();
        }

        final OperationChain<Iterable<? extends Element>> getOperation;
        LOGGER.debug(GET_DEBUG_MSG, variables.getElementsLimit());
        if (getAll) {
            getOperation = new Builder()
                    .first(new GetAllElements.Builder()
                            .view(entitiesView)
                            .build())
                    .then(new Limit<>(variables.getElementsLimit(), true))
                    .build();
        } else {
            getOperation = new Builder()
                    .first(new GetElements.Builder()
                            .input(seeds)
                            .view(entitiesView)
                            .build())
                    .then(new Limit<>(variables.getElementsLimit(), true))
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
        final Iterable<? extends Element> result = execute(getOperation);

        // Translate results to Gafferpop elements
        final GafferPopElementGenerator generator = new GafferPopElementGenerator(this);
        final Iterable<GafferPopVertex> translatedResults = () -> StreamSupport.stream(result.spliterator(), false)
                .map(generator::_apply)
                .filter(GafferPopVertex.class::isInstance)
                .map(e -> (GafferPopVertex) e)
                .limit(variables.getElementsLimit())
                .iterator();

        return translatedResults.iterator();

    }

    private Iterator<Vertex> adjVerticesWithSeedsAndView(final List<ElementSeed> seeds, final Direction direction, final View view) {
        if (null == seeds || seeds.isEmpty()) {
            throw new UnsupportedOperationException("There could be a lot of vertices, so please add some seeds");
        }

        final Iterable<? extends EntityId> getAdjEntitySeeds = execute(new OperationChain.Builder()
                .first(new GetAdjacentIds.Builder()
                    .input(seeds)
                    .view(view)
                    .inOutType(getInOutType(direction))
                    .build())
                .build());

        List<EntityId> seedList = StreamSupport.stream(getAdjEntitySeeds.spliterator(), false).collect(Collectors.toList());

        // GetAdjacentIds provides list of entity seeds so run a GetElements to get the actual Entities
        final Iterable<? extends Element> result = execute(new OperationChain.Builder()
                .first(new GetElements.Builder()
                    .input(seedList)
                    .build())
                .build());

        // Translate results to Gafferpop elements
        final GafferPopElementGenerator generator = new GafferPopElementGenerator(this);
        final Iterable<Vertex> translatedResults = () -> StreamSupport.stream(result.spliterator(), false)
                .map(generator::_apply)
                .filter(Vertex.class::isInstance)
                .map(e -> (Vertex) e)
                .iterator();

        // Check for seeds that are not entities but are vertices on an edge (orphan vertices)
        Iterable<Vertex> chainedIterable = translatedResults;
        for (final EntityId seed : seedList) {
            Iterable<Vertex> orphanVertices = GafferVertexUtils.getOrphanVertices(result, this, seed.getVertex());
            chainedIterable = IterableUtils.chainedIterable(chainedIterable, orphanVertices);
        }
        return chainedIterable.iterator();
    }

    private Iterator<Edge> edgesWithSeedsAndView(final List<ElementSeed> seeds, final Direction direction, final View view) {
        final boolean getAll = null == seeds || seeds.isEmpty();

        View edgesView = view;
        if (null == edgesView) {
            edgesView = createAllEdgesView();
        } else if (edgesView.hasEntities()) {
            edgesView = new View.Builder()
                    .merge(edgesView)
                    .entities(Collections.emptyMap())
                    .build();
        }

        final OperationChain<Iterable<? extends Element>> getOperation;
        LOGGER.debug(GET_DEBUG_MSG, variables.getElementsLimit());
        if (getAll) {
            getOperation = new Builder()
                    .first(new GetAllElements.Builder()
                            .view(edgesView)
                            .build())
                    .then(new Limit<>(variables.getElementsLimit(), true))
                    .build();
        } else {
            getOperation = new Builder()
                    .first(new GetElements.Builder()
                            .input(seeds)
                            .view(edgesView)
                            .inOutType(getInOutType(direction))
                            .build())
                    .then(new Limit<>(variables.getElementsLimit(), true))
                    .build();
        }

        // Run requested chain on the graph
        final Iterable<? extends Element> result = execute(getOperation);

        // Translate results to Gafferpop elements
        final GafferPopElementGenerator generator = new GafferPopElementGenerator(this, true);
        final Iterable<Edge> translatedResults = () -> StreamSupport.stream(result.spliterator(), false)
                .map(generator::_apply)
                .filter(Edge.class::isInstance)
                .map(e -> (Edge) e)
                .limit(variables.getElementsLimit())
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
            final Schema schema = execute(new OperationChain.Builder()
                .first(new GetSchema.Builder()
                    .compact(true)
                    .build())
                .options(opOptions)
                .build());
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
        return new View.Builder().allEntities(true).build();
    }

    private View createAllEdgesView() {
        return new View.Builder().allEdges(true).build();
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
                Object parsedId = GafferCustomTypeFactory.parseAsCustomTypeIfValid(((Vertex) id).id());
                seeds.add(new EntitySeed(parsedId));
            // Extract Edge ID
            } else if (id instanceof Edge) {
                Object src = GafferCustomTypeFactory.parseAsCustomTypeIfValid(((Edge) id).outVertex().id());
                Object target = GafferCustomTypeFactory.parseAsCustomTypeIfValid(((Edge) id).inVertex().id());
                seeds.add(new EdgeSeed(src, target));
            // Extract source and destination from ID list
            } else if (id instanceof Iterable) {
                ((Iterable<?>) id).forEach(edgeIdList::add);
            // Attempt to extract source and destination IDs from a string from of an
            // array/list
            } else if (id instanceof String) {
                Matcher edgeIDMatcher = EDGE_ID_REGEX.matcher((String) id);
                // Check if contains label in edge ID
                if (edgeIDMatcher.matches()) {
                    seeds.add(new EdgeSeed(edgeIDMatcher.group("src"), edgeIDMatcher.group("dest")));
                // If not then check if a custom type e.g. TSTV
                } else {
                    seeds.add(new EntitySeed(GafferCustomTypeFactory.parseAsCustomTypeIfValid(id)));
                }
            // Assume entity ID as a fallback
            } else {
                seeds.add(new EntitySeed(id));
            }
        });

        return seeds;
    }

    /**
     * Extracts edge labels from edge IDs if found.
     * All ids must be of the [src, label, dest] format.
     * Otherwise, returns an empty set.
     *
     * @param ids The iterable of IDs
     * @return Set of edge labels for the view
     */
    private Set<String> getEdgeLabelsFromIds(final Iterable<Object> ids) {
        Set<String> labels = new HashSet<>();

        for (final Object id: ids) {
            if ((id instanceof String) && (EDGE_ID_REGEX.matcher((String) id).matches())) {
                Matcher edgeIdWithLabelMatcher = EDGE_ID_REGEX.matcher((String) id);

                // If contains label, extract to use in View as edge group
                if (edgeIdWithLabelMatcher.matches() && edgeIdWithLabelMatcher.group("label") != null) {
                    labels.add(edgeIdWithLabelMatcher.group("label"));
                }
            } else {
                // If a single ID isn't of the format [src, label, dest]
                // Then all edge labels must be used
                return Collections.emptySet();
            }
        }

        return labels;
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
     * Gets the next ID to assign to a supplied vertex based on the currently configured ID manager.
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
