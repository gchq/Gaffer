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

import gaffer.WrappedIterable;
import gaffer.commonutil.CommonConstants;
import gaffer.data.element.Entity;
import gaffer.data.elementdefinition.view.View;
import gaffer.gafferpop.generator.GafferPopEdgeGenerator;
import gaffer.gafferpop.generator.GafferPopVertexGenerator;
import gaffer.graph.Graph;
import gaffer.operation.GetOperation;
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
import gaffer.operation.impl.get.GetEdgesBySeed;
import gaffer.operation.impl.get.GetEntitiesBySeed;
import gaffer.operation.impl.get.GetRelatedEdges;
import gaffer.operation.impl.get.GetRelatedEntities;
import gaffer.store.schema.Schema;
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

public class GafferPopGraph implements org.apache.tinkerpop.gremlin.structure.Graph {
    public static final String STORE_PROPERTIES = "gaffer.storeproperties";
    public static final String SCHEMAS = "gaffer.schemas";
    public static final String OP_OPTIONS = "gaffer.operation.options";
    private final Graph graph;
    private final Configuration configuration;
    private final GafferPopGraphVariables variables;
    private final GafferPopGraphFeatures features;
    private final Map<String, String> opOptions;

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

        vertex.setReadOnly(true);
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

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        if (null == vertexIds || 0 == vertexIds.length) {
            throw new UnsupportedOperationException("There could be a lot of vertices, so please add some seeds");
        }
        final List<EntitySeed> entitySeeds = getEntitySeeds(Arrays.asList(vertexIds));
        final Iterable<GafferPopVertex> result = execute(new Builder()
                .first(new GetEntitiesBySeed.Builder()
                        .seeds(entitySeeds)
                        .build())
                .then(new GenerateObjects.Builder<Entity, GafferPopVertex>()
                        .generator(new GafferPopVertexGenerator(this))
                        .build())
                .build());

        final LinkedList<Vertex> idVertices = new LinkedList<>();
        for (EntitySeed entitySeed : entitySeeds) {
            idVertices.add(new GafferPopVertex("id", entitySeed.getVertex(), this));
        }

        return new WrappedIterable<Vertex>(result, idVertices).iterator();
    }

    public Iterator<GafferPopVertex> vertices(final Iterable<Object> ids, final String... labels) {
        return verticesWithView(ids, createViewWithEntities(labels));
    }

    public Iterator<GafferPopVertex> verticesWithView(final Object id, final View view) {
        return verticesWithView(Collections.singletonList(id), view);
    }

    public Iterator<GafferPopVertex> verticesWithView(final Iterable<Object> ids, final View view) {
        return verticesWithSeedsAndView(getElementSeeds(ids), view);
    }

    private Iterator<GafferPopVertex> verticesWithSeedsAndView(final Iterable<ElementSeed> seeds, final View view) {
        final GetRelatedEntities getRelEntities = new GetRelatedEntities.Builder()
                .seeds(seeds)
                .view(view)
                .build();

        Iterable<GafferPopVertex> result = execute(new Builder()
                .first(getRelEntities)
                .then(new GenerateObjects.Builder<Entity, GafferPopVertex>()
                        .generator(new GafferPopVertexGenerator(this))
                        .build())
                .build());

        if (null == view || view.getEntityGroups().contains("id")) {
            final LinkedList<Vertex> idVertices = new LinkedList<>();
            for (ElementSeed elementSeed : seeds) {
                if (elementSeed instanceof EntitySeed) {
                    idVertices.add(new GafferPopVertex("id", ((EntitySeed) elementSeed).getVertex(), this));
                }
            }

            result = new WrappedIterable<>(result, idVertices);
        }

        return result.iterator();
    }

    public Iterator<GafferPopVertex> adjVertices(final Object vertexId, final Direction direction, final String... labels) {
        return adjVerticesWithView(vertexId, direction, createViewWithEdges(labels));
    }

    public Iterator<GafferPopVertex> adjVertices(final Iterable<Object> vertexIds, final Direction direction, final String... labels) {
        return adjVerticesWithView(vertexIds, direction, createViewWithEdges(labels));
    }

    public Iterator<GafferPopVertex> adjVerticesWithView(final Object vertexId, final Direction direction, final View view) {
        return adjVerticesWithView(Collections.singletonList(vertexId), direction, view);
    }

    public Iterator<GafferPopVertex> adjVerticesWithView(final Iterable<Object> ids, final Direction direction, final View view) {
        return adjVerticesWithSeedsAndView(getEntitySeeds(ids), direction, view);
    }

    private Iterator<GafferPopVertex> adjVerticesWithSeedsAndView(final Iterable<EntitySeed> seeds, final Direction direction, final View view) {
        final GetAdjacentEntitySeeds getAdjEntitySeeds = new GetAdjacentEntitySeeds.Builder()
                .seeds(seeds)
                .view(view)
                .build();
        setInOutType(direction, getAdjEntitySeeds);

        return execute(new OperationChain.Builder()
                .first(getAdjEntitySeeds)
                .then(new GetEntitiesBySeed())
                .then(new GenerateObjects.Builder<Entity, GafferPopVertex>()
                        .generator(new GafferPopVertexGenerator(this))
                        .build())
                .build()).iterator();
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        if (null == edgeIds || 0 == edgeIds.length) {
            throw new UnsupportedOperationException("There could be a lot of vertices, so please add some seeds");
        }

        return (Iterator) execute(new OperationChain.Builder()
                .first(new GetEdgesBySeed.Builder()
                        .seeds(getEdgeSeeds(Arrays.asList(edgeIds)))
                        .build())
                .then(new GenerateObjects.Builder<gaffer.data.element.Edge, GafferPopEdge>()
                        .generator(new GafferPopEdgeGenerator(this))
                        .build())
                .build()).iterator();
    }


    public Iterator<GafferPopEdge> edges(final Object id, final Direction direction, final String... labels) {
        return edgesWithView(id, direction, createViewWithEdges(labels));
    }

    public Iterator<GafferPopEdge> edges(final Iterable<Object> id, final Direction direction, final String... labels) {
        return edgesWithView(id, direction, createViewWithEdges(labels));
    }

    public Iterator<GafferPopEdge> edgesWithView(final Object id, final Direction direction, final View view) {
        return edgesWithView(Collections.singletonList(id), direction, view);
    }

    public Iterator<GafferPopEdge> edgesWithView(final Iterable<Object> ids, final Direction direction, final View view) {
        return edgesWithSeedsAndView(getElementSeeds(ids), direction, view);
    }

    private Iterator<GafferPopEdge> edgesWithSeedsAndView(final Iterable<ElementSeed> seeds, final Direction direction, final View view) {
        final GetRelatedEdges getRelEdges = new GetRelatedEdges.Builder()
                .seeds(seeds)
                .view(view)
                .build();
        setInOutType(direction, getRelEdges);

        return execute(new OperationChain.Builder()
                .first(getRelEdges)
                .then(new GenerateObjects.Builder<gaffer.data.element.Edge, GafferPopEdge>()
                        .generator(new GafferPopEdgeGenerator(this))
                        .build())
                .build()).iterator();
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
            return graph.execute(opChain);
        } catch (OperationException e) {
            throw new RuntimeException(e);
        }
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

    private View createViewWithEdges(final String[] labels) {
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
                    viewBuilder.edge(label);
                }
                view = viewBuilder.build();
            }
        }
        return view;
    }

    private List<ElementSeed> getElementSeeds(final Iterable<Object> ids) {
        final List<ElementSeed> seeds = new LinkedList<>();
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

        return seeds;
    }

    private List<EntitySeed> getEntitySeeds(final Iterable<Object> vertexIds) {
        final List<EntitySeed> seeds = new LinkedList<>();
        for (Object vertexId : vertexIds) {
            if (vertexId instanceof Vertex) {
                seeds.add(new EntitySeed(((Vertex) vertexId).id()));
            } else {
                seeds.add(new EntitySeed(vertexId));
            }
        }
        return seeds;
    }

    private List<EdgeSeed> getEdgeSeeds(final Iterable<Object> edgeIds) {
        final List<EdgeSeed> edgeSeeds = new LinkedList<>();
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
            edgeSeeds.add(new EdgeSeed(edgeId.getSource(), edgeId.getDest(), true));
        }
        return edgeSeeds;
    }

    private void setInOutType(final Direction direction, final GetOperation op) {
        final IncludeIncomingOutgoingType inOutType;
        if (Direction.OUT == direction) {
            inOutType = IncludeIncomingOutgoingType.OUTGOING;
        } else if (Direction.IN == direction) {
            inOutType = IncludeIncomingOutgoingType.INCOMING;
        } else {
            inOutType = IncludeIncomingOutgoingType.BOTH;
        }
        op.setIncludeIncomingOutGoing(inOutType);
    }

    private GafferPopGraphVariables createVariables() {
        final ConcurrentHashMap<String, Object> variablesMap = new ConcurrentHashMap<>();
        variablesMap.put(GafferPopGraphVariables.OP_OPTIONS, opOptions);
        variablesMap.put(GafferPopGraphVariables.SCHEMA, graph.getSchema());
        return new GafferPopGraphVariables(variablesMap);
    }
}
