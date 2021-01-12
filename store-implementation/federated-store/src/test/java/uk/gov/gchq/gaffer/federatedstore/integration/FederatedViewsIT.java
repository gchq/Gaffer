/*
 * Copyright 2019-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.integration;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.integration.util.TestUtil;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.federatedstore.PublicAccessPredefinedFederatedStore.ACCUMULO_GRAPH_WITH_EDGES;
import static uk.gov.gchq.gaffer.federatedstore.PublicAccessPredefinedFederatedStore.ACCUMULO_GRAPH_WITH_ENTITIES;

/**
 * In all of theses tests the Federated graph contains two graphs, one containing
 * a schema with only edges the other with only entities.
 */
public class FederatedViewsIT {

    public static final String BASIC_EDGE = "BasicEdge";
    public static final String BASIC_ENTITY = "BasicEntity";

    private static final FederatedStoreProperties FEDERATED_PROPERTIES = FederatedStoreProperties.loadStoreProperties(
        StreamUtil.openStream(FederatedViewsIT.class, "publicAccessPredefinedFederatedStore.properties"));

    private static final AccumuloProperties ACCUMULO_PROPERTIES = AccumuloProperties.loadStoreProperties(
            StreamUtil.openStream(FederatedViewsIT.class, "properties/singleUseAccumuloStore.properties"));


    @BeforeEach
    @AfterEach
    public void clearCache() {
        CacheServiceLoader.shutdown();
    }

    private Schema createSchema() {
        final Schema.Builder schemaBuilder = new Schema.Builder(TestUtil.createDefaultSchema());
        schemaBuilder.edges(Collections.EMPTY_MAP);
        schemaBuilder.entities(Collections.EMPTY_MAP);
        schemaBuilder.json(StreamUtil.openStream(FederatedViewsIT.class, "schema/basicEdgeSchema.json"));
        schemaBuilder.json(StreamUtil.openStream(FederatedViewsIT.class, "schema/basicEntitySchema.json"));
        return schemaBuilder.build();
    }

    private Graph createGraph() {
        return new Graph.Builder()
            .addSchema(createSchema())
            .storeProperties(FEDERATED_PROPERTIES)
            .config(new GraphConfig("federated"))
            .build();
    }

    @Test
    public void shouldBeEmptyAtStart() throws OperationException {
        // Given
        Graph graph = createGraph();
        User user = new User();
        final CloseableIterable<? extends Element> edges = graph.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(BASIC_EDGE)
                        .build())
                .build(), user);

        assertFalse(edges.iterator().hasNext());

        final CloseableIterable<? extends Element> entities = graph.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity(BASIC_ENTITY)
                        .build())
                .build(), user);

        assertFalse(entities.iterator().hasNext());

    }

    /**
     * Federation acts as a Edge/Entity graph with a view of Edge
     *
     * @throws OperationException any
     */
    @Test
    public void shouldAddAndGetEdge() throws OperationException {
        Graph graph = createGraph();
        User user = new User();

        addBasicEdge(graph, user);

        final CloseableIterable<? extends Element> rtn = graph.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(BASIC_EDGE)
                        .build())
                .build(), user);

        assertTrue(rtn.iterator().hasNext());

    }

    /**
     * Federation acts as a Edge/Entity graph with a view of Entity
     *
     * @throws OperationException any
     */
    @Test
    public void shouldAddAndGetEntity() throws OperationException {
        Graph graph = createGraph();
        User user = new User();
        addBasicEntity(graph, user);

        final CloseableIterable<? extends Element> rtn = graph.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity(BASIC_ENTITY)
                        .build())
                .build(), user);

        assertTrue(rtn.iterator().hasNext());

    }

    /**
     * Federation acts as a Edge graph with a view of Edge
     *
     * @throws OperationException any
     */
    @Test
    public void shouldAddAndGetEdgeWithEdgeGraph() throws OperationException {
        Graph graph = createGraph();
        User user = new User();
        addBasicEdge(graph, user);

        final CloseableIterable<? extends Element> rtn = graph.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(BASIC_EDGE)
                        .build())
                .option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, ACCUMULO_GRAPH_WITH_EDGES)
                .build(), user);

        assertTrue(rtn.iterator().hasNext());

    }

    /**
     * Federation acts as a Entity graph with a view of Entity
     *
     * @throws OperationException any
     */
    @Test
    public void shouldAddAndGetEntityWithEntityGraph() throws OperationException {
        Graph graph = createGraph();
        User user = new User();
        addBasicEntity(graph, user);

        final CloseableIterable<? extends Element> rtn = graph.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity(BASIC_ENTITY)
                        .build())
                .option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, ACCUMULO_GRAPH_WITH_ENTITIES)
                .build(), user);

        assertTrue(rtn.iterator().hasNext());

    }

    /**
     * Federation acts as a Entity graph with a view of Edge
     *
     * @throws OperationException any
     */
    @Test
    public void shouldNotAddAndGetEdgeWithEntityGraph() throws OperationException {
        Graph graph = createGraph();
        User user = new User();
        addBasicEdge(graph, user);

        Exception e = assertThrows(Exception.class, () -> {
            final CloseableIterable<? extends Element> rtn = graph.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                    .edge(BASIC_EDGE)
                    .build())
                .option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, ACCUMULO_GRAPH_WITH_ENTITIES)
                .build(), user);
        });

        assertEquals("Operation chain is invalid. Validation errors: \n" +
                    "View is not valid for graphIds:[AccumuloStoreContainingEntities]\n" +
                    "(graphId: AccumuloStoreContainingEntities) View for operation uk.gov.gchq.gaffer.operation.impl.get.GetAllElements is not valid. \n" +
                    "(graphId: AccumuloStoreContainingEntities) Edge group BasicEdge does not exist in the schema", e.getMessage());

    }

    /**
     * Federation acts as a Edge graph with a view of Entity
     *
     * @throws OperationException any
     */
    @Test
    public void shouldNotAddAndGetEntityWithEntityGraph() throws OperationException {
        Graph graph = createGraph();
        User user = new User();
        addBasicEntity(graph, user);

        Exception e = assertThrows(Exception.class, () -> {
            final CloseableIterable<? extends Element> rtn = graph.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                    .entity(BASIC_ENTITY)
                    .build())
                .option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, ACCUMULO_GRAPH_WITH_EDGES)
                .build(), user);
        });

        assertEquals("Operation chain is invalid. Validation errors: \n" +
                "View is not valid for graphIds:[AccumuloStoreContainingEdges]\n" +
                "(graphId: AccumuloStoreContainingEdges) View for operation uk.gov.gchq.gaffer.operation.impl.get.GetAllElements is not valid. \n" +
                "(graphId: AccumuloStoreContainingEdges) Entity group BasicEntity does not exist in the schema", e.getMessage());


    }

    /**
     * Federation acts as a Edge/Entity graph with a view of Edge and Entity
     *
     * @throws OperationException any
     */
    @Test
    public void shouldGetEntitiesAndEdgesFromAnEntityAndAnEdgeGraph() throws OperationException {
        Graph graph = createGraph();
        User user = new User();
        addBasicEntity(graph, user);
        addBasicEdge(graph, user);

        final CloseableIterable<? extends Element> rtn = graph.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(BASIC_EDGE)
                        .entity(BASIC_ENTITY)
                        .build())
                .build(), user);

        final ArrayList<? extends Element> elements = Lists.newArrayList(rtn.iterator());
        assertEquals(2, elements.size());
    }

    @Test
    public void shouldGetDoubleEdgesFromADoubleEdgeGraph() throws OperationException {
        Graph graph = createGraph();
        User user = new User();

        graph.execute(new RemoveGraph.Builder()
                .graphId(ACCUMULO_GRAPH_WITH_ENTITIES)
                .build(), user);

        graph.execute(new AddGraph.Builder()
                .graphId(ACCUMULO_GRAPH_WITH_EDGES + 2)
                .storeProperties(ACCUMULO_PROPERTIES)
                .schema(Schema.fromJson(StreamUtil.openStream(FederatedViewsIT.class, "schema/basicEdge2Schema.json")))
                .build(), user);

        addBasicEdge(graph, user);

        graph.execute(new AddElements.Builder()
                .input(Lists.newArrayList(new Edge.Builder()
                        .group(BASIC_EDGE + 2)
                        .source("a")
                        .dest("b")
                        .build()))
                .build(), user);

        final CloseableIterable<? extends Element> rtn = graph.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(BASIC_EDGE)
                        .edge(BASIC_EDGE + 2)
                        .build())
                .build(), user);

        final ArrayList<? extends Element> elements = Lists.newArrayList(rtn.iterator());

        assertEquals(2, elements.size());
    }

    @Test
    public void shouldGetDoubleEntitiesFromADoubleEntityGraph() throws OperationException {
        Graph graph = createGraph();
        User user = new User();

        graph.execute(new RemoveGraph.Builder()
                .graphId(ACCUMULO_GRAPH_WITH_EDGES)
                .build(), user);

        graph.execute(new AddGraph.Builder()
                .graphId(ACCUMULO_GRAPH_WITH_ENTITIES + 2)
                .storeProperties(ACCUMULO_PROPERTIES)
                .schema(Schema.fromJson(StreamUtil.openStream(FederatedViewsIT.class, "schema/basicEntity2Schema.json")))
                .build(), user);

        addBasicEntity(graph, user);

        graph.execute(new AddElements.Builder()
                .input(Lists.newArrayList(new Entity.Builder()
                        .group(BASIC_ENTITY + 2)
                        .vertex("a")
                        .build()))
                .build(), user);

        final CloseableIterable<? extends Element> rtn = graph.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity(BASIC_ENTITY)
                        .entity(BASIC_ENTITY + 2)
                        .build())
                .build(), user);

        final ArrayList<? extends Element> elements = Lists.newArrayList(rtn.iterator());

        assertEquals(2, elements.size());
    }

    protected void addBasicEdge(final Graph graph, final User user) throws OperationException {

        graph.execute(new AddElements.Builder()
                .input(Lists.newArrayList(new Edge.Builder()
                        .group(BASIC_EDGE)
                        .source("a")
                        .dest("b")
                        .build()))
                .build(), user);
    }

    protected void addBasicEntity(final Graph graph, final User user) throws OperationException {
        graph.execute(new AddElements.Builder()
                .input(Lists.newArrayList(new Entity.Builder()
                        .group(BASIC_ENTITY)
                        .vertex("a")
                        .build()))
                .build(), user);
    }
}


