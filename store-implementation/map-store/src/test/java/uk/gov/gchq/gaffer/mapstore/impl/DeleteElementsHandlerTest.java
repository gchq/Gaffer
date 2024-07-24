/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.mapstore.impl;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.mapstore.SingleUseMapStore;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.delete.DeleteElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DeleteElementsHandlerTest {
    private static final GetAllElements GET_ALL_ELEMENTS = new GetAllElements.Builder().build();
    private static final String BASIC_ENTITY = "BasicEntity";
    private static final String BASIC_EDGE1 = "BasicEdge";
    private static final View EDGE_VIEW = new View.Builder().edge(BASIC_EDGE1).build();
    private static final View ENTITY_VIEW = new View.Builder().entity(BASIC_ENTITY).build();
    private static Graph aggregatedGraph;
    private static Graph nonAggregatedGraph;

    static final User USER = new User();

    @BeforeEach
    public void setUp() throws Exception {
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();

        aggregatedGraph = getGraph();
        aggregatedGraph.execute(addElements, USER);

        nonAggregatedGraph = getGraphNoAggregation();
        nonAggregatedGraph.execute(addElements, USER);
    }

    @Test
    void shouldDeleteEntityOnlyForAggregatedGraph() throws OperationException {
        // Given/When

        // Delete Vertex A
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("A"))
                        .view(ENTITY_VIEW)
                        .build())
                .then(new DeleteElements())
                .build();

        aggregatedGraph.execute(chain, USER);

        // Then

        // Vertex A deleted
        // Vertices B and C and Edges A->B and B->C remaining
        final Iterable<? extends Element> results = aggregatedGraph.execute(GET_ALL_ELEMENTS, USER);
        assertThat(results)
            .hasSize(4)
            .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
            .doesNotContain(getEntity("A"));

        // Check Vertex A cannot be retrieved from Graph
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .view(ENTITY_VIEW)
                .build();
        final Iterable<? extends Element> getElementResults = aggregatedGraph.execute(getElements, USER);

        assertThat(getElementResults).isEmpty();
    }

    @Test
    void shouldDeleteEdgeOnlyForAggregatedGraph() throws OperationException {
        // Given/When

        // Delete Edge B->C
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EdgeSeed("B", "C"))
                        .view(EDGE_VIEW)
                        .build())
                .then(new DeleteElements())
                .build();

        aggregatedGraph.execute(chain, USER);

        // Then

        // Edge B->C deleted
        // All vertices remain but only edge A->B remains
        final Iterable<? extends Element> results = aggregatedGraph.execute(GET_ALL_ELEMENTS, USER);
        assertThat(results)
            .hasSize(4)
            .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
            .doesNotContain(getEdge("B", "C"));

        // Check Vertex A cannot be retrieved from Graph
        final GetElements getElements = new GetElements.Builder()
                    .input(new EdgeSeed("B", "C"))
                    .view(EDGE_VIEW)
                    .build();
        final Iterable<? extends Element> getElementResults = aggregatedGraph.execute(getElements, USER);

        assertThat(getElementResults).isEmpty();
    }

    @Test
    void shouldDeleteEntityOnlyForNonAggregatedGraph() throws OperationException {
        // Given/When

        // Delete Vertex A
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("A"))
                        .view(ENTITY_VIEW)
                        .build())
                .then(new DeleteElements())
                .build();

        nonAggregatedGraph.execute(chain, USER);

        // Then

        // Vertex A deleted
        // Vertices B and C and Edges A->B and B->C remaining
        final Iterable<? extends Element> results = nonAggregatedGraph.execute(GET_ALL_ELEMENTS, USER);
        assertThat(results)
            .hasSize(4)
            .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
            .doesNotContain(getEntity("A"));

        // Check Vertex A cannot be retrieved from Graph
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .view(ENTITY_VIEW)
                .build();
        final Iterable<? extends Element> getElementResults = nonAggregatedGraph.execute(getElements, USER);

        assertThat(getElementResults).isEmpty();
    }

    @Test
    void shouldDeleteEdgeOnlyForNonAggregatedGraph() throws OperationException {
        // Given
        // When

        // Delete Edge B->C
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EdgeSeed("B", "C"))
                        .view(EDGE_VIEW)
                        .build())
                .then(new DeleteElements())
                .build();

        nonAggregatedGraph.execute(chain, USER);

        // Then

        // Edge B->C deleted
        // All vertices remain but only edge A->B remains
        final Iterable<? extends Element> results = nonAggregatedGraph.execute(GET_ALL_ELEMENTS, USER);
        assertThat(results)
            .hasSize(4)
            .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
            .doesNotContain(getEdge("B", "C"));

        // Check Vertex A cannot be retrieved from Graph
        final GetElements getElements = new GetElements.Builder()
                    .input(new EdgeSeed("B", "C"))
                    .view(EDGE_VIEW)
                    .build();
        final Iterable<? extends Element> getElementResults = nonAggregatedGraph.execute(getElements, USER);

        assertThat(getElementResults).isEmpty();
    }

    @Test
    void shouldDeleteEntityAndEdgeForAggregatedGraph() throws OperationException {
        // Given/When

        // Delete Vertex A and its edges
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("A"))
                        .build())
                .then(new DeleteElements())
                .build();

        aggregatedGraph.execute(chain, USER);

        // Then

        // Vertex A Deleted and Edge A->B
        // Vertices B and C and Edge B->C remaining
        final Iterable<? extends Element> results = aggregatedGraph.execute(GET_ALL_ELEMENTS, USER);
        assertThat(results)
            .hasSize(3)
            .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
            .doesNotContain(getEntity("A"), getEdge("A", "B"));

        // Check Vertex A and it's edges can no longer be retrieved from graph
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .build();
        final Iterable<? extends Element> getElementResults = aggregatedGraph.execute(getElements, USER);
        assertThat(getElementResults).isEmpty();
    }

    @Test
    void shouldDeleteEntityAndEdgeForNonAggregatedGraph() throws OperationException {
        // Given/When

        // Delete Vertex A and its edges
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("A"))
                        .build())
                .then(new DeleteElements())
                .build();

        nonAggregatedGraph.execute(chain, USER);

        // Then

        // Vertex A Deleted and Edge A->B
        // Vertices B and C and Edge B->C remaining
        final Iterable<? extends Element> results = nonAggregatedGraph.execute(GET_ALL_ELEMENTS, USER);
        assertThat(results)
            .hasSize(3)
            .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
            .doesNotContain(getEntity("A"), getEdge("A", "B"));

        // Check Vertex A and it's edges can no longer be retrieved from graph
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .build();
        final Iterable<? extends Element> getElementResults = nonAggregatedGraph.execute(getElements, USER);
        assertThat(getElementResults).isEmpty();
    }

    @Test
    void shouldDeleteEntityAndAllEdgesForAggregatedGraph() throws OperationException {
        // Given/When

        // Delete Vertex B and its edges
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("B"))
                        .build())
                .then(new DeleteElements())
                .build();

        aggregatedGraph.execute(chain, USER);

        // Then

        // Vertex B Deleted and Edge A->B and Edge B->C
        // Vertices A and C remaining
        final Iterable<? extends Element> results = aggregatedGraph.execute(GET_ALL_ELEMENTS, USER);
        assertThat(results)
            .hasSize(2)
            .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
            .doesNotContain(getEntity("B"), getEdge("A", "B"), getEdge("B", "C"));

        // Check Vetex B and it's edges can no longer be retrieved from graph
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("B"))
                .build();
        final Iterable<? extends Element> getElementResults = aggregatedGraph.execute(getElements, USER);
        assertThat(getElementResults).isEmpty();
    }

    @Test
    void shouldDeleteEntityAndAllEdgesForNonAggregatedGraph() throws OperationException {
        // Given/When

        // Delete Vertex B and its edges
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("B"))
                        .build())
                .then(new DeleteElements())
                .build();

        nonAggregatedGraph.execute(chain, USER);

        // Then

        // Vertex B Deleted and Edge A->B and Edge B->C
        // Vertices A and C remaining
        final Iterable<? extends Element> results = nonAggregatedGraph.execute(GET_ALL_ELEMENTS, USER);
        assertThat(results)
            .hasSize(2)
            .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
            .doesNotContain(getEntity("B"), getEdge("A", "B"), getEdge("B", "C"));

        // Check Vetex B and it's edges can no longer be retrieved from graph
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("B"))
                .build();
        final Iterable<? extends Element> getElementResults = nonAggregatedGraph.execute(getElements, USER);
        assertThat(getElementResults).isEmpty();
    }

    @Test
    void shouldDeleteAll() throws OperationException {
        // Given/When

        // Delete all
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                        .build())
                .then(new DeleteElements())
                .build();

        aggregatedGraph.execute(chain, USER);

        // Then

        // All deleted
        final Iterable<? extends Element> results = aggregatedGraph.execute(GET_ALL_ELEMENTS, USER);
        assertThat(results).isEmpty();
    }

    @Test
    void shouldDeleteElementsInBatches() throws StoreException, OperationException {
        // Given
        // Map Store with larger ingest buffer size
        final MapStore store = new SingleUseMapStore();
        store.initialise("graphId1", getSchema(), new MapStoreProperties());
        store.getProperties().setIngestBufferSize(4);

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graph1")
                        .build())
                .addSchema(getSchema())
                .storeProperties(store.getProperties())
                .build();

        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, USER);

        // When

        // Delete all
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                        .build())
                .then(new DeleteElements())
                .build();

        graph.execute(chain, USER);

        // Then

        // All deleted
        final Iterable<? extends Element> results = graph.execute(GET_ALL_ELEMENTS, USER);
        assertThat(results).isEmpty();
    }


    public static List<Element> getElements() {
        final List<Element> elements = new ArrayList<>();

        elements.add(new Entity.Builder()
                .group(BASIC_ENTITY)
                .vertex("A")
                .build());

        elements.add(new Entity.Builder()
                .group(BASIC_ENTITY)
                .vertex("B")
                .build());

        elements.add(new Entity.Builder()
                .group(BASIC_ENTITY)
                .vertex("C")
                .build());

        elements.add(new Edge.Builder()
                .group(BASIC_EDGE1)
                .source("A")
                .dest("B")
                .directed(true)
                .build());

        elements.add(new Edge.Builder()
                .group(BASIC_EDGE1)
                .source("B")
                .dest("C")
                .directed(true)
                .build());

        return elements;
    }

    private static Edge getEdge(final String source, final String dest) {
        return new Edge.Builder()
                .group(BASIC_EDGE1)
                .source(source)
                .dest(dest)
                .directed(true)
                .build();
    }

    private static Entity getEntity(final String vertex) {
        return new Entity.Builder()
                .group(BASIC_ENTITY)
                .vertex(vertex)
                .build();
    }

    public static Schema getSchema() {
        return Schema.fromJson(StreamUtil.schemas(DeleteElementsHandlerTest.class));
    }

    static Graph getGraph() {
        final MapStoreProperties storeProperties = new MapStoreProperties();
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graph1")
                        .build())
                .addSchema(getSchema())
                .storeProperties(storeProperties)
                .build();
    }

    static Schema getSchemaNoAggregation() {
        return Schema.fromJson(StreamUtil.openStreams(DeleteElementsHandlerTest.class, "schema-no-aggregation"));
    }

    static Graph getGraphNoAggregation() {
        final MapStoreProperties storeProperties = new MapStoreProperties();
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graph1")
                        .build())
                .addSchema(getSchemaNoAggregation())
                .storeProperties(storeProperties)
                .build();
    }
}
