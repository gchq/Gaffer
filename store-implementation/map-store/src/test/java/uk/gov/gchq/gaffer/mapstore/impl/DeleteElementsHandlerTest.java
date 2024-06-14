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

    static final String BASIC_ENTITY = "BasicEntity";
    static final String BASIC_EDGE1 = "BasicEdge";
    static final String BASIC_EDGE2 = "BasicEdge2";
    static final String PROPERTY1 = "property1";
    static final String PROPERTY2 = "property2";
    static final String COUNT = "count";

    @Test
    public void shouldDeleteEntityOnlyForAggregatedGraph() throws OperationException {
        // Given

        // Aggregated graph
        final Graph graph = getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When

        // Delete Vertex A
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("A"))
                        .view(new View.Builder().entity(BASIC_ENTITY).build())
                        .build())
                .then(new DeleteElements())
                .build();

        graph.execute(chain, new User());

        // Then

        // Vertex A deleted
        // Vertices B and C and Edges A->B and B->C remaining
        final GetAllElements getAllElements = new GetAllElements.Builder().build();
        final Iterable<? extends Element> results = graph.execute(getAllElements, new User());
        assertThat(results).hasSize(4);

        // Check Vertex A cannot be retrieved from Graph
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .view(new View.Builder().entity(BASIC_ENTITY).build())
                .build();
        final Iterable<? extends Element> getElementResults = graph.execute(getElements, new User());

        assertThat(getElementResults).isEmpty();
    }

    @Test
    public void shouldDeleteEntityOnlyForNonAggregatedGraph() throws OperationException {
        // Given

        // Non aggregated graph
        final Graph graph = getGraphNoAggregation();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When

        // Delete Vertex A
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("A"))
                        .view(new View.Builder().entity(BASIC_ENTITY).build())
                        .build())
                .then(new DeleteElements())
                .build();

        graph.execute(chain, new User());

        // Then

        // Vertex A deleted
        // Vertices B and C and Edges A->B and B->C remaining
        final GetAllElements getAllElements = new GetAllElements.Builder().build();
        final Iterable<? extends Element> results = graph.execute(getAllElements, new User());
        assertThat(results).hasSize(4);

        // Check Vertex A cannot be retrieved from Graph
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .view(new View.Builder().entity(BASIC_ENTITY).build())
                .build();
        final Iterable<? extends Element> getElementResults = graph.execute(getElements, new User());

        assertThat(getElementResults).isEmpty();
    }

    @Test
    public void shouldDeleteEntityAndEdgeForAggregatedGraph() throws OperationException {
        // Given

        // Aggregated graph
        final Graph graph = getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When

        // Delete Vertex A and its edges
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("A"))
                        .build())
                .then(new DeleteElements())
                .build();

        graph.execute(chain, new User());

        // Then

        // Vertex A Deleted and Edge A->B
        // Vertices B and C and Edge B->C remaining
        final GetAllElements getAllElements = new GetAllElements.Builder().build();
        final Iterable<? extends Element> results = graph.execute(getAllElements, new User());
        assertThat(results).hasSize(3);

        // Check Vetex A and it's edges can no longer be retrieved from graph
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .build();
        final Iterable<? extends Element> getElementResults = graph.execute(getElements, new User());
        assertThat(getElementResults).isEmpty();
    }

    @Test
    public void shouldDeleteEntityAndEdgeForNonAggregatedGraph() throws OperationException {
        // Given

        // Non Aggregated graph
        final Graph graph = getGraphNoAggregation();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When

        // Delete Vertex A and its edges
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("A"))
                        .build())
                .then(new DeleteElements())
                .build();

        graph.execute(chain, new User());

        // Then

        // Vertex A Deleted and Edge A->B
        // Vertices B and C and Edge B->C remaining
        final GetAllElements getAllElements = new GetAllElements.Builder().build();
        final Iterable<? extends Element> results = graph.execute(getAllElements, new User());
        assertThat(results).hasSize(3);

        // Check Vetex A and it's edges can no longer be retrieved from graph
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .build();
        final Iterable<? extends Element> getElementResults = graph.execute(getElements, new User());
        assertThat(getElementResults).isEmpty();
    }

    @Test
    public void shouldDeleteEntityAndAllEdgesForAggregatedGraph() throws OperationException {
        // Given

        // Aggregated graph
        final Graph graph = getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When

        // Delete Vertex B and its edges
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("B"))
                        .build())
                .then(new DeleteElements())
                .build();

        graph.execute(chain, new User());

        // Then

        // Vertex B Deleted and Edge A->B and Edge B->C
        // Vertices A and C remaining
        final GetAllElements getAllElements = new GetAllElements.Builder().build();
        final Iterable<? extends Element> results = graph.execute(getAllElements, new User());
        assertThat(results).hasSize(2);

        // Check Vetex B and it's edges can no longer be retrieved from graph
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("B"))
                .build();
        final Iterable<? extends Element> getElementResults = graph.execute(getElements, new User());
        assertThat(getElementResults).isEmpty();
    }

    @Test
    public void shouldDeleteEntityAndAllEdgesForNonAggregatedGraph() throws OperationException {
        // Given

        // Non Aggregated graph
        final Graph graph = getGraphNoAggregation();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When

        // Delete Vertex B and its edges
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("B"))
                        .build())
                .then(new DeleteElements())
                .build();

        graph.execute(chain, new User());

        // Then

        // Vertex B Deleted and Edge A->B and Edge B->C
        // Vertices A and C remaining
        final GetAllElements getAllElements = new GetAllElements.Builder().build();
        final Iterable<? extends Element> results = graph.execute(getAllElements, new User());
        assertThat(results).hasSize(2);

        // Check Vetex B and it's edges can no longer be retrieved from graph
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("B"))
                .build();
        final Iterable<? extends Element> getElementResults = graph.execute(getElements, new User());
        assertThat(getElementResults).isEmpty();
    }

    @Test
    public void shouldDeleteAll() throws OperationException {
        // Given

        // Aggregated graph
        final Graph graph = getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When

        // Delete all
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                        .build())
                .then(new DeleteElements())
                .build();

        graph.execute(chain, new User());

        // Then

        // All deleted
        final GetAllElements getAllElements = new GetAllElements.Builder().build();
        final Iterable<? extends Element> results = graph.execute(getAllElements, new User());
        assertThat(results).isEmpty();
    }

    @Test
    public void shouldDeleteElementsInBatches() throws StoreException, OperationException {
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
        graph.execute(addElements, new User());

        // When

        // Delete all
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                        .build())
                .then(new DeleteElements())
                .build();

        graph.execute(chain, new User());

        // Then

        // All deleted
        final GetAllElements getAllElements = new GetAllElements.Builder().build();
        final Iterable<? extends Element> results = graph.execute(getAllElements, new User());
        assertThat(results).isEmpty();
    }


    public static List<Element> getElements() {
        final List<Element> elements = new ArrayList<>();

        elements.add(new Entity.Builder()
                .group(BASIC_ENTITY)
                .vertex("A")
                .property(PROPERTY1, "p")
                .property(COUNT, 1)
                .build());

        elements.add(new Entity.Builder()
                .group(BASIC_ENTITY)
                .vertex("B")
                .property(PROPERTY1, "p")
                .property(COUNT, 1)
                .build());

        elements.add(new Entity.Builder()
                .group(BASIC_ENTITY)
                .vertex("C")
                .property(PROPERTY1, "p")
                .property(COUNT, 1)
                .build());

        elements.add(new Edge.Builder()
                .group(BASIC_EDGE1)
                .source("A")
                .dest("B")
                .directed(true)
                .property(PROPERTY1, "q")
                .property(COUNT, 1)
                .build());

        elements.add(new Edge.Builder()
                .group(BASIC_EDGE1)
                .source("B")
                .dest("C")
                .directed(true)
                .property(PROPERTY1, "q")
                .property(COUNT, 1)
                .build());

        return elements;
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
