/*
 * Copyright 2022 Crown Copyright
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
package uk.gov.gchq.gaffer.accumulostore.integration;

import com.google.common.collect.Lists;
import org.assertj.core.api.Condition;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.integration.StandaloneIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class AccumuloMatchedVertexIT extends StandaloneIT {
    private static final String VERTEX = "vertex";

    private final User user = new User();

    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloStoreITs.class));

    private static Edge getEdgeWithSourceMatch() {
        return new Edge.Builder()
                .group(TestGroups.EDGE)
                .source(VERTEX)
                .dest("dest")
                .directed(true)
                .build();
    }

    private static Edge getEdgeWithDestinationMatch() {
        return new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("src")
                .dest(VERTEX)
                .directed(true)
                .build();
    }

    private static Stream<Arguments> getEdgeVariants() {
        return Stream.of(
                Arguments.of(getEdgeWithSourceMatch()),
                Arguments.of(getEdgeWithDestinationMatch())
        );
    }

    Condition<Edge> matchedVertex = new Condition<>(
        edge -> null != edge.getMatchedVertex(), "matched vertex");

    @ParameterizedTest
    @MethodSource("getEdgeVariants")
    public void shouldHaveMatchedVertexWithEntityInputAndBothView(Edge edge) throws OperationException {
        // Given
        final Graph graph = createGraph(edge);
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed(VERTEX))
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements, user));
        final List<Edge> edgeResults = getEdgeResults(results);

        // Then
        assertThat(results).hasSize(2);
        assertThat(edgeResults).hasSize(1);
        assertThat(edgeResults).have(matchedVertex);
    }

    @ParameterizedTest
    @MethodSource("getEdgeVariants")
    public void shouldNotHaveMatchedVertexWithEntityInputAndEntityView(Edge edge) throws OperationException {
        // Given
        final Graph graph = createGraph(edge);
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed(VERTEX))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements, user));
        final List<Edge> edgeResults = getEdgeResults(results);

        // Then
        assertThat(results).hasSize(1);
        assertThat(edgeResults).hasSize(0);
    }

    @ParameterizedTest
    @MethodSource("getEdgeVariants")
    public void shouldHaveMatchedVertexWithEntityInputAndEdgeView(Edge edge) throws OperationException {
        // Given
        final Graph graph = createGraph(edge);
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed(VERTEX))
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements, user));
        final List<Edge> edgeResults = getEdgeResults(results);

        // Then
        assertThat(results).hasSize(1);
        assertThat(edgeResults).hasSize(1);
        assertThat(edgeResults).have(matchedVertex);
    }

    @ParameterizedTest
    @MethodSource("getEdgeVariants")
    public void shouldNotHaveMatchedVertexWithEdgeInputAndBothView(Edge edge) throws OperationException {
        // Given
        final Graph graph = createGraph(edge);
        final GetElements getElements = new GetElements.Builder()
                .input(edge)
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements, user));
        final List<Edge> edgeResults = getEdgeResults(results);

        // Then
        assertThat(results).hasSize(2);
        assertThat(edgeResults).hasSize(1);
        assertThat(edgeResults).doNotHave(matchedVertex);
    }

    @ParameterizedTest
    @MethodSource("getEdgeVariants")
    public void shouldNotHaveMatchedVertexWithEdgeInputAndEdgeView(Edge edge) throws OperationException {
        // Given
        final Graph graph = createGraph(edge);
        final GetElements getElements = new GetElements.Builder()
                .input(edge)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements, user));
        final List<Edge> edgeResults = getEdgeResults(results);

        // Then
        assertThat(results).hasSize(1);
        assertThat(edgeResults).hasSize(1);
        assertThat(edgeResults).doNotHave(matchedVertex);
    }

    @ParameterizedTest
    @MethodSource("getEdgeVariants")
    public void shouldNotHaveMatchedVertexWithEdgeInputAndEntityView(Edge edge) throws OperationException {
        // Given
        final Graph graph = createGraph(edge);
        final GetElements getElements = new GetElements.Builder()
                .input(edge)
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements, user));
        final List<Edge> edgeResults = getEdgeResults(results);

        // Then
        assertThat(results).hasSize(1);
        assertThat(edgeResults).hasSize(0);
    }

    @ParameterizedTest
    @MethodSource("getEdgeVariants")
    public void shouldNotHaveMatchedVertexWithBothInputAndBothView(Edge edge) throws OperationException {
        // Given
        final Graph graph = createGraph(edge);
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed(VERTEX), edge)
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements, user));
        final List<Edge> edgeResults = getEdgeResults(results);

        // Then
        if (edge == getEdgeWithSourceMatch()) {
            assertThat(results).hasSize(2);
            assertThat(edgeResults).hasSize(1);
            assertThat(edgeResults).doNotHave(matchedVertex);
        } else if (edge == getEdgeWithDestinationMatch()) {
            // When the Edge input matches on the dest and the Entity input matches on
            // the src, two versions of the same Edge are returned despite matchedVertex being false
            assertThat(results).hasSize(3);
            assertThat(edgeResults).hasSize(2);
            assertThat(edgeResults).doNotHave(matchedVertex);
        }
    }

    @ParameterizedTest
    @MethodSource("getEdgeVariants")
    public void shouldNotHaveMatchedVertexWithBothInputAndEntityView(Edge edge) throws OperationException {
        // Given
        final Graph graph = createGraph(edge);
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed(VERTEX), edge)
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements, user));
        final List<Edge> edgeResults = getEdgeResults(results);

        // Then
        assertThat(results).hasSize(1);
        assertThat(edgeResults).hasSize(0);
    }

    @ParameterizedTest
    @MethodSource("getEdgeVariants")
    public void shouldTest9(Edge edge) throws OperationException {
        // Given
        final Graph graph = createGraph(edge);
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed(VERTEX), edge)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements, user));
        final List<Edge> edgeResults = getEdgeResults(results);

        // Then
        if (edge == getEdgeWithSourceMatch()) {
            assertThat(results).hasSize(1);
            assertThat(edgeResults).hasSize(1);
            assertThat(edgeResults).doNotHave(matchedVertex);
        } else if (edge == getEdgeWithDestinationMatch()) {
            // When the Edge input matches on the dest and the Entity input matches on
            // the src, two versions of the same Edge are returned despite matchedVertex being false
            assertThat(results).hasSize(2);
            assertThat(edgeResults).hasSize(2);
            assertThat(edgeResults).doNotHave(matchedVertex);
        }
    }

    protected Graph createGraph(Edge edge) throws OperationException {
        Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graph")
                        .build())
                .storeProperties(PROPERTIES)
                .addSchema(new Schema.Builder()
                        .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                                .clazz(String.class)
                                .build())
                        .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                                .vertex(TestTypes.ID_STRING)
                                .build())
                        .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                                .source(TestTypes.ID_STRING)
                                .destination(TestTypes.ID_STRING)
                                .build())
                        .build())
                .build();
        graph.execute(new AddElements.Builder()
                .input(getEntity(), edge)
                .build(), user);
        return graph;
    }

    @Override
    protected Schema createSchema() {
        return new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .build())
                .build();
    }

    protected static Entity getEntity() {
        return new Entity.Builder()
                .vertex(VERTEX)
                .group(TestGroups.ENTITY)
                .build();
    }

    protected static List<Edge> getEdgeResults(List<Element> results) {
        return results.stream().filter(entity -> Edge.class.isInstance(entity)).map(e -> (Edge) e).collect(Collectors.toList());
    }

}
