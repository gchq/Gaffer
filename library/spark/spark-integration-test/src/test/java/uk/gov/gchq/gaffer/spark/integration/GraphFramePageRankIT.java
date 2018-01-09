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

package uk.gov.gchq.gaffer.spark.integration;

import com.google.common.collect.Lists;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Map;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.spark.SparkSessionProvider;
import uk.gov.gchq.gaffer.spark.algorithm.GraphFramePageRank;
import uk.gov.gchq.gaffer.spark.data.generator.RowToElementGenerator;
import uk.gov.gchq.gaffer.spark.function.GraphFrameToIterableRow;
import uk.gov.gchq.gaffer.spark.operation.graphframe.GetGraphFrameOfElements;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GraphFramePageRankIT {

    @Test
    public void shouldGetCorrectPageRankForGraphFrameUsingMaxIterations() throws OperationException {
        final Graph graph = getGraph("/schema-GraphFrame/elements.json", getElements());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(new GetGraphFrameOfElements.Builder()
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE)
                                .entity(TestGroups.ENTITY)
                                .build())
                        .build())
                .then(new GraphFramePageRank.Builder()
                        .maxIterations(20)
                        .build())
                .then(new Map.Builder<>()
                        .first(new GraphFrameToIterableRow())
                        .then(new RowToElementGenerator())
                        .build())
                .build();

        final Iterable<? extends Element> results = graph.execute(opChain, new User());

        final java.util.Map<Object, Entity> map = Streams.toStream(results)
                .filter(e -> e instanceof Entity)
                .map(e -> (Entity) e)
                .collect(Collectors.toMap(Entity::getVertex, Function.identity(), (a, b) -> a));

        assertEquals(1.49, (Double) map.get("a").getProperty("pagerank"), 1E-1);
        assertEquals(0.78, (Double) map.get("b").getProperty("pagerank"), 1E-1);
        assertEquals(1.58, (Double) map.get("c").getProperty("pagerank"), 1E-1);
        assertEquals(0.15, (Double) map.get("d").getProperty("pagerank"), 1E-1);
    }

    @Test
    public void shouldGetCorrectPageRankForGraphFrameUsingTolerance() throws OperationException {
        final Graph graph = getGraph("/schema-GraphFrame/elements.json", getElements());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(new GetGraphFrameOfElements.Builder()
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE)
                                .entity(TestGroups.ENTITY)
                                .build())
                        .build())
                .then(new GraphFramePageRank.Builder()
                        .tolerance(1E-2)
                        .build())
                .then(new Map.Builder<>()
                        .first(new GraphFrameToIterableRow())
                        .then(new RowToElementGenerator())
                        .build())
                .build();

        final Iterable<? extends Element> results = graph.execute(opChain, new User());

        final java.util.Map<Object, Entity> map = Streams.toStream(results)
                .filter(e -> e instanceof Entity)
                .map(e -> (Entity) e)
                .collect(Collectors.toMap(Entity::getVertex, Function.identity(), (a, b) -> a));

        assertEquals(1.49, (Double) map.get("a").getProperty("pagerank"), 1E-1);
        assertEquals(0.78, (Double) map.get("b").getProperty("pagerank"), 1E-1);
        assertEquals(1.58, (Double) map.get("c").getProperty("pagerank"), 1E-1);
        assertEquals(0.15, (Double) map.get("d").getProperty("pagerank"), 1E-1);
    }

    @Test
    public void shouldGetCorrectPageRankForGraphFrameWithResetProbability() throws OperationException {
        final Graph graph = getGraph("/schema-GraphFrame/elements.json", getElements());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(new GetGraphFrameOfElements.Builder()
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE)
                                .entity(TestGroups.ENTITY)
                                .build())
                        .build())
                .then(new GraphFramePageRank.Builder()
                        .tolerance(1E-1)
                        .resetProbability(0.5)
                        .build())
                .then(new Map.Builder<>()
                        .first(new GraphFrameToIterableRow())
                        .then(new RowToElementGenerator())
                        .build())
                .build();

        final Iterable<? extends Element> results = graph.execute(opChain, new User());

        final java.util.Map<Object, Entity> map = Streams.toStream(results)
                .filter(e -> e instanceof Entity)
                .map(e -> (Entity) e)
                .collect(Collectors.toMap(Entity::getVertex, Function.identity(), (a, b) -> a));

        assertEquals(1.21, (Double) map.get("a").getProperty("pagerank"), 1E-1);
        assertEquals(0.78, (Double) map.get("b").getProperty("pagerank"), 1E-1);
        assertEquals(1.43, (Double) map.get("c").getProperty("pagerank"), 1E-1);
        assertEquals(0.54, (Double) map.get("d").getProperty("pagerank"), 1E-1);
    }

    @Test
    public void shouldGetCorrectPageRankForGraphFrameWithNoEntities() throws OperationException {
        final Graph graph = getGraph("/schema-GraphFrame/elements.json", getElements());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final OperationChain<GraphFrame> opChain = new OperationChain.Builder()
                .first(new GetGraphFrameOfElements.Builder()
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE)
                                .build())
                        .build())
                .then(new GraphFramePageRank.Builder()
                        .maxIterations(20)
                        .build())
                .build();

        try {
            final GraphFrame result = graph.execute(opChain, new User());

            fail("Expected exception when the View does not contain both edges and entities.");
        } catch (final IllegalArgumentException ex) {
            assertThat(ex, is(instanceOf(IllegalArgumentException.class)));
            assertThat(ex.getMessage(), containsString("Cannot create a Graphframe unless the View contains both edges and entities."));
        }
    }

    @Test
    public void shouldGetCorrectPageRankForGraphFrameWithNoEdges() throws OperationException {
        final Graph graph = getGraph("/schema-GraphFrame/elements.json", getElements());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final OperationChain<GraphFrame> opChain = new OperationChain.Builder()
                .first(new GetGraphFrameOfElements.Builder()
                        .view(new View.Builder()
                                .entity(TestGroups.ENTITY)
                                .build())
                        .build())
                .then(new GraphFramePageRank.Builder()
                        .maxIterations(20)
                        .build())
                .build();

        try {
            final GraphFrame result = graph.execute(opChain, new User());

            fail("Expected exception when the View does not contain both edges and entities.");
        } catch (final IllegalArgumentException ex) {
            assertThat(ex, is(instanceOf(IllegalArgumentException.class)));
            assertThat(ex.getMessage(), containsString("Cannot create a Graphframe unless the View contains both edges and entities."));
        }
    }

    @Test
    public void shouldGetCorrectElementsInGraphFrameWithMultipleEntityTypes() throws OperationException {
        final Graph graph = getGraph("/schema-GraphFrame/elements.json", getElementsWithMultipleEntityTypes());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(new GetGraphFrameOfElements.Builder()
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE)
                                .entities(Lists.newArrayList(TestGroups.ENTITY, TestGroups.ENTITY_2))
                                .build())
                        .build())
                .then(new GraphFramePageRank.Builder()
                        .maxIterations(20)
                        .build())
                .then(new Map.Builder<>()
                        .first(new GraphFrameToIterableRow())
                        .then(new RowToElementGenerator())
                        .build())
                .build();

        final Iterable<? extends Element> results = graph.execute(opChain, new User());

        final java.util.Map<Object, Entity> map = Streams.toStream(results)
                .filter(e -> e instanceof Entity)
                .map(e -> (Entity) e)
                .collect(Collectors.toMap(Entity::getVertex, Function.identity(), (a, b) -> a));

        assertEquals(1.49, (Double) map.get("a").getProperty("pagerank"), 1E-1);
        assertEquals(0.78, (Double) map.get("b").getProperty("pagerank"), 1E-1);
        assertEquals(1.58, (Double) map.get("c").getProperty("pagerank"), 1E-1);
        assertEquals(0.15, (Double) map.get("d").getProperty("pagerank"), 1E-1);
    }

    @Test
    public void shouldGetCorrectElementsInGraphFrameWithMultipleEdgeTypes() throws OperationException {
        final Graph graph = getGraph("/schema-GraphFrame/elements.json", getElementsWithMultipleEntityTypes());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(new GetGraphFrameOfElements.Builder()
                        .view(new View.Builder()
                                .edges(Lists.newArrayList(TestGroups.EDGE, TestGroups.EDGE_2))
                                .entity(TestGroups.ENTITY)
                                .build())
                        .build())
                .then(new GraphFramePageRank.Builder()
                        .maxIterations(20)
                        .build())
                .then(new Map.Builder<>()
                        .first(new GraphFrameToIterableRow())
                        .then(new RowToElementGenerator())
                        .build())
                .build();

        final Iterable<? extends Element> results = graph.execute(opChain, new User());

        final java.util.Map<Object, Entity> map = Streams.toStream(results)
                .filter(e -> e instanceof Entity)
                .map(e -> (Entity) e)
                .collect(Collectors.toMap(Entity::getVertex, Function.identity(), (a, b) -> a));

        assertEquals(1.49, (Double) map.get("a").getProperty("pagerank"), 1E-1);
        assertEquals(0.78, (Double) map.get("b").getProperty("pagerank"), 1E-1);
        assertEquals(1.58, (Double) map.get("c").getProperty("pagerank"), 1E-1);
        assertEquals(0.15, (Double) map.get("d").getProperty("pagerank"), 1E-1);
    }

    @Test
    public void shouldGetCorrectPageRankForGraphFrameWithRepeatedElements() throws OperationException {
        final Graph graph = getGraph("/schema-GraphFrame/elements.json", getElements());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        graph.execute(new AddElements.Builder().input(getElements()).build(), new User());

        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(new GetGraphFrameOfElements.Builder()
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE)
                                .entity(TestGroups.ENTITY).build())
                        .build())
                .then(new GraphFramePageRank.Builder()
                        .maxIterations(20)
                        .build())
                .then(new Map.Builder<>()
                        .first(new GraphFrameToIterableRow())
                        .then(new RowToElementGenerator())
                        .build())
                .build();

        final Iterable<? extends Element> results = graph.execute(opChain, new User());

        final java.util.Map<Object, Entity> map = Streams.toStream(results)
                .filter(e -> e instanceof Entity)
                .map(e -> (Entity) e)
                .collect(Collectors.toMap(Entity::getVertex, Function.identity(), (a, b) -> a));

        assertEquals(1.49, (Double) map.get("a").getProperty("pagerank"), 1E-1);
        assertEquals(0.78, (Double) map.get("b").getProperty("pagerank"), 1E-1);
        assertEquals(1.58, (Double) map.get("c").getProperty("pagerank"), 1E-1);
        assertEquals(0.15, (Double) map.get("d").getProperty("pagerank"), 1E-1);
    }

    @Test
    public void shouldGetCorrectPageRankForGraphFrameWithSelfLoop() throws OperationException {
        final Graph graph = getGraph("/schema-GraphFrame/elements.json", getElements());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final Edge selfLoop = new Edge.Builder().group(TestGroups.EDGE)
                .source("a")
                .dest("a")
                .directed(true)
                .property("type", "friend")
                .build();

        graph.execute(new AddElements.Builder().input(selfLoop).build(), new User());

        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(new GetGraphFrameOfElements.Builder()
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE)
                                .entity(TestGroups.ENTITY).build())
                        .build())
                .then(new GraphFramePageRank.Builder()
                        .maxIterations(20)
                        .build())
                .then(new Map.Builder<>()
                        .first(new GraphFrameToIterableRow())
                        .then(new RowToElementGenerator())
                        .build())
                .build();

        final Iterable<? extends Element> results = graph.execute(opChain, new User());

        final java.util.Map<Object, Entity> map = Streams.toStream(results)
                .filter(e -> e instanceof Entity)
                .map(e -> (Entity) e)
                .collect(Collectors.toMap(Entity::getVertex, Function.identity(), (a, b) -> a));

        assertEquals(1.82, (Double) map.get("a").getProperty("pagerank"), 1E-1);
        assertEquals(0.67, (Double) map.get("b").getProperty("pagerank"), 1E-1);
        assertEquals(1.36, (Double) map.get("c").getProperty("pagerank"), 1E-1);
        assertEquals(0.15, (Double) map.get("d").getProperty("pagerank"), 1E-1);
    }

    @Test
    public void shouldGetCorrectPageRankForGraphFrameWithMultipleEdgesBetweenVertices() throws OperationException {
        final Graph graph = getGraph("/schema-GraphFrame/elements.json", getElements());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final Edge edge1 = new Edge.Builder()
                .source("a")
                .dest("b")
                .directed(true)
                .group(TestGroups.EDGE_2)
                .property("type", "friend")
                .build();

        graph.execute(new AddElements.Builder()
                .input(edge1)
                .build(), new User());

        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(new GetGraphFrameOfElements.Builder()
                        .view(new View.Builder()
                                .edges(Lists.newArrayList(TestGroups.EDGE, TestGroups.EDGE_2))
                                .entity(TestGroups.ENTITY).build())
                        .build())
                .then(new GraphFramePageRank.Builder()
                        .maxIterations(20)
                        .build())
                .then(new Map.Builder<>()
                        .first(new GraphFrameToIterableRow())
                        .then(new RowToElementGenerator())
                        .build())
                .build();

        final Iterable<? extends Element> results = graph.execute(opChain, new User());

        final java.util.Map<Object, Entity> map = Streams.toStream(results)
                .filter(e -> e instanceof Entity)
                .map(e -> (Entity) e)
                .collect(Collectors.toMap(Entity::getVertex, Function.identity(), (a, b) -> a));

        assertEquals(1.43, (Double) map.get("a").getProperty("pagerank"), 1E-1);
        assertEquals(0.96, (Double) map.get("b").getProperty("pagerank"), 1E-1);
        assertEquals(1.51, (Double) map.get("c").getProperty("pagerank"), 1E-1);
        assertEquals(0.15, (Double) map.get("d").getProperty("pagerank"), 1E-1);
    }

    private Graph getGraph(final String elementsSchema, final List<Element> elements) throws OperationException {
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .build())
                .addSchema(getClass().getResourceAsStream(elementsSchema))
                .addSchema(getClass().getResourceAsStream("/schema-GraphFrame/types.json"))
                .storeProperties(getClass().getResourceAsStream("/store.properties"))
                .build();
        graph.execute(new AddElements.Builder().input(elements).build(), new User());
        return graph;
    }

    private static List<Element> getElements() {
        final List<String> names = Lists.newArrayList("Alice", "Bob", "Charlie", "David");
        final List<Element> elements = new ArrayList<>();

        final List<Entity> entities = names.stream().map(n -> {
            return new Entity.Builder().vertex(n.substring(0, 1).toLowerCase()).group(TestGroups.ENTITY).build();
        }).collect(Collectors.toList());

        final Edge edge1 = new Edge.Builder().source("a").dest("b").directed(true).group(TestGroups.EDGE)
                .property("type", "friend").build();
        final Edge edge2 = new Edge.Builder().source("b").dest("c").directed(true).group(TestGroups.EDGE)
                .property("type", "follow").build();
        final Edge edge3 = new Edge.Builder().source("a").dest("c").directed(true).group(TestGroups.EDGE)
                .property("type", "follow").build();
        final Edge edge4 = new Edge.Builder().source("c").dest("a").directed(true).group(TestGroups.EDGE)
                .property("type", "follow").build();
        final Edge edge5 = new Edge.Builder().source("d").dest("c").directed(true).group(TestGroups.EDGE)
                .property("type", "follow").build();

        final List<Edge> edges = Lists.newArrayList(edge1, edge2, edge3, edge4, edge5);

        elements.addAll(entities);
        elements.addAll(edges);

        return elements;
    }

    private static List<Element> getElementsWithMultipleEntityTypes() {
        final List<String> names = Lists.newArrayList("Alice", "Bob", "Charlie", "David");
        final List<Element> elements = new ArrayList<>();

        final List<Entity> entities = names.stream()
                .map(n -> {
                    final Entity a = new Entity.Builder().vertex(n.substring(0, 1).toLowerCase()).group(TestGroups.ENTITY).build();
                    final Entity b = new Entity.Builder().vertex(n.substring(0, 1).toLowerCase()).group(TestGroups.ENTITY_2).build();
                    return Lists.newArrayList(a, b);
                })
                .flatMap(List::stream)
                .collect(Collectors.toList());

        final Edge edge1 = new Edge.Builder()
                .source("a")
                .dest("b")
                .directed(true)
                .group(TestGroups.EDGE)
                .property("type", "friend")
                .build();

        final Edge edge2 = new Edge.Builder()
                .source("b")
                .dest("c")
                .directed(true)
                .group(TestGroups.EDGE)
                .property("type", "follow")
                .build();

        final Edge edge3 = new Edge.Builder()
                .source("a")
                .dest("c")
                .directed(true)
                .group(TestGroups.EDGE)
                .property("type", "follow")
                .build();

        final Edge edge4 = new Edge.Builder()
                .source("c")
                .dest("a")
                .directed(true)
                .group(TestGroups.EDGE)
                .property("type", "follow")
                .build();

        final Edge edge5 = new Edge.Builder()
                .source("d")
                .dest("c")
                .directed(true)
                .group(TestGroups.EDGE)
                .property("type", "follow")
                .build();

        final List<Edge> edges = Lists.newArrayList(edge1, edge2, edge3, edge4, edge5);

        elements.addAll(entities);
        elements.addAll(edges);

        return elements;
    }

    private static List<Element> getElementsWithMultipleEdgeTypes() {
        final List<String> names = Lists.newArrayList("Alice", "Bob", "Charlie", "David");
        final List<Element> elements = new ArrayList<>();

        final List<Entity> entities = names.stream().map(n -> {
            return new Entity.Builder().vertex(n.substring(0, 1).toLowerCase()).group(TestGroups.ENTITY).build();
        }).collect(Collectors.toList());

        final Edge edge1 = new Edge.Builder()
                .source("a")
                .dest("b")
                .directed(true)
                .group(TestGroups.EDGE)
                .property("type", "friend")
                .build();

        final Edge edge2 = new Edge.Builder()
                .source("b")
                .dest("c")
                .directed(true)
                .group(TestGroups.EDGE_2)
                .property("type", "follow")
                .build();

        final Edge edge3 = new Edge.Builder()
                .source("a")
                .dest("c")
                .directed(true)
                .group(TestGroups.EDGE_2)
                .property("type", "follow")
                .build();

        final Edge edge4 = new Edge.Builder()
                .source("c")
                .dest("a")
                .directed(true)
                .group(TestGroups.EDGE_2)
                .property("type", "follow")
                .build();

        final Edge edge5 = new Edge.Builder()
                .source("d")
                .dest("c")
                .directed(true)
                .group(TestGroups.EDGE_2)
                .property("type", "follow")
                .build();

        final List<Edge> edges = Lists.newArrayList(edge1, edge2, edge3, edge4, edge5);

        elements.addAll(entities);
        elements.addAll(edges);

        return elements;
    }
}
