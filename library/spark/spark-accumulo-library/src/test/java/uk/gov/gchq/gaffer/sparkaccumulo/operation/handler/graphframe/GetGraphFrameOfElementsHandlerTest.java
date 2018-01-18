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

package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.graphframe;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.spark.SparkSessionProvider;
import uk.gov.gchq.gaffer.spark.operation.graphframe.GetGraphFrameOfElements;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GetGraphFrameOfElementsHandlerTest {

    private static final int NUM_ELEMENTS = 10;

    @Test
    public void shouldGetCorrectElementsInGraphFrame() throws OperationException {
        final Graph graph = getGraph("/schema-GraphFrame/elements.json", getElements());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final GetGraphFrameOfElements gfOperation = new GetGraphFrameOfElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();

        final GraphFrame graphFrame = graph.execute(gfOperation, new User());

        final Set<String> vertices = graphFrame.vertices()
                .javaRDD()
                .map(row -> Sets.newHashSet(Arrays.asList(row.mkString(",").split(","))))
                .collect()
                .stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        final Set<String> edges = graphFrame.edges()
                .javaRDD()
                .map(row -> Sets.newHashSet(Arrays.asList(row.mkString(",").split(","))))
                .collect()
                .stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        edges.remove("null");
        vertices.remove("null");

        assertThat(vertices, hasSize(13));
        assertThat(vertices, hasItems("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "B", "C"));
        assertThat(vertices, hasItem(TestGroups.ENTITY));

        assertThat(edges, hasSize(24));
        assertThat(edges, hasItems("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "B", "C"));
        assertThat(edges, hasItems("10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"));
        assertThat(edges, hasItem(TestGroups.EDGE));
    }

    @Test
    public void shouldGetCorrectElementsInGraphFrameWithMultipleGroups() throws OperationException {
        final Graph graph = getGraph("/schema-GraphFrame/elements.json", getElements());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final GetGraphFrameOfElements gfOperation = new GetGraphFrameOfElements.Builder()
                .view(new View.Builder()
                        .edges(Lists.newArrayList(TestGroups.EDGE, TestGroups.EDGE_2))
                        .entities(Lists.newArrayList(TestGroups.ENTITY, TestGroups.ENTITY_2))
                        .build())
                .build();

        final GraphFrame graphFrame = graph.execute(gfOperation, new User());

        final Set<String> vertices = graphFrame.vertices()
                .javaRDD()
                .map(row -> Sets.newHashSet(Arrays.asList(row.mkString(",").split(","))))
                .collect()
                .stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        final Set<String> edges = graphFrame.edges()
                .javaRDD()
                .map(row -> Sets.newHashSet(Arrays.asList(row.mkString(",").split(","))))
                .collect()
                .stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        edges.remove("null");
        vertices.remove("null");

        assertThat(vertices, hasSize(14));
        assertThat(vertices, hasItems("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "B", "C"));
        assertThat(vertices, hasItems(TestGroups.ENTITY, TestGroups.ENTITY_2));

        assertThat(edges, hasSize(24));
        assertThat(edges, hasItems("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "B", "C"));
        assertThat(edges, hasItems("10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"));
        assertThat(edges, hasItem(TestGroups.EDGE));
    }

    @Test
    public void shouldGetCorrectElementsInGraphFrameWithVertexProperty() throws OperationException {

        final List<Element> elements = getElements();

        // Set the property on all entities
        for (final Element element : elements) {
            if (element instanceof Entity && ((Entity) element).getGroup().equals(TestGroups.ENTITY)) {
                element.putProperty("vertex", "value");
            }
        }

        final Graph graph = getGraph("/schema-GraphFrame/elementsWithVertexProperty.json", elements);
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final GetGraphFrameOfElements gfOperation = new GetGraphFrameOfElements.Builder()
                .view(new View.Builder()
                        .edges(Lists.newArrayList(TestGroups.EDGE, TestGroups.EDGE_2))
                        .entities(Lists.newArrayList(TestGroups.ENTITY, TestGroups.ENTITY_2))
                        .build())
                .build();

        try {
            final GraphFrame graphFrame = graph.execute(gfOperation, new User());

            fail("Validation in the GetDataFrameOfElementsHandler should result in an exception being thrown.");
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains("schema contains property [vertex]"));
        }
    }

    @Test
    public void shouldGetCorrectElementsInGraphFrameWithNoEdges() throws OperationException {

        final Graph graph = getGraph("/schema-GraphFrame/elements.json", getElements());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final GetGraphFrameOfElements gfOperation = new GetGraphFrameOfElements.Builder()
                .view(new View.Builder()
                        .entities(Lists.newArrayList(TestGroups.ENTITY, TestGroups.ENTITY_2))
                        .build())
                .build();

        try {
            final GraphFrame graphFrame = graph.execute(gfOperation, new User());

            fail("Validation in the GetGraphFrameOfElements operation should result in an exception being thrown.");
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains("Cannot create a Graphframe unless the View contains edges."));
        }
    }

    @Test
    public void shouldBehaviourInGraphFrameWithNoEntities() throws OperationException {

        final Graph graph = getGraph("/schema-GraphFrame/elements.json", getElements());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final GetGraphFrameOfElements gfOperation = new GetGraphFrameOfElements.Builder()
                .view(new View.Builder()
                        .edges(Lists.newArrayList(TestGroups.EDGE, TestGroups.EDGE_2))
                        .build())
                .build();

        final GraphFrame graphFrame = graph.execute(gfOperation, new User());

        final Set<String> vertices = graphFrame.vertices()
                .javaRDD()
                .map(row -> Sets.newHashSet(Arrays.asList(row.mkString(",").split(","))))
                .collect()
                .stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        final Set<String> edges = graphFrame.edges()
                .javaRDD()
                .map(row -> Sets.newHashSet(Arrays.asList(row.mkString(",").split(","))))
                .collect()
                .stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        edges.remove("null");
        vertices.remove("null");

        assertThat(vertices, hasSize(12));
        assertThat(vertices, hasItems("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "B", "C"));

        assertThat(edges, hasSize(24));
        assertThat(edges, hasItems("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "B", "C"));
        assertThat(edges, hasItems("10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"));
        assertThat(edges, hasItem(TestGroups.EDGE));
    }

    @Test
    public void shouldGetCorrectElementsInGraphFrameWithNoElements() throws OperationException {

        final Graph graph = getGraph("/schema-GraphFrame/elements.json", new ArrayList<>());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final GetGraphFrameOfElements gfOperation = new GetGraphFrameOfElements.Builder()
                .view(new View.Builder()
                        .entities(Lists.newArrayList(TestGroups.ENTITY))
                        .edges(Lists.newArrayList(TestGroups.EDGE, TestGroups.EDGE_2))
                        .build())
                .build();

        final GraphFrame graphFrame = graph.execute(gfOperation, new User());

        assertTrue(graphFrame.edges().javaRDD().isEmpty());
        assertTrue(graphFrame.vertices().javaRDD().isEmpty());
    }

    @Test
    public void shouldGetCorrectElementsInGraphFrameWithMatchingVertexNamesInDifferentGroups() throws OperationException {
        final Graph graph = getGraph("/schema-GraphFrame/elements.json", getElements());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final GetGraphFrameOfElements gfOperation = new GetGraphFrameOfElements.Builder()
                .view(new View.Builder()
                        .entities(Lists.newArrayList(TestGroups.ENTITY, TestGroups.ENTITY_2))
                        .edges(Lists.newArrayList(TestGroups.EDGE))
                        .build())
                .build();

        final GraphFrame graphFrame = graph.execute(gfOperation, new User());

        final Set<String> vertices = graphFrame.vertices()
                .javaRDD()
                .map(row -> Sets.newHashSet(Arrays.asList(row.mkString(",").split(","))))
                .collect()
                .stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        vertices.remove("null");

        assertThat(vertices, hasSize(14));
        assertThat(vertices, hasItems("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "B", "C"));
        assertThat(vertices, hasItem(TestGroups.ENTITY));
        assertThat(vertices, hasItem(TestGroups.ENTITY_2));
    }

    @Test
    public void shouldGetCorrectElementsInGraphFrameWithMultipleEdgesBetweenVertices() throws OperationException {
        final Graph graph = getGraph("/schema-GraphFrame/elements.json", getElements());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final Edge edge1 = new Edge.Builder().group(TestGroups.EDGE_2)
                .source("1")
                .dest("B")
                .directed(true)
                .property("columnQualifier", 1)
                .property("property1", 2)
                .property("property2", 3.0F)
                .property("property3", 4.0D)
                .property("property4", 5L)
                .property("count", 100L)
                .build();

        final Edge edge2 = new Edge.Builder().group(TestGroups.EDGE_2)
                .source("1")
                .dest("C")
                .directed(true)
                .property("columnQualifier", 6)
                .property("property1", 7)
                .property("property2", 8.0F)
                .property("property3", 9.0D)
                .property("property4", 10L)
                .property("count", 200L)
                .build();

        graph.execute(new AddElements.Builder().input(edge1, edge2).build(), new User());

        final GetGraphFrameOfElements gfOperation = new GetGraphFrameOfElements.Builder()
                .view(new View.Builder()
                        .entities(Lists.newArrayList(TestGroups.ENTITY, TestGroups.ENTITY_2))
                        .edges(Lists.newArrayList(TestGroups.EDGE, TestGroups.EDGE_2))
                        .build())
                .build();

        final GraphFrame graphFrame = graph.execute(gfOperation, new User());

        final Set<String> edges = graphFrame.edges()
                .javaRDD()
                .map(row -> Sets.newHashSet(Arrays.asList(row.mkString(",").split(","))))
                .collect()
                .stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        edges.remove("null");

        assertThat(edges, hasSize(25));
        assertThat(edges, hasItems("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "B", "C"));
        assertThat(edges, hasItems("10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"));
        assertThat(edges, hasItem(TestGroups.EDGE));
        assertThat(edges, hasItem(TestGroups.EDGE_2));
    }

    @Test
    public void shouldGetCorrectElementsInGraphFrameWithLoops() throws OperationException {
        final Graph graph = getGraph("/schema-GraphFrame/elements.json", getElements());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final Edge edge1 = new Edge.Builder().group(TestGroups.EDGE)
                .source("B")
                .dest("1")
                .directed(true)
                .property("columnQualifier", 1)
                .property("property1", 2)
                .property("property2", 3.0F)
                .property("property3", 4.0D)
                .property("property4", 5L)
                .property("count", 100L)
                .build();

        final Edge edge2 = new Edge.Builder().group(TestGroups.EDGE)
                .source("C")
                .dest("1")
                .directed(true)
                .property("columnQualifier", 6)
                .property("property1", 7)
                .property("property2", 8.0F)
                .property("property3", 9.0D)
                .property("property4", 10L)
                .property("count", 200L)
                .build();

        graph.execute(new AddElements.Builder().input(edge1, edge2).build(), new User());

        final GetGraphFrameOfElements gfOperation = new GetGraphFrameOfElements.Builder()
                .view(new View.Builder()
                        .entities(Lists.newArrayList(TestGroups.ENTITY))
                        .edges(Lists.newArrayList(TestGroups.EDGE))
                        .build())
                .build();

        final GraphFrame graphFrame = graph.execute(gfOperation, new User());

        final Set<String> edges = graphFrame.edges()
                .javaRDD()
                .map(row -> Sets.newHashSet(Arrays.asList(row.mkString(",").split(","))))
                .collect()
                .stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        edges.remove("null");

        assertThat(edges, hasSize(26));
        assertThat(edges, hasItems("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "B", "C"));
        assertThat(edges, hasItems("10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"));
        assertThat(edges, hasItem(TestGroups.EDGE));
    }

    @Test
    public void shouldGetCorrectElementsInGraphFrameWithRepeatedElements() throws OperationException {
        final Graph graph = getGraph("/schema-GraphFrame/elements.json", getElements());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        graph.execute(new AddElements.Builder().input(getElements()).build(), new User());

        final GetGraphFrameOfElements gfOperation = new GetGraphFrameOfElements.Builder()
                .view(new View.Builder()
                        .entities(Lists.newArrayList(TestGroups.ENTITY))
                        .edges(Lists.newArrayList(TestGroups.EDGE))
                        .build())
                .build();

        final GraphFrame graphFrame = graph.execute(gfOperation, new User());

        final Set<String> edges = graphFrame.edges()
                .javaRDD()
                .map(row -> Sets.newHashSet(Arrays.asList(row.mkString(",").split(","))))
                .collect()
                .stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        final Set<String> vertices = graphFrame.vertices()
                .javaRDD()
                .map(row -> Sets.newHashSet(Arrays.asList(row.mkString(",").split(","))))
                .collect()
                .stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        edges.remove("null");
        vertices.remove("null");

        assertThat(edges, hasSize(24));
        assertThat(edges, hasItems("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "B", "C"));
        assertThat(edges, hasItems("10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"));
        assertThat(edges, hasItem(TestGroups.EDGE));

        assertThat(vertices, hasSize(13));
        assertThat(vertices, hasItems("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "B", "C"));
        assertThat(vertices, hasItem(TestGroups.ENTITY));
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

    static List<Element> getElements() {
        final List<Element> elements = new ArrayList<>();

        final Entity entityB = new Entity.Builder().group(TestGroups.ENTITY)
                .vertex("B")
                .property("columnQualifier", 1)
                .property("count", 1L)
                .build();

        final Entity entityC = new Entity.Builder().group(TestGroups.ENTITY)
                .vertex("C")
                .property("columnQualifier", 1)
                .property("count", 1L)
                .build();

        elements.add(entityB);
        elements.add(entityC);

        for (int i = 0; i < NUM_ELEMENTS; i++) {
            final Entity entity1 = new Entity.Builder().group(TestGroups.ENTITY)
                    .vertex("" + i)
                    .property("columnQualifier", 1)
                    .property("property1", i)
                    .property("property2", 3.0F)
                    .property("property3", 4.0D)
                    .property("property4", 5L)
                    .property("count", 6L)
                    .build();

            final Entity entity2 = new Entity.Builder().group(TestGroups.ENTITY_2)
                    .vertex("" + i)
                    .property("columnQualifier", 1)
                    .property("property1", i)
                    .property("property2", 3.0F)
                    .property("property3", 4.0D)
                    .property("property4", 5L)
                    .property("count", 6L)
                    .build();


            final Edge edge1 = new Edge.Builder().group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("B")
                    .directed(true)
                    .property("columnQualifier", 1)
                    .property("property1", 2)
                    .property("property2", 3.0F)
                    .property("property3", 4.0D)
                    .property("property4", 5L)
                    .property("count", 100L)
                    .build();

            final Edge edge2 = new Edge.Builder().group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("C")
                    .directed(true)
                    .property("columnQualifier", 6)
                    .property("property1", 7)
                    .property("property2", 8.0F)
                    .property("property3", 9.0D)
                    .property("property4", 10L)
                    .property("count", i * 200L)
                    .build();

            elements.add(edge1);
            elements.add(edge2);
            elements.add(entity1);
            elements.add(entity2);
        }
        return elements;
    }
}
