///*
// * Copyright 2017-2018 Crown Copyright
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package uk.gov.gchq.gaffer.parquetstore.operation.handler.spark.graphframes;
//
//import org.junit.Before;
//import org.junit.Rule;
//import org.junit.Test;
//import org.junit.rules.TemporaryFolder;
//
//import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
//import uk.gov.gchq.gaffer.commonutil.TestGroups;
//import uk.gov.gchq.gaffer.data.element.Edge;
//import uk.gov.gchq.gaffer.data.element.Element;
//import uk.gov.gchq.gaffer.data.element.Entity;
//import uk.gov.gchq.gaffer.graph.Graph;
//import uk.gov.gchq.gaffer.graph.GraphConfig;
//import uk.gov.gchq.gaffer.operation.OperationChain;
//import uk.gov.gchq.gaffer.operation.OperationException;
//import uk.gov.gchq.gaffer.operation.impl.Map;
//import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
//import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
//import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
//import uk.gov.gchq.gaffer.spark.SparkSessionProvider;
//import uk.gov.gchq.gaffer.spark.data.generator.RowToElementGenerator;
//import uk.gov.gchq.gaffer.spark.function.GraphFrameToIterableRow;
//import uk.gov.gchq.gaffer.spark.operation.graphframe.GetGraphFrameOfElements;
//import uk.gov.gchq.gaffer.user.User;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//import static uk.gov.gchq.gaffer.data.util.ElementUtil.assertElementEquals;
//
//public class GetGraphFrameOfElementsHandlerTest {
//    private static final int NUM_ELEMENTS = 10;
//
//    @Rule
//    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
//
//    @Before
//    public void before() {
//        SparkSessionProvider.getSparkSession();
//    }
//
//    @Test
//    public void shouldGetCorrectElementsInGraphFrame() throws OperationException, IOException {
//        // Given
//        final Graph graph = getGraph("shouldGetCorrectElementsInGraphFrame",
//                "/schema-GraphFrame/elements.json", getSimpleElements());
//        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
//                .first(new GetGraphFrameOfElements())
//                .then(new Map.Builder<>()
//                        .first(new GraphFrameToIterableRow())
//                        .then(new RowToElementGenerator())
//                        .build())
//                .build();
//
//        // When
//        final Iterable<? extends Element> results = graph.execute(opChain, new User());
//
//        // Then
//        assertElementEquals(getSimpleElements(), results);
//    }
//
//    @Test
//    public void shouldGetCorrectElementsInGraphFrameWithMultipleEdgesBetweenVertices() throws OperationException, IOException {
//        // Given
//        final Graph graph = getGraph("shouldGetCorrectElementsInGraphFrameWithMultipleEdgesBetweenVertices",
//                "/schema-GraphFrame/elementsComplex.json", getElements());
//        final Edge edge1 = new Edge.Builder().group(TestGroups.EDGE_2)
//                .source("1")
//                .dest("B")
//                .directed(true)
//                .property("columnQualifier", 1)
//                .property("property1", 2)
//                .property("property2", 3.0F)
//                .property("property3", 4.0D)
//                .property("property4", 5L)
//                .property("count", 100L)
//                .build();
//
//        final Edge edge2 = new Edge.Builder().group(TestGroups.EDGE_2)
//                .source("1")
//                .dest("C")
//                .directed(true)
//                .property("columnQualifier", 6)
//                .property("property1", 7)
//                .property("property2", 8.0F)
//                .property("property3", 9.0D)
//                .property("property4", 10L)
//                .property("count", 200L)
//                .build();
//
//        graph.execute(new AddElements.Builder().input(edge1, edge2).build(), new User());
//
//        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
//                .first(new GetGraphFrameOfElements())
//                .then(new Map.Builder<>()
//                        .first(new GraphFrameToIterableRow())
//                        .then(new RowToElementGenerator())
//                        .build())
//                .build();
//
//        // When
//        final Iterable<? extends Element> results = graph.execute(opChain, new User());
//
//        // Then
//        final List<Element> expected = getElements();
//        expected.add(edge1);
//        expected.add(edge2);
//
//        assertElementEquals(expected, results);
//    }
//
//    @Test
//    public void shouldGetCorrectElementsInGraphFrameWithLoops() throws OperationException, IOException {
//        // Given
//        final Graph graph = getGraph("shouldGetCorrectElementsInGraphFrameWithLoops",
//                "/schema-GraphFrame/elementsComplex.json", getElements());
//        final Edge edge1 = new Edge.Builder().group(TestGroups.EDGE)
//                .source("B")
//                .dest("1")
//                .directed(true)
//                .property("columnQualifier", 1)
//                .property("property1", 2)
//                .property("property2", 3.0F)
//                .property("property3", 4.0D)
//                .property("property4", 5L)
//                .property("count", 100L)
//                .build();
//
//        final Edge edge2 = new Edge.Builder().group(TestGroups.EDGE)
//                .source("C")
//                .dest("1")
//                .directed(true)
//                .property("columnQualifier", 6)
//                .property("property1", 7)
//                .property("property2", 8.0F)
//                .property("property3", 9.0D)
//                .property("property4", 10L)
//                .property("count", 200L)
//                .build();
//
//        graph.execute(new AddElements.Builder().input(edge1, edge2).build(), new User());
//
//        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
//                .first(new GetGraphFrameOfElements())
//                .then(new Map.Builder<>()
//                        .first(new GraphFrameToIterableRow())
//                        .then(new RowToElementGenerator())
//                        .build())
//                .build();
//
//        // When
//        final Iterable<? extends Element> results = graph.execute(opChain, new User());
//
//        // Then
//        final List<Element> expected = getElements();
//        expected.add(edge1);
//        expected.add(edge2);
//        assertElementEquals(expected, results);
//    }
//
//    @Test
//    public void shouldGetCorrectElementsInGraphFrameWithRepeatedElements() throws OperationException, IOException {
//        // Given
//        final Graph graph = getGraph("shouldGetCorrectElementsInGraphFrameWithRepeatedElements",
//                "/schema-GraphFrame/elements.json",
//                getSimpleElements());
//        graph.execute(new AddElements.Builder().input(getSimpleElements()).build(), new User());
//
//        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
//                .first(new GetGraphFrameOfElements())
//                .then(new Map.Builder<>()
//                        .first(new GraphFrameToIterableRow())
//                        .then(new RowToElementGenerator())
//                        .build())
//                .build();
//
//        // When
//        final Iterable<? extends Element> results = graph.execute(opChain, new User());
//
//        // Then
//        assertElementEquals(getSimpleElements(), results);
//    }
//
//    private Graph getGraph(final String graphId,
//                           final String elementsSchema,
//                           final List<Element> elements) throws OperationException, IOException {
//        final String folder = testFolder.newFolder().getAbsolutePath();
//        final ParquetStoreProperties properties = TestUtils.getParquetStoreProperties(folder);
//        final Graph graph = new Graph.Builder()
//                .config(new GraphConfig.Builder()
//                        .graphId(graphId)
//                        .build())
//                .addSchema(getClass().getResourceAsStream(elementsSchema))
//                .addSchema(getClass().getResourceAsStream("/schema-GraphFrame/types.json"))
//                .storeProperties(properties)
//                .build();
//        graph.execute(new AddElements.Builder().input(elements).build(), new User());
//        return graph;
//    }
//
//    private List<Element> getElements() {
//        final List<Element> elements = new ArrayList<>();
//
//        final Entity entityB = new Entity.Builder().group(TestGroups.ENTITY)
//                .vertex("B")
//                .property("columnQualifier", 1)
//                .property("count", 1L)
//                .build();
//
//        final Entity entityC = new Entity.Builder().group(TestGroups.ENTITY)
//                .vertex("C")
//                .property("columnQualifier", 1)
//                .property("count", 1L)
//                .build();
//
//        elements.add(entityB);
//        elements.add(entityC);
//
//        for (int i = 0; i < NUM_ELEMENTS; i++) {
//            final Entity entity1 = new Entity.Builder().group(TestGroups.ENTITY)
//                    .vertex("" + i)
//                    .property("columnQualifier", 1)
//                    .property("property1", i)
//                    .property("property2", 3.0F)
//                    .property("property3", 4.0D)
//                    .property("property4", 5L)
//                    .property("count", 6L)
//                    .build();
//
//            final Entity entity2 = new Entity.Builder().group(TestGroups.ENTITY_2)
//                    .vertex("" + i)
//                    .property("columnQualifier", 1)
//                    .property("property1", i)
//                    .property("property2", 3.0F)
//                    .property("property3", 4.0D)
//                    .property("property4", 5L)
//                    .property("count", 6L)
//                    .build();
//
//
//            final Edge edge1 = new Edge.Builder().group(TestGroups.EDGE)
//                    .source("" + i)
//                    .dest("B")
//                    .directed(true)
//                    .property("columnQualifier", 1)
//                    .property("property1", 2)
//                    .property("property2", 3.0F)
//                    .property("property3", 4.0D)
//                    .property("property4", 5L)
//                    .property("count", 100L)
//                    .build();
//
//            final Edge edge2 = new Edge.Builder().group(TestGroups.EDGE)
//                    .source("" + i)
//                    .dest("C")
//                    .directed(true)
//                    .property("columnQualifier", 6)
//                    .property("property1", 7)
//                    .property("property2", 8.0F)
//                    .property("property3", 9.0D)
//                    .property("property4", 10L)
//                    .property("count", i * 200L)
//                    .build();
//
//            elements.add(edge1);
//            elements.add(edge2);
//            elements.add(entity1);
//            elements.add(entity2);
//        }
//        return elements;
//    }
//
//    private List<Element> getSimpleElements() {
//        final List<Element> elements = new ArrayList<>();
//
//        elements.add(new Entity.Builder().group(TestGroups.ENTITY)
//                .vertex("B")
//                .property("fullname", "")
//                .build());
//
//        elements.add(new Entity.Builder().group(TestGroups.ENTITY_2)
//                .vertex("C")
//                .build());
//
//        for (int i = 0; i < NUM_ELEMENTS; i++) {
//            elements.add(new Entity.Builder().group(TestGroups.ENTITY)
//                    .vertex("" + i)
//                    .property("fullname", "name_" + i)
//                    .build());
//
//            elements.add(new Entity.Builder().group(TestGroups.ENTITY_2)
//                    .vertex("" + i)
//                    .build());
//
//            elements.add(new Edge.Builder().group(TestGroups.EDGE)
//                    .source("" + i)
//                    .dest("B")
//                    .directed(true)
//                    .property("type", "type_" + i)
//                    .build());
//
//            elements.add(new Edge.Builder().group(TestGroups.EDGE)
//                    .source("" + i)
//                    .dest("C")
//                    .directed(true)
//                    .property("type", "type_" + i)
//                    .build());
//        }
//        return elements;
//    }
//}
