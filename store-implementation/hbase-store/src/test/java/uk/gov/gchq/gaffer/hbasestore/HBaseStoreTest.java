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

package uk.gov.gchq.gaffer.hbasestore;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.function.filter.IsMoreThan;
import uk.gov.gchq.gaffer.function.transform.Concat;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.impl.GetElementsIT;
import uk.gov.gchq.gaffer.operation.GetOperation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Ignore
public class HBaseStoreTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseStore.class);

    @Test
    public void shouldGetAllElementsWithFilters() throws StoreException, OperationException {
        LOGGER.info("shouldGetAllElementsWithFilters");

        final Graph graph = createPopulatedGraph();

        final User user = new User.Builder()
                .userId("publicUser")
                .dataAuth("public")
                .build();
        final CloseableIterable<Element> elements = graph.execute(new GetAllElements.Builder<>()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .edge(TestGroups.EDGE_2, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("property1")
                                        .execute(new IsMoreThan(5))
                                        .build())
                                .build())
                        .build())
                .build(), user);
        for (Element element : elements) {
            LOGGER.info("Element: " + element);
        }
    }

    @Test
    public void shouldGetEntity() throws StoreException, OperationException {
        LOGGER.info("shouldGetEntity");

        final Graph graph = createPopulatedGraph();

        final User user = new User.Builder()
                .userId("user")
                .dataAuth("public")
                .build();
        final CloseableIterable<Element> elements = graph.execute(new GetElements.Builder<>()
                .addSeed(new EntitySeed("vertex1"))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                        .transientProperty("transientProp", String.class)
                                        .transformer(new ElementTransformer.Builder()
                                                .select(IdentifierType.VERTEX.name(), "property1")
                                                .execute(new Concat())
                                                .project("transientProp")
                                                .build())
                                        .build()
                        )
                        .build())
                .build(), user);
        for (Element element : elements) {
            LOGGER.info("Element: " + element);
        }
    }

    @Test
    public void shouldGetEdge() throws StoreException, OperationException {
        LOGGER.info("shouldGetEdge");

        final Graph graph = createPopulatedGraph();

        final User user = new User.Builder()
                .userId("user")
                .dataAuth("public")
                .build();
        final CloseableIterable<Element> elements = graph.execute(new GetElements.Builder<>()
                .addSeed(new EntitySeed("vertex2"))
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .build())
                .build(), user);
        for (Element element : elements) {
            LOGGER.info("Element: " + element);
        }
    }

    @Test
    public void shouldGetSelfEdge() throws StoreException, OperationException {
        LOGGER.info("shouldGetSelfEdge");

        final Graph graph = createGraph();
        graph.execute(new AddElements.Builder()
                .elements(new Edge.Builder()
                                .source("vertex1")
                                .dest("vertex1")
                                .group(TestGroups.EDGE)
                                .property("property1", 5)
                                .property("columnQualifier", 10)
                                .property("visibility", "public")
                                .build(),
                        new Edge.Builder()
                                .source("vertex2")
                                .dest("vertex2")
                                .group(TestGroups.EDGE)
                                .property("property1", 5)
                                .property("columnQualifier", 10)
                                .property("visibility", "public")
                                .build())
                .build(), new User());

        final User user = new User.Builder()
                .userId("user")
                .dataAuth("public")
                .build();
        final CloseableIterable<Element> elements = graph.execute(new GetAllElements.Builder<>()
                .build(), user);
        for (Element element : elements) {
            LOGGER.info("Element: " + element);
        }
    }

    @Test
    public void shouldGetElements() throws StoreException, OperationException {
        LOGGER.info("shouldGetElements");

        final Graph graph = createPopulatedGraph();

        final User user = new User.Builder()
                .userId("user")
                .dataAuth("public")
                .build();
        final CloseableIterable<Element> elements = graph.execute(new GetElements.Builder<>()
                .addSeed(new EntitySeed("vertex1"))
                .addSeed(new EntitySeed("vertex2"))
                .build(), user);
        for (Element element : elements) {
            LOGGER.info("Element: " + element);
        }
    }

    @Test
    public void integrationTest() throws OperationException, IOException {
        // Given
        final Graph graph = createIntegrationTestGraph();

        List<ElementSeed> seeds = new ArrayList<>();
        for (Object seed : GetElementsIT.ALL_SEED_VERTICES) {
            seeds.add(new EntitySeed(seed));
        }

        seeds.add(new EdgeSeed(GetElementsIT.SOURCE_DIR_2, GetElementsIT.DEST_DIR_2, true));
        seeds.add(new EdgeSeed(GetElementsIT.SOURCE_DIR_1, GetElementsIT.DEST_DIR_1, true));
        seeds.add(new EdgeSeed(GetElementsIT.SOURCE_DIR_3, GetElementsIT.DEST_DIR_3, true));

        // When
        try (final CloseableIterable<Element> elements = graph.execute(new GetElements.Builder<>()
                .addSeed(new EntitySeed("source2"))
                .includeEntities(true)
                .includeEdges(GetOperation.IncludeEdgeType.DIRECTED)
                .inOutType(GetOperation.IncludeIncomingOutgoingType.BOTH)
                .build(), new User())) {

            for (Element element : elements) {
                LOGGER.info("element: " + element);
            }
        }
    }

    private Graph createPopulatedGraph() throws OperationException {
        final Graph graph = createGraph();
        addElements(graph);
        return graph;
    }

    private Graph createGraph() {
        return new Graph.Builder()
                .addSchemas(StreamUtil.schemas(HBaseStoreTest.class))
                .storeProperties(StreamUtil.storeProps(HBaseStoreTest.class))
                .build();
    }

    private Graph createIntegrationTestGraph() throws OperationException {
        final Graph graph = new Graph.Builder()
                .addSchemas(AbstractStoreIT.createDefaultSchema())
                .storeProperties(StreamUtil.storeProps(HBaseStoreTest.class))
                .build();

        graph.execute(new AddElements.Builder()
                .elements(new ArrayList<>(AbstractStoreIT.createDefaultEdges().values()))
                .build(), new User());
        graph.execute(new AddElements.Builder()
                .elements(new ArrayList<>(AbstractStoreIT.createDefaultEntities().values()))
                .build(), new User());
//
//        final List<Element> edges = new ArrayList<>();
//        for (int i = 0; i <= 10; i++) {
//            final Edge thirdEdge = new Edge(TestGroups.EDGE, AbstractStoreIT.DEST_DIR + i, AbstractStoreIT.SOURCE_DIR + (i + 1), true);
//            thirdEdge.putProperty(TestPropertyNames.INT, 1);
//            thirdEdge.putProperty(TestPropertyNames.COUNT, 1L);
//            edges.add(thirdEdge);
//        }
//
//        graph.execute(new AddElements.Builder()
//                .elements(edges)
//                .build(), new User());

//        final Edge secondEdge = new Edge(TestGroups.EDGE, AbstractStoreIT.SOURCE_DIR_0, AbstractStoreIT.DEST_DIR_0, true);
//        secondEdge.putProperty(TestPropertyNames.INT, 1);
//        secondEdge.putProperty(TestPropertyNames.COUNT, 1L);
//
//        graph.execute(new AddElements.Builder()
//                .elements(secondEdge)
//                .build(), new User());

        return graph;
    }

    private void addElements(final Graph graph) throws OperationException {
        graph.execute(new AddElements.Builder()
                .elements(new Edge.Builder()
                                .source("vertex1")
                                .dest("vertex1b")
                                .group(TestGroups.EDGE)
                                .property("property1", 1)
                                .property("columnQualifier", 10)
                                .property("visibility", "private")
                                .build(),
                        new Edge.Builder()
                                .source("vertex2")
                                .dest("vertex2b")
                                .group(TestGroups.EDGE)
                                .property("property1", 5)
                                .property("columnQualifier", 10)
                                .property("visibility", "public")
                                .build(),
                        new Edge.Builder()
                                .source("vertex3")
                                .dest("vertex3b")
                                .group(TestGroups.EDGE_2)
                                .property("property1", 10)
                                .property("columnQualifier", 10)
                                .property("visibility", "public")
                                .build(),
                        new Entity.Builder()
                                .vertex("vertex1")
                                .group(TestGroups.ENTITY)
                                .property("property1", 1)
                                .property("columnQualifier", 10)
                                .property("visibility", "public")
                                .build(),
                        new Entity.Builder()
                                .vertex("vertex2")
                                .group(TestGroups.ENTITY)
                                .property("property1", 2)
                                .property("columnQualifier", 20)
                                .property("visibility", "public")
                                .build(),
                        new Entity.Builder()
                                .vertex("vertex3")
                                .group(TestGroups.ENTITY)
                                .property("property1", 3)
                                .property("columnQualifier", 30)
                                .property("visibility", "public")
                                .build())
                .build(), new User());
    }
}
