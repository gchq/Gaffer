/*
 * Copyright 2020 Crown Copyright
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

import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.util.AggregatorUtil;
import uk.gov.gchq.gaffer.user.User;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;

public final class VisibilityTest {

    private static final Schema SCHEMA = Schema.fromJson(StreamUtil.openStreams(VisibilityTest.class, "schema-with-visibilities"));

    private static final String PUBLIC = "public";
    private static final String PRIVATE = "private";
    private static final String BASIC_EDGE = "BasicEdge";
    private static final String BASIC_ENTITY = "BasicEntity";
    private static final String PROPERTY_1 = "property1";
    private static final String PROPERTY_3 = "property3";
    private static final String VISIBILITY = "visibility";
    private static final String COUNT = "count";
    static final String VERTEX_1 = "vertex1";
    static final String VERTEX_2 = "vertex2";

    private static final List<String[]> DATA_AUTH_COMBINATIONS = asList(
            new String[]{},
            new String[]{PRIVATE},
            new String[]{PUBLIC},
            new String[]{PRIVATE, PUBLIC}
    );

    private VisibilityTest() {
    }

    private static Iterable<Element> createElements() {
        return asList(
                /* Aggregatable Entities */
                new Entity.Builder()
                        .group(BASIC_ENTITY)
                        .vertex(VERTEX_1)
                        .property(PROPERTY_1, "p1_publicEntity")
                        .property(COUNT, 1)
                        .property(VISIBILITY, PUBLIC)
                        .build(),
                new Entity.Builder()
                        .group(BASIC_ENTITY)
                        .vertex(VERTEX_1)
                        .property(PROPERTY_1, "p1_publicEntity")
                        .property(COUNT, 1)
                        .property(VISIBILITY, PUBLIC)
                        .build(),
                new Entity.Builder()
                        .group(BASIC_ENTITY)
                        .vertex(VERTEX_2)
                        .property(PROPERTY_1, "p1_publicEntity")
                        .property(COUNT, 1)
                        .property(VISIBILITY, PRIVATE)
                        .build(),
                new Entity.Builder()
                        .group(BASIC_ENTITY)
                        .vertex(VERTEX_2)
                        .property(PROPERTY_1, "p1_publicEntity")
                        .property(COUNT, 1)
                        .property(VISIBILITY, PRIVATE)
                        .build(),

                /* Non-aggregatable Entities */
                new Entity.Builder()
                        .group(BASIC_ENTITY)
                        .vertex(VERTEX_1)
                        .property(PROPERTY_3, "p3_publicEntity")
                        .property(COUNT, 1)
                        .property(VISIBILITY, PUBLIC)
                        .build(),
                new Entity.Builder()
                        .group(BASIC_ENTITY)
                        .vertex(VERTEX_1)
                        .property(PROPERTY_3, "p3_publicEntity")
                        .property(COUNT, 1)
                        .property(VISIBILITY, PRIVATE)
                        .build(),

                /* Aggregatable Edges */
                new Edge.Builder()
                        .group(BASIC_EDGE)
                        .directed(true)
                        .source(VERTEX_1)
                        .dest(VERTEX_2)
                        .property(PROPERTY_1, "p1_publicEdge")
                        .property(COUNT, 1)
                        .property(VISIBILITY, PUBLIC)
                        .build(),
                new Edge.Builder()
                        .group(BASIC_EDGE)
                        .directed(true)
                        .source(VERTEX_1)
                        .dest(VERTEX_2)
                        .property(PROPERTY_1, "p1_publicEdge")
                        .property(COUNT, 1)
                        .property(VISIBILITY, PUBLIC)
                        .build(),
                new Edge.Builder()
                        .group(BASIC_EDGE)
                        .directed(true)
                        .source(VERTEX_2)
                        .dest(VERTEX_1)
                        .property(PROPERTY_1, "p1_privateEdge")
                        .property(COUNT, 1)
                        .property(VISIBILITY, PRIVATE)
                        .build(),
                new Edge.Builder()
                        .group(BASIC_EDGE)
                        .directed(true)
                        .source(VERTEX_2)
                        .dest(VERTEX_1)
                        .property(PROPERTY_1, "p1_privateEdge")
                        .property(COUNT, 1)
                        .property(VISIBILITY, PRIVATE)
                        .build(),

                /* Non-aggregatable Edges */
                new Edge.Builder()
                        .group(BASIC_EDGE)
                        .directed(true)
                        .source(VERTEX_1)
                        .dest(VERTEX_2)
                        .property(PROPERTY_3, "p3_publicEdge")
                        .property(COUNT, 1)
                        .property(VISIBILITY, PUBLIC)
                        .build(),
                new Edge.Builder()
                        .group(BASIC_EDGE)
                        .directed(true)
                        .source(VERTEX_1)
                        .dest(VERTEX_2)
                        .property(PROPERTY_3, "p3_privateEdge")
                        .property(COUNT, 1)
                        .property(VISIBILITY, PRIVATE)
                        .build()
        );
    }

    private static User getUserWithDataAuths(final String... dataAuths) {
        return new User.Builder()
                .dataAuths(dataAuths)
                .build();
    }

    private static List<Element> getExpectedElementsFor(final Schema schema, final String... dataAuths) {
        final ElementVisibilityPredicate elementVisibilityPredicate = new ElementVisibilityPredicate(dataAuths);
        final CloseableIterable<Element> expectedIterable = AggregatorUtil.ingestAggregate(createElements(), schema);
        final CloseableIterator<Element> expectedIterator = expectedIterable.iterator();
        while (expectedIterator.hasNext()) {
            final Element element = expectedIterator.next();
            if (!elementVisibilityPredicate.test(element)) {
                expectedIterator.remove();
            }
        }
        return Lists.newArrayList(expectedIterable);
    }

    public static <OUTPUT, OPERATION extends Operation & Output<OUTPUT>> void executeOperation(
            final OPERATION operation,
            final BiConsumer<OUTPUT, String[]> resultConsumer) throws OperationException {

        final MapStoreProperties storeProperties = new MapStoreProperties();
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graph1")
                        .build())
                .addSchema(SCHEMA)
                .storeProperties(storeProperties)
                .build();

        final AddElements addElements = new AddElements.Builder().input(createElements()).build();
        graph.execute(addElements, getUserWithDataAuths(PUBLIC, PRIVATE));

        for (final String[] dataAuths : DATA_AUTH_COMBINATIONS) {
            resultConsumer.accept(graph.execute(operation, getUserWithDataAuths(dataAuths)), dataAuths);
        }
    }

    public static <OUTPUT> void elementIterableResultSizeConsumer(final OUTPUT output, final String... dataAuths) {
        assertEquals((long) getExpectedElementsFor(SCHEMA, dataAuths).size(), output);
    }

    public static <OUTPUT> void elementIterableResultConsumer(final OUTPUT output, final String... dataAuths) {
        ElementUtil.assertElementEquals(getExpectedElementsFor(SCHEMA, dataAuths), (Iterable<Element>) output);
    }

    public static <OUTPUT> void vertex1AdjacentIdsResultConsumer(final OUTPUT output, final String... dataAuths) {
        ElementUtil.assertElementEquals(getExpectedAdjacentIdsFor(SCHEMA, asList(VERTEX_1), dataAuths), (Iterable<Element>) output);
    }

    private static List<EntityId> getExpectedAdjacentIdsFor(final Schema schema, final List<String> vertexSeeds, final String... dataAuths) {
        final Set<EntitySeed> seedsToRemove = vertexSeeds.stream().map(EntitySeed::new).collect(toSet());
        final ElementVisibilityPredicate elementVisibilityPredicate = new ElementVisibilityPredicate(dataAuths);
        final EdgeVertexPredicate edgeVertexPredicate = new EdgeVertexPredicate(vertexSeeds);
        return Streams.toStream(AggregatorUtil.ingestAggregate(createElements(), schema))
                .filter(Edge.class::isInstance)
                .map(Edge.class::cast)
                .filter(elementVisibilityPredicate)
                .filter(edgeVertexPredicate)
                .map(VisibilityTest::sourceAndDestinationFrom)
                .flatMap(Set::stream)
                .filter(((Predicate<EntityId>) seedsToRemove::contains).negate())
                .collect(toList());
    }

    private static class ElementVisibilityPredicate implements Predicate<Element> {
        private final String[] dataAuths;

        ElementVisibilityPredicate(final String... dataAuths) {
            this.dataAuths = dataAuths;
        }

        @Override
        public boolean test(final Element element) {
            for (String dataAuth : dataAuths) {
                if (element.getProperty(VISIBILITY).toString().contains(dataAuth)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static class EdgeVertexPredicate implements Predicate<Edge> {
        private final List<String> vertexes;

        EdgeVertexPredicate(final List<String> vertexes) {
            this.vertexes = vertexes;
        }

        @Override
        public boolean test(final Edge edge) {
            for (final String vertex : vertexes) {
                if (edge.getSource().equals(vertex) || edge.getDestination().equals(vertex)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static Set<EntityId> sourceAndDestinationFrom(final Edge edge) {
        final Set<EntityId> nodes = new HashSet<>();
        nodes.add(new EntitySeed(edge.getSource()));
        nodes.add(new EntitySeed(edge.getDestination()));
        return nodes;
    }
}
