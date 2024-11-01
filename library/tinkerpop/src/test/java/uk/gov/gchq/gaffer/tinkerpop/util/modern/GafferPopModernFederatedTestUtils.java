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

package uk.gov.gchq.gaffer.tinkerpop.util.modern;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.StoreType;
import uk.gov.gchq.gaffer.user.User;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.CREATED;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.JOSH;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.KNOWS;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.LOP;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.MARKO;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.MODERN_CONFIGURATION;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.PETER;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.RIPPLE;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.VADAS;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.WEIGHT;

public final class GafferPopModernFederatedTestUtils {

    public static final String FEDERATED_GRAPH_ID = "federatedGraph";
    public static final String CREATED_GRAPH_ID = "createdGraph";
    public static final String KNOWS_GRAPH_ID = "knowsGraph";
    private static final User USER = new User("user01");

    private GafferPopModernFederatedTestUtils() {
    }

    /*
     * Creates a federated graph of the Tinkerpop Modern dataset containing two subgraphs
     * One graph contains all the 'knows' edges, the other the 'created' edges
     */
    public static GafferPopGraph createModernGraph(Class<?> clazz, StoreType storeType) throws OperationException {
        CacheServiceLoader.shutdown();
        Graph g = setUpFederatedGraph(clazz, GafferPopTestUtil.getStoreProperties(storeType));
        return GafferPopGraph.open(MODERN_CONFIGURATION, g);
    }

    private static Graph setUpFederatedGraph(Class<?> clazz, StoreProperties properties) throws OperationException {
        final Graph federatedGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(FEDERATED_GRAPH_ID)
                        .build())
                .addStoreProperties(GafferPopTestUtil.getStoreProperties(StoreType.FEDERATED))
                .build();

        federatedGraph.execute(new AddGraph.Builder()
                .graphId(KNOWS_GRAPH_ID)
                .storeProperties(properties)
                .schema(Schema.fromJson(StreamUtil.openStreams(clazz, "/gaffer/schema")))
                .build(), USER);

        federatedGraph.execute(new AddGraph.Builder()
                .graphId(CREATED_GRAPH_ID)
                .storeProperties(properties)
                .schema(Schema.fromJson(StreamUtil.openStreams(clazz, "/gaffer/schema")))
                .build(), USER);

        setupKnowsGraph(federatedGraph);
        setupCreatedGraph(federatedGraph);

        return federatedGraph;
    }

    private static void setupKnowsGraph(final Graph federatedGraph) throws OperationException {
        List<Element> knowsGraphElements = Stream.of(MARKO, VADAS, JOSH)
                .map((Person p) -> {
                    Entity personEntity = p.toEntity();
                    List<Element> personElements = p.getKnows()
                            .stream()
                            .map(e -> getEdge(p, e.getFirst(), e.getSecond()))
                            .collect(Collectors.toList());
                    personElements.add(personEntity);
                    return personElements;
                })
                .flatMap(List::stream)
                .collect(Collectors.toList());

        federatedGraph.execute(new FederatedOperation.Builder()
                .op(new AddElements.Builder()
                        .input(knowsGraphElements.toArray(new Element[0]))
                        .build())
                .graphIdsCSV(KNOWS_GRAPH_ID)
                .build(), USER);
    }

    private static void setupCreatedGraph(final Graph federatedGraph) throws OperationException {
        List<Element> createdGraphElements = Stream.of(MARKO, JOSH, PETER)
                .map((Person p) -> {
                    Entity personEntity = p.toEntity();
                    List<Element> personElements = p.getCreated()
                            .stream()
                            .map(e -> getEdge(p, e.getFirst(), e.getSecond()))
                            .collect(Collectors.toList());
                    personElements.add(personEntity);
                    return personElements;
                })
                .flatMap(List::stream)
                .collect(Collectors.toList());

        createdGraphElements.add(LOP.toEntity());
        createdGraphElements.add(RIPPLE.toEntity());

        federatedGraph.execute(new FederatedOperation.Builder()
                .op(new AddElements.Builder()
                        .input(createdGraphElements.toArray(new Element[0]))
                        .build())
                .graphIdsCSV(CREATED_GRAPH_ID)
                .build(), USER);
    }

    private static Edge getEdge(Person from, Person to, double weight) {
        return new Edge.Builder()
                .group(KNOWS)
                .source(from.getId())
                .dest(to.getId())
                .directed(true)
                .property(WEIGHT, weight)
                .build();
    }

    private static Edge getEdge(Person from, Software to, double weight) {
        return new Edge.Builder()
                .group(CREATED)
                .source(from.getId())
                .dest(to.getId())
                .directed(true)
                .property(WEIGHT, weight)
                .build();
    }
}
