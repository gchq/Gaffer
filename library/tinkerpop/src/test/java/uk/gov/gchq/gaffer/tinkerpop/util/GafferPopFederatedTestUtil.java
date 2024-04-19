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

package uk.gov.gchq.gaffer.tinkerpop.util;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

public final class GafferPopFederatedTestUtil {
        public static final String SOFTWARE_GROUP = "software";
        public static final String PERSON_GROUP = "person";
        public static final String CREATED_EDGE_GROUP = "created";
        public static final String KNOWS_EDGE_GROUP = "knows";
        public static final String NAME_PROPERTY = "name";
        public static final String WEIGHT_PROPERTY = "weight";
        private static final User USER = new User("user01");

        private static final FederatedStoreProperties FEDERATED_STORE_PROPERTIES = FederatedStoreProperties
                        .loadStoreProperties("/federatedStore/fed-store.properties");
        private static final MapStoreProperties MAP_STORE_PROPERTIES = MapStoreProperties
                        .loadStoreProperties("/tinkerpop/map-store.properties");

        private GafferPopFederatedTestUtil() {
        }

        // Creates a basic federated graph with two sub-graphs within it
        public static Graph setUpFederatedGraph(Class<?> clazz) throws Exception {
                final Graph federatedGraph = new Graph.Builder()
                                .config(new GraphConfig.Builder()
                                                .graphId("federatedGraph")
                                                .build())
                                .addStoreProperties(FEDERATED_STORE_PROPERTIES)
                                .build();

                federatedGraph.execute(new AddGraph.Builder()
                                .graphId("graphA")
                                .storeProperties(MAP_STORE_PROPERTIES)
                                .schema(Schema.fromJson(StreamUtil.openStreams(clazz, "/gaffer/schema")))
                                .build(), USER);

                federatedGraph.execute(new AddGraph.Builder()
                                .graphId("graphB")
                                .storeProperties(MAP_STORE_PROPERTIES)
                                .schema(Schema.fromJson(StreamUtil.openStreams(clazz, "/gaffer/schema")))
                                .build(), USER);

                addElements(federatedGraph);

                return federatedGraph;
        }

        // Pre-adds elements to each graph
        public static void addElements(final Graph federatedGraph) throws OperationException {
                federatedGraph.execute(new FederatedOperation.Builder()
                                .op(new AddElements.Builder()
                                                .input(
                                                                new Entity.Builder()
                                                                                .group(PERSON_GROUP)
                                                                                .vertex("p1")
                                                                                .property("name", "person1Name")
                                                                                .build(),
                                                                new Entity.Builder()
                                                                                .group(SOFTWARE_GROUP)
                                                                                .vertex("s1")
                                                                                .property("name", "software1Name")
                                                                                .build(),
                                                                new Entity.Builder()
                                                                                .group(PERSON_GROUP)
                                                                                .vertex("p2")
                                                                                .property("name", "person2Name")
                                                                                .build(),
                                                                new Entity.Builder()
                                                                                .group(PERSON_GROUP)
                                                                                .vertex("p3")
                                                                                .property("name", "person3Name")
                                                                                .build(),
                                                                new Edge.Builder()
                                                                                .group(CREATED_EDGE_GROUP)
                                                                                .source("p1")
                                                                                .dest("s1")
                                                                                .property("weight", 0.4)
                                                                                .build(),
                                                                new Edge.Builder()
                                                                                .group(CREATED_EDGE_GROUP)
                                                                                .source("p3")
                                                                                .dest("s1")
                                                                                .property("weight", 0.2)
                                                                                .build(),
                                                                new Edge.Builder()
                                                                                .group(KNOWS_EDGE_GROUP)
                                                                                .source("p1")
                                                                                .dest("p2")
                                                                                .property("weight", 1.0)
                                                                                .build())
                                                .build())
                                .graphIdsCSV("graphA")
                                .build(), USER);

                federatedGraph.execute(new FederatedOperation.Builder()
                                .op(new AddElements.Builder()
                                                .input(
                                                                new Entity.Builder()
                                                                                .group(PERSON_GROUP)
                                                                                .vertex("p4")
                                                                                .property("name", "person4Name")
                                                                                .build(),
                                                                new Entity.Builder()
                                                                                .group(SOFTWARE_GROUP)
                                                                                .vertex("s2")
                                                                                .property("name", "software2Name")
                                                                                .build(),
                                                                new Edge.Builder()
                                                                                .group(CREATED_EDGE_GROUP)
                                                                                .source("p4")
                                                                                .dest("s2")
                                                                                .property("weight", 0.8)
                                                                                .build())
                                                .build())
                                .graphIdsCSV("graphB")
                                .build(), USER);
        }

}
