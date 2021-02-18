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
package uk.gov.gchq.gaffer.federatedstore.integration;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.integration.factory.FederatedStoreGraphFactory;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperationChain;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.proxystore.ProxyProperties;
import uk.gov.gchq.gaffer.rest.GafferWebApplication;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.predicate.Exists;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = GafferWebApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("recursion")
public class FederatedStoreRecursionIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederatedStore.class);
    public static final String INNER_FEDERATED_GRAPH = "innerFederatedGraph";
    public static final String INNER_PROXY = "innerProxy";
    public static final String ENTITY_GRAPH = "entityGraph";
    public static final String PROXY_TO_REST_SERVICE_FEDERATED_GRAPH = "proxyToRestServiceFederatedGraph";
    public static final String ENT_GROUP = "ent1";
    public static final String PROPERTY_NAME = "count";
    private Graph proxyToRestServiceFederatedGraph;

    @Autowired
    private GraphFactory graphFactory;

    @LocalServerPort
    private int port;

    @BeforeEach
    @AfterEach
    public void clearCache() {
        CacheServiceLoader.shutdown();
    }

    @BeforeEach
    public void restGraphFactory() {
        if (graphFactory instanceof FederatedStoreGraphFactory) {
            ((FederatedStoreGraphFactory) graphFactory).reset();
        } else {
            throw new RuntimeException("Expected the FederatedStoreGraphFactory Factory to be injected");
        }
    }

    @Test
    @Timeout(60)
    public void shouldNotInfinityLoopWhenAddingElements() throws Exception {
        /*
         * Structure:
         *
         * proxyToRestServiceFederatedGraph (in scope) --> restServiceFederatedGraph <--------------
         *                                                  |                   |                   ^
         *                                                  v                   v                   |
         *                                                  outerEntityGraph    innerFederatedStore |
         *                                                                      |                   |
         *                                                                      v                   |
         *                                                                      innerProxy -------->
         */
        createProxyToRestServiceFederatedGraph();
        createTheInnerFederatedStore();
        createInnerProxyToOuterFederatedStore();
        testOuterGetGraphIds(INNER_FEDERATED_GRAPH);
        testInnerGetGraphIds(INNER_PROXY);
        createEntityGraph();
        testOuterGetGraphIds(INNER_FEDERATED_GRAPH, ENTITY_GRAPH);
        addEntity();
        testGetAllElements(1);
        addEntity();
        testGetAllElements(2);
    }


    protected void addEntity() throws OperationException {
        LOGGER.debug("addEntity");
        proxyToRestServiceFederatedGraph.execute(
                new AddElements.Builder()
                        .input(new Entity.Builder()
                                .group(ENT_GROUP)
                                .vertex("e1")
                                .property(PROPERTY_NAME, 1)
                                .build())
                        .build(),
                new User());
    }

    protected void testGetAllElements(final int expected) throws OperationException {
        LOGGER.debug("testGetAllElements");
        ArrayList<Element> elements = Lists.newArrayList(proxyToRestServiceFederatedGraph.execute(
                new GetAllElements.Builder()
                        .build(),
                new User()));
        assertEquals(1, elements.size(), elements.toString());
        assertEquals(expected, elements.get(0).getProperties().get(PROPERTY_NAME));
    }

    protected void createEntityGraph() throws OperationException {
        proxyToRestServiceFederatedGraph.execute(new AddGraph.Builder()
                .graphId(ENTITY_GRAPH)
                .storeProperties(new MapStoreProperties())
                .schema(new Schema.Builder()
                        .entity(ENT_GROUP,
                                new SchemaEntityDefinition.Builder()
                                        .vertex("string")
                                        .property(PROPERTY_NAME, "count")
                                        .build())
                        .type("string", String.class)
                        .type("count", new TypeDefinition.Builder()
                                .clazz(Integer.class)
                                .aggregateFunction(new Sum())
                                .validateFunctions(new Exists(), new IsEqual(1))
                                .build())
                        .build())
                .build(), new User());
    }

    protected void testInnerGetGraphIds(final String... ids) throws OperationException {
        ArrayList<? extends String> list = Lists.newArrayList(proxyToRestServiceFederatedGraph.execute(
                new FederatedOperationChain.Builder<Object, String>()
                        .operationChain(OperationChain.wrap(
                                new GetAllGraphIds()
                        )).build(),
                new User()));
        assertEquals(ids.length, list.size());
        for (String id : ids) {
            assertTrue(list.contains(id), list.toString());
        }
    }

    protected void testOuterGetGraphIds(final String... ids) throws OperationException {
        ArrayList<? extends String> list = Lists.newArrayList(proxyToRestServiceFederatedGraph.execute(new GetAllGraphIds(), new User()));
        assertEquals(ids.length, list.size());
        for (String id : ids) {
            assertTrue(list.contains(id), list.toString());
        }
    }

    protected void createInnerProxyToOuterFederatedStore() throws OperationException {
        ProxyProperties storeProperties = new ProxyProperties();
        storeProperties.setGafferPort(port);
        storeProperties.setGafferContextRoot("rest");
        storeProperties.setReadTimeout(120000);
        storeProperties.setConnectTimeout(120000);
        proxyToRestServiceFederatedGraph.execute(new FederatedOperationChain.Builder<>()
                .operationChain(OperationChain.wrap(new AddGraph.Builder()
                        .graphId(INNER_PROXY)
                        .schema(new Schema())
                        .storeProperties(storeProperties)
                        .build()))
                .build(), new User());
    }

    protected void createTheInnerFederatedStore() throws
            OperationException {
        proxyToRestServiceFederatedGraph.execute(new AddGraph.Builder()
                .graphId(INNER_FEDERATED_GRAPH)
                .schema(new Schema())
                .storeProperties(new FederatedStoreProperties())
                .build(), new User());
    }

    protected void createProxyToRestServiceFederatedGraph() {
        ProxyProperties proxyProps = new ProxyProperties();
        proxyProps.setGafferPort(port);
        proxyProps.setGafferContextRoot("rest");
        proxyProps.setReadTimeout(120000);
        proxyProps.setConnectTimeout(120000);

        this.proxyToRestServiceFederatedGraph = new Graph.Builder()
                .storeProperties(proxyProps)
                .addSchema(new Schema())
                .config(new GraphConfig(PROXY_TO_REST_SERVICE_FEDERATED_GRAPH))
                .build();
    }
}
