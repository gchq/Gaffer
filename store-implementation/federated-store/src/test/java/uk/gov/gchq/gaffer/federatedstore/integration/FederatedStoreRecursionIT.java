/*
 * Copyright 2020-2022 Crown Copyright
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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.SingleUseFederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.proxystore.ProxyProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.predicate.Exists;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedOperation;

public class FederatedStoreRecursionIT {
    public static final String INNER_FEDERATED_GRAPH = "innerFederatedGraph";
    public static final String INNER_PROXY = "innerProxy";
    public static final String ENTITY_GRAPH = "entityGraph";
    public static final String PROXY_TO_REST_SERVICE_FEDERATED_GRAPH = "proxyToRestServiceFederatedGraph";
    public static final String ENT_GROUP = "ent1";
    public static final String PROPERTY_NAME = "count";
    private Graph proxyToRestServiceFederatedGraph;
    private User user = new User();

    @AfterAll
    public static void afterClass() {
        SingleUseFederatedStore.cleanUp();
    }

    protected void addEntity() throws OperationException {
        proxyToRestServiceFederatedGraph.execute(
                new AddElements.Builder()
                        .input(new Entity.Builder()
                                .group(ENT_GROUP)
                                .vertex("e1")
                                .property(PROPERTY_NAME, 1)
                                .build())
                        .build(),
                user);
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
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
        CacheServiceLoader.shutdown();

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
                .build(), user);
    }

    protected void testGetAllElements(final int expected) throws OperationException {
        ArrayList<Element> elements = Lists.newArrayList(proxyToRestServiceFederatedGraph.execute(
                new GetAllElements.Builder()
                        .build(),
                user));
        assertThat(elements).as(elements.toString()).hasSize(1);
        assertThat(elements.get(0).getProperties()).containsEntry(PROPERTY_NAME, expected);
    }

    protected void testInnerGetGraphIds(final String... ids) throws OperationException {
        ArrayList<? extends String> list = (ArrayList<? extends String>) proxyToRestServiceFederatedGraph.execute(
                getFederatedOperation(
                        OperationChain.wrap(
                                new GetAllGraphIds()
                        )),
                user);
        assertThat(list).hasSameSizeAs(ids);
        for (String id : ids) {
            Assertions.<String>assertThat(list).as(list.toString()).contains(id);
        }
    }

    protected void createInnerProxyToOuterFederatedStore() throws OperationException {
        ProxyProperties storeProperties = new ProxyProperties();
        storeProperties.setReadTimeout(120000);
        storeProperties.setConnectTimeout(120000);
        proxyToRestServiceFederatedGraph.execute(getFederatedOperation(
                OperationChain.wrap(new AddGraph.Builder()
                        .graphId(INNER_PROXY)
                        .schema(new Schema())
                        .storeProperties(storeProperties)
                        .build())), user);
    }

    protected void createTheInnerFederatedStore() throws
            OperationException {
        proxyToRestServiceFederatedGraph.execute(new AddGraph.Builder()
                .graphId(INNER_FEDERATED_GRAPH)
                .schema(new Schema())
                .storeProperties(new FederatedStoreProperties())
                .build(), user);
    }

    protected void createProxyToRestServiceFederatedGraph() {
        final Graph proxyToRestServiceFederatedGraph;
        ProxyProperties singleUseFedProperties = new ProxyProperties();
        singleUseFedProperties.setStoreClass(SingleUseFederatedStore.class);
        singleUseFedProperties.setReadTimeout(120000);
        singleUseFedProperties.setConnectTimeout(120000);

        proxyToRestServiceFederatedGraph = new Graph.Builder()
                .storeProperties(singleUseFedProperties)
                .addSchema(new Schema())
                .config(new GraphConfig(PROXY_TO_REST_SERVICE_FEDERATED_GRAPH))
                .build();
        this.proxyToRestServiceFederatedGraph = proxyToRestServiceFederatedGraph;
    }

    protected void testOuterGetGraphIds(final String... ids) throws OperationException {
        ArrayList<? extends String> list = Lists.newArrayList(proxyToRestServiceFederatedGraph.execute(new GetAllGraphIds(), user));
        assertThat(list).hasSameSizeAs(ids);
        for (String id : ids) {
            Assertions.<String>assertThat(list).as(list.toString()).contains(id);
        }
    }
}
