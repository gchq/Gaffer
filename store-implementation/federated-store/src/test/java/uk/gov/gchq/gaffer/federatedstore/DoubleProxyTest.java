/*
 * Copyright 2021-2022 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.proxystore.ProxyProperties;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * The DoubleProxyTest Test works as follows:
 *                                      --------------------
 * FederatedStore                      |   GAFFER REST API |
 *      -> Proxy Store 1 ------------> |                   |
 *                                     |                   |
 *      -> Proxy Store 2 ------------> |      MapStore     |
 *                                      --------------------
 */
public class DoubleProxyTest {

    private Graph federatedStoreGraph;


    @BeforeEach
    public void setUpStores() throws OperationException {
       FederatedStoreTestUtil.resetForFederatedTests();

        ProxyProperties proxyProperties = new ProxyProperties();
        proxyProperties.setStoreClass(SingleUseProxyMapStore.class);


        //this direct Proxy to the RestMapStore can be ignored, unless adding elements is desired.
        Graph ignore = new Graph.Builder()
                .storeProperties(proxyProperties)
                .config(new GraphConfig("RestApiGraph"))
                .addSchema(Schema.fromJson(getClass().getResourceAsStream("/schema/basicEntitySchema.json")))
                .build();

        federatedStoreGraph = new Graph.Builder()
                .config(new GraphConfig("federatedStoreGraph"))
                .storeProperties(new FederatedStoreProperties())
                .build();

        connectGraphs("RestProxy1");
        connectGraphs("RestProxy2");
    }

    private void connectGraphs(final String graphId) throws OperationException {
        federatedStoreGraph.execute(new AddGraph.Builder()
                .storeProperties(new ProxyProperties())
                .graphId(graphId)
                .schema(new Schema())
                .build(), new User());
    }

    @AfterAll
    public static void afterClass() {
        SingleUseProxyMapStore.cleanUp();
    }

    @Test
    public void shouldNotErrorDueToRestProxy1FlagsPersistingIntoRestProxy2() throws Exception {
        assertThatNoException().isThrownBy(() -> federatedStoreGraph.execute(new GetAllElements(), new Context()));
    }
}
