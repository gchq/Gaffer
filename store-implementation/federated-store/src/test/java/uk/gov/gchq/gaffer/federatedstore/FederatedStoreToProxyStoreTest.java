/*
 * Copyright 2021 Crown Copyright
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.proxystore.ProxyProperties;
import uk.gov.gchq.gaffer.proxystore.SingleUseMapProxyStore;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.gchq.gaffer.federatedstore.integration.FederatedViewsIT.BASIC_ENTITY;

/**
 * The Test structure:
 *                                           --------------------
 *      FederatedStore                      |   GAFFER REST API |
 *           -> Proxy Store --------------> |                   |
 *                                          |      MapStore     |
 *                                          --------------------
 */
public class FederatedStoreToProxyStoreTest {

    private Graph federatedStoreGraph;
    private Graph restApiMapGraph;

    @BeforeEach
    public void setUpStores() throws OperationException {
        ProxyProperties proxyProperties = new ProxyProperties();
        proxyProperties.setStoreClass(SingleUseMapProxyStore.class);

        restApiMapGraph = new Graph.Builder()
            .storeProperties(proxyProperties)
            .config(new GraphConfig("RestApiGraph"))
            .addSchema(Schema.fromJson(getClass().getResourceAsStream("/schema/basicEntitySchema.json")))
            .build();

        federatedStoreGraph = new Graph.Builder()
            .config(new GraphConfig("federatedStoreGraph"))
            .storeProperties(new FederatedStoreProperties())
            .build();

        connectGraphs();
    }

    private void connectGraphs() throws OperationException {
        federatedStoreGraph.execute(new AddGraph.Builder()
            .storeProperties(new ProxyProperties())
            .graphId("RestProxy")
            .schema(new Schema())
            .build(), new User());
    }

    @Test
    public void shouldGetAllElements() throws OperationException {
        // Given
        Entity entity = new Entity.Builder()
            .group(BASIC_ENTITY)
            .vertex("myVertex")
            .property("property1", 1)
            .build();

        restApiMapGraph.execute(new AddElements.Builder()
            .input(entity)
            .build(), new User());

        // When
        CloseableIterable<? extends Element> results = federatedStoreGraph.execute(new GetAllElements(), new User());


        List<Element> elements = new ArrayList<>();
        results.iterator().forEachRemaining(elements::add);

        assertEquals(1,elements.size());
        assertEquals(entity,elements.get(0));
    }


}
