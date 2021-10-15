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

package uk.gov.gchq.gaffer.rest.integration.proxy;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Count;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.proxystore.ProxyProperties;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.integration.controller.AbstractRestApiIT;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

public class ProxyStoreIT extends AbstractRestApiIT {
    @Autowired
    private GraphFactory graphFactory;
    private Graph delegateRestApiGraph;

    @Before
    public void setUpGraph() {
        delegateRestApiGraph = new Graph.Builder()
            .storeProperties(new MapStoreProperties())
            .config(new GraphConfig("myGraph"))
            .addSchema(Schema.fromJson(StreamUtil.openStream(getClass(), "/schema/schema.json")))
            .build();
        when(graphFactory.getGraph()).thenReturn(delegateRestApiGraph);
    }

    @Test
    public void shouldBeAbleToInitialiseProxyStoreBackedBySpringRESTAPI() {
        // Given / When
       Graph proxy = createProxy();

        // Then
        assertNotNull(proxy.getSchema());
    }

    private Graph createProxy() {
        ProxyProperties proxyProperties = new ProxyProperties();
        proxyProperties.setGafferHost("localhost");
        proxyProperties.setGafferPort(getPort());
        proxyProperties.setGafferContextRoot(getContextPath());

        // When
        return new Graph.Builder()
            .config(new GraphConfig("proxy"))
            .addSchema(new Schema())
            .storeProperties(proxyProperties)
            .build();
    }

    @Test
    public void shouldBeAbleToFetchElementsFromProxyStoreBackedBySpringRestAPI() throws OperationException {
        // Given
        delegateRestApiGraph.execute(new AddElements.Builder()
            .input(new Entity.Builder()
                .group("BasicEntity")
                .vertex("vertex1")
                .property("count", 1)
                .build())
            .build(), new User());

        // When
        Graph proxy = createProxy();

        // Then
        ArrayList<Element> elements = Lists.newArrayList(proxy.execute(new GetAllElements(), new User()));
        assertThat(elements).hasSize(1);

        assertEquals("vertex1", elements.get(0).getIdentifier(IdentifierType.VERTEX));
    }

    @Test
    public void shouldBeAbleToHandleNonJsonResponses() throws OperationException {
        // Given
        delegateRestApiGraph.execute(new AddElements.Builder()
            .input(new Entity.Builder()
                .group("BasicEntity")
                .vertex("vertex1")
                .property("count", 1)
                .build())
            .build(), new User());

        // When
        Graph proxy = createProxy();

        // Then
        Long total = proxy.execute(new OperationChain.Builder().first(new GetAllElements()).then(new Count<>()).build(), new User());
        assertEquals(1, total);
    }
}
