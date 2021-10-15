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
package uk.gov.gchq.gaffer.proxystore.integration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationChainDAO;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.proxystore.ProxyProperties;
import uk.gov.gchq.gaffer.proxystore.ProxyStore;
import uk.gov.gchq.gaffer.proxystore.response.deserialiser.ResponseDeserialiser;
import uk.gov.gchq.gaffer.rest.RestApiTestClient;
import uk.gov.gchq.gaffer.rest.service.v2.RestApiV2TestClient;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProxyStoreResponseDeserialiserIT {

    private static final RestApiTestClient CLIENT = new RestApiV2TestClient();

    @TempDir
    public final File testFolder = CommonTestConstants.TMP_DIRECTORY;

    @BeforeAll
    public static void beforeAll() {
        CLIENT.startServer();
    }

    @AfterAll
    public static void afterAll() {
        CLIENT.stopServer();
    }

    @BeforeEach
    public void before() throws IOException {
        CLIENT.reinitialiseGraph(testFolder, StreamUtil.SCHEMA, "map-store.properties");
    }

    @Test
    public void shouldUseOperationsResponseDeserialiserToDeserialiseOperationsResponse() throws Exception {
        final ResponseDeserialiser<Set<Class<? extends Operation>>> operationResponseDeserialiser = mock(ResponseDeserialiser.class);
        final Set<Class<? extends Operation>> storeOperations = Collections.singleton(AddElements.class);
        when(operationResponseDeserialiser.deserialise(anyString())).thenReturn(storeOperations);

        final TestProxyStore proxyStore = new TestProxyStore.Builder(operationResponseDeserialiser)
                .graphId("graph1")
                .host("localhost")
                .port(8080)
                .contextRoot("rest/v2")
                .build();

        // Create Graph and initialise ProxyStore
        new Graph.Builder()
                .store(proxyStore)
                .build();

        verify(operationResponseDeserialiser).deserialise(anyString());

        final Set<Class<? extends Operation>> actualOperationClasses = proxyStore.getSupportedOperations();
        final Set<Class<? extends Operation>> expectedOperationClasses = new HashSet<>();
        expectedOperationClasses.addAll(storeOperations);
        expectedOperationClasses.add(OperationChain.class);
        expectedOperationClasses.add(OperationChainDAO.class);

        assertEquals(actualOperationClasses.size(), expectedOperationClasses.size());
        assertTrue(actualOperationClasses.containsAll(expectedOperationClasses));
    }


    public static class TestProxyStore extends ProxyStore {

        private final ResponseDeserialiser<Set<Class<? extends Operation>>> operationsResponseDeserialiser;

        TestProxyStore(final ResponseDeserialiser<Set<Class<? extends Operation>>> operationsResponseDeserialiser) {
            this.operationsResponseDeserialiser = operationsResponseDeserialiser;
        }

        @Override
        protected ResponseDeserialiser<Set<Class<? extends Operation>>> getOperationsResponseDeserialiser() {
            return operationsResponseDeserialiser;
        }

        public static final class Builder {
            private final TestProxyStore store;
            private final ProxyProperties properties;
            private String graphId;

            public Builder(final ResponseDeserialiser<Set<Class<? extends Operation>>> operationsResponseDeserialiser) {
                store = new TestProxyStore(operationsResponseDeserialiser);
                properties = new ProxyProperties();
                properties.setStoreClass(ProxyStore.class);
                properties.setStorePropertiesClass(ProxyProperties.class);
            }

            public TestProxyStore.Builder host(final String host) {
                properties.setGafferHost(host);
                return this;
            }

            public TestProxyStore.Builder port(final int port) {
                properties.setGafferPort(port);
                return this;
            }

            public TestProxyStore.Builder contextRoot(final String contextRoot) {
                properties.setGafferContextRoot(contextRoot);
                return this;
            }


            public TestProxyStore.Builder graphId(final String graphId) {
                this.graphId = graphId;
                return this;
            }

            public TestProxyStore build() {
                try {
                    store.initialise(graphId, new Schema(), properties);
                } catch (final StoreException e) {
                    throw new IllegalArgumentException("The store could not be initialised with the provided properties", e);
                }
                return store;
            }
        }
    }
}
