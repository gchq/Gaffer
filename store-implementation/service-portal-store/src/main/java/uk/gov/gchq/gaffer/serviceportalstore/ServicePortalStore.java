/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.serviceportalstore;

import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.proxystore.ProxyProperties;
import uk.gov.gchq.gaffer.proxystore.ProxyStore;
import uk.gov.gchq.gaffer.serviceportalstore.handler.AddGraphHandler;
import uk.gov.gchq.gaffer.serviceportalstore.handler.ServicePortalOperationChainHandler;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

/**
 * Gaffer {@code ProxyStore} implementation.
 * <p>.
 */
public class ServicePortalStore extends ProxyStore {

    @Override
    protected void addAdditionalOperationHandlers() {
        super.addAdditionalOperationHandlers();
        addOperationHandler(AddGraph.class, new AddGraphHandler());
    }

    @Override
    protected OperationHandler<? extends OperationChain<?>> getOperationChainHandler() {
        return new ServicePortalOperationChainHandler<>(opChainValidator, opChainOptimisers);
    }

    public static class Builder {
        private final ServicePortalStore store;
        private final ProxyProperties properties;
        private String graphId;

        public Builder() {
            this.store = new ServicePortalStore();
            properties = new ProxyProperties();
            properties.setStoreClass(ProxyStore.class);
            properties.setStorePropertiesClass(ProxyProperties.class);
        }

        public Builder host(final String host) {
            properties.setGafferHost(host);
            return this;
        }

        public Builder port(final int port) {
            properties.setGafferPort(port);
            return this;
        }

        public Builder contextRoot(final String contextRoot) {
            properties.setGafferContextRoot(contextRoot);
            return this;
        }

        public Builder connectTimeout(final int timeout) {
            properties.setConnectTimeout(timeout);
            return this;
        }

        public Builder readTimeout(final int timeout) {
            properties.setReadTimeout(timeout);
            return this;
        }

        public Builder jsonSerialiser(final Class<? extends JSONSerialiser> serialiserClass) {
            properties.setJsonSerialiserClass(serialiserClass);
            return this;
        }

        public Builder graphId(final String graphId) {
            this.graphId = graphId;
            return this;
        }

        public ServicePortalStore build() {
            try {
                store.initialise(graphId, new Schema(), properties);
            } catch (final StoreException e) {
                throw new IllegalArgumentException("The store could not be initialised with the provided properties", e);
            }
            return store;
        }
    }
}
