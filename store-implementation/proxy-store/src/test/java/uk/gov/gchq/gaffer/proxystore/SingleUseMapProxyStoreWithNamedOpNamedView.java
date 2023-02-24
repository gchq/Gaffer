/*
 * Copyright 2023 Crown Copyright
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

package uk.gov.gchq.gaffer.proxystore;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.DeleteNamedOperation;
import uk.gov.gchq.gaffer.named.operation.GetAllNamedOperations;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.view.AddNamedView;
import uk.gov.gchq.gaffer.named.view.DeleteNamedView;
import uk.gov.gchq.gaffer.named.view.GetAllNamedViews;
import uk.gov.gchq.gaffer.store.operation.handler.named.AddNamedOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.AddNamedViewHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.DeleteNamedOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.DeleteNamedViewHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.GetAllNamedOperationsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.GetAllNamedViewsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.NamedOperationHandler;

import static java.util.Objects.nonNull;

/**
 * An extension of {@link ProxyStore} that starts a REST API backed by a
 * {@link SingleUseMapProxyStoreWithNamedOpNamedView} with the provided schema. This store
 * is useful for testing when there is no actual REST API to connect a ProxyStore to.
 * Each time this store is initialised it will reset the underlying graph, delete
 * any elements that had been added and initialise it with the new schema. The
 * server will not be restarted every time.
 * <p>
 * After using this store you must remember to call
 * SingleUseMapProxyStore.cleanUp to stop the server and delete the temporary folder.
 */
public class SingleUseMapProxyStoreWithNamedOpNamedView extends SingleUseProxyStore {
    @Override
    protected String getPathToDelegateProperties() {
        return "map-store.properties";
    }


    @Override
    protected void addAdditionalOperationHandlers() {
        /*
         * this must be a copy past of addAdditionalOperationHandlers in ProxyStoreWithNamedOpNamedView.java
         */
        super.addAdditionalOperationHandlers();

        if (nonNull(CacheServiceLoader.getService())) {
            //Because of Graph.updateGraphHooks the hook resolvers are forced in, requiring these Handlers

            // Named operation
            addOperationHandler(NamedOperation.class, new NamedOperationHandler());
            addOperationHandler(AddNamedOperation.class, new AddNamedOperationHandler(getProperties().getCacheServiceNameSuffix(getGraphId())));
            addOperationHandler(GetAllNamedOperations.class, new GetAllNamedOperationsHandler(getProperties().getCacheServiceNameSuffix(getGraphId())));
            addOperationHandler(DeleteNamedOperation.class, new DeleteNamedOperationHandler(getProperties().getCacheServiceNameSuffix(getGraphId())));

            // Named view
            addOperationHandler(AddNamedView.class, new AddNamedViewHandler(getProperties().getCacheServiceNameSuffix(getGraphId())));
            addOperationHandler(GetAllNamedViews.class, new GetAllNamedViewsHandler(getProperties().getCacheServiceNameSuffix(getGraphId())));
            addOperationHandler(DeleteNamedView.class, new DeleteNamedViewHandler(getProperties().getCacheServiceNameSuffix(getGraphId())));
        }
    }
}
