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
package uk.gov.gchq.gaffer.integration.factory;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.integration.util.TestUtil;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

/**
 * The Graph factory used by the remote REST API which backs the ProxyStore. It provides an easy mechanism for resetting
 * the graph which is useful for resetting state between tests without having to restart the whole REST API. As no user
 * operation or data auths can be passed over the proxy-store REST call in these integration tests the backing MapStore
 * is deliberately configured without support for the VISIBILITY StoreTrait.
 */
public class MapStoreGraphFactory implements GraphFactory {
    private static final StoreProperties STORE_PROPERTIES = StoreProperties.loadStoreProperties(StreamUtil.openStream(MapStoreGraphFactory.class, "/stores/mapstore_without_visibility_support.properties"));
    private Graph instance;
    private Schema schema;

    public MapStoreGraphFactory() {
        schema = TestUtil.createDefaultSchema();
        instance = createGraphBuilder().build();
    }


    @Override
    public Graph.Builder createGraphBuilder() {
        return new Graph.Builder()
                .storeProperties(STORE_PROPERTIES)
                .config(new GraphConfig("proxyStoreTest"))
                .addSchema(schema);
    }

    @Override
    public Graph getGraph() {
        return instance;
    }

    public void reset() {
        reset(TestUtil.createDefaultSchema());
    }

    public void reset(final Schema schema) {
        this.schema = schema;
        instance = createGraphBuilder().build();
    }
}
