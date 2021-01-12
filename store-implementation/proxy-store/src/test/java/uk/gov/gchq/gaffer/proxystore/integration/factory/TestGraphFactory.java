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

package uk.gov.gchq.gaffer.proxystore.integration.factory;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

public class TestGraphFactory implements GraphFactory {
    private static final StoreProperties MAP_STORE_PROPERTIES = MapStoreProperties.loadStoreProperties(StreamUtil.openStream(TestGraphFactory.class, "/map-store.properties"));
    private static final Schema SCHEMA = Schema.fromJson(StreamUtil.schema(TestGraphFactory.class));

    private Graph graph;

    @Override
    public Graph.Builder createGraphBuilder() {
        return new Graph.Builder()
                .storeProperties(MAP_STORE_PROPERTIES)
                .addSchema(SCHEMA)
                .config(new GraphConfig("myGraph"));
    }

    @Override
    public Graph getGraph() {
        if (graph == null) {
            graph = createGraphBuilder().build();
        }
        return graph;
    }

    public void reset() {
        graph = createGraphBuilder().build();
    }

}
