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

package uk.gov.gchq.gaffer.federatedstore.integration.factory;

import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.store.schema.Schema;

public class FederatedStoreGraphFactory implements GraphFactory {

    private Graph graph;

    @Override
    public Graph.Builder createGraphBuilder() {
        return new Graph.Builder()
            .storeProperties(new FederatedStoreProperties())
            .addSchema(new Schema())
            .config(new GraphConfig("store"));
    }

    @Override
    public Graph getGraph() {
        if (graph == null) {
            graph = createGraph();
        }
        return graph;
    }
}
