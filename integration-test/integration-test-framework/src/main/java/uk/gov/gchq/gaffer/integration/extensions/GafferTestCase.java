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

package uk.gov.gchq.gaffer.integration.extensions;

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.integration.util.TestUtil;
import uk.gov.gchq.gaffer.store.StoreProperties;

/**
 * A POJO which is injected into Gaffer tests. It provides the test a way to create graphs easily. To create a graph,
 * you only need to provide some store properties. The graphs will then be created lazily.
 */
public class GafferTestCase extends AbstractTestCase {

    private Graph graph;

    public GafferTestCase(final StoreProperties storeProperties) {
        super(storeProperties);
    }

    @Override
    public Graph getGraph() {
        if (graph == null) {
            graph = createEmptyGraph();
        }

        return graph;
    }

    private Graph createEmptyGraph() {
        return new Graph.Builder()
                .config(new GraphConfig("test"))
                .addSchema(TestUtil.createDefaultSchema())
                .storeProperties(this.getStoreProperties())
                .build();
    }



    public Graph getPopulatedGraph() {
        return TestUtil.addDefaultElements(getGraph());
    }
}
