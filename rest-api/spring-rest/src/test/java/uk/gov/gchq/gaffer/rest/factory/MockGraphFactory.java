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

package uk.gov.gchq.gaffer.rest.factory;

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A Wrapper for a mock instance of a GraphFactory
 */
public class MockGraphFactory implements GraphFactory {

    private final GraphFactory graphFactory = mock(GraphFactory.class);

    public MockGraphFactory() {
        // By default we set this to get the REST API to build
        Graph emptyGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("id")
                        .build())
                .storeProperties(new MapStoreProperties())
                .addSchema(new Schema())
                .build();

        when(graphFactory.getGraph()).thenReturn(emptyGraph);
    }

    @Override
    public Graph.Builder createGraphBuilder() {
        return graphFactory.createGraphBuilder();
    }

    @Override
    public Graph getGraph() {
        return graphFactory.getGraph();
    }

    public GraphFactory getMock() {
        return graphFactory;
    }
}
