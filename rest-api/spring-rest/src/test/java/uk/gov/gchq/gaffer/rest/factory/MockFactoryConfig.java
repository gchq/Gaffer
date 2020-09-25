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

package uk.gov.gchq.gaffer.rest.factory;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.mockito.Mockito.when;

/**
 * Configuration which loads in our Mock backed factories
 */
@Configuration
public class MockFactoryConfig {

    @Bean
    @Primary
    public GraphFactory graphFactory() {
        MockGraphFactory factory = new MockGraphFactory();

        // Initially set just to make the REST service build
        Graph emptyGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("id")
                        .build())
                .storeProperties(new MapStoreProperties())
                .addSchema(new Schema())
                .build();
        when(factory.getGraph()).thenReturn(emptyGraph);

        return factory;
    }
}
