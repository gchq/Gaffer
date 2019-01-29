/*
 * Copyright 2018-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.integration;

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

public abstract class StandaloneIT {

    protected Graph createGraph() {
        return new Graph.Builder()
                .config(createGraphConfig())
                .storeProperties(createStoreProperties())
                .addSchema(createSchema())
                .build();
    }

    protected Graph createGraph(final Schema schema) {
        return new Graph.Builder()
                .config(createGraphConfig())
                .storeProperties(createStoreProperties())
                .addSchema(schema)
                .build();
    }

    protected Graph createGraph(final StoreProperties storeProperties) {
        return new Graph.Builder()
                .config(createGraphConfig())
                .storeProperties(storeProperties)
                .addSchema(createSchema())
                .build();
    }

    protected GraphConfig createGraphConfig() {
        return new GraphConfig.Builder()
                .graphId("graphId")
                .build();
    }

    protected abstract Schema createSchema();

    protected User getUser() {
        return new User.Builder()
                .userId("user01")
                .dataAuth("public")
                .build();
    }

    public StoreProperties createStoreProperties() {
        return StoreProperties.loadStoreProperties("store.properties");
    }
}
