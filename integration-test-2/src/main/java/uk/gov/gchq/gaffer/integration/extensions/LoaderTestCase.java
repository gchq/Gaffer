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
import uk.gov.gchq.gaffer.integration.template.loader.schemas.SchemaSetup;
import uk.gov.gchq.gaffer.store.StoreProperties;

/**
 * A POJO which is injected into Gaffer Loader tests. It provides the test a way to create graphs easily. To create a graph,
 * you only need to provide some store properties.
 */
public class LoaderTestCase {

    private final StoreProperties storeProperties;
    private final SchemaSetup schemaSetup;
    private Graph graph;

    public LoaderTestCase(final StoreProperties storeProperties, final SchemaSetup schemaSetup) {
        this.storeProperties = storeProperties;
        this.schemaSetup = schemaSetup;
    }

    public String getStoreType() {
        return this.storeProperties.getStoreClass();
    }

    public String getSchemaName() {
        return this.schemaSetup.name();
    }

    public SchemaSetup getSchemaSetup() {
        return schemaSetup;
    }

    public StoreProperties getStoreProperties() {
        return storeProperties;
    }

    public Graph getGraph() {
        if (graph == null) {
            graph = new Graph.Builder()
                .config(new GraphConfig("test"))
                .addSchema(this.schemaSetup.getTestSchema().getSchema())
                .storeProperties(this.storeProperties)
                .build();
        }
        return graph;
    }
}
