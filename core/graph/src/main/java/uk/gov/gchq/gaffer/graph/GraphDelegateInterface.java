/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.graph;

import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.List;

interface GraphDelegateInterface {

    interface Builder<GRAPH_DELEGATE, BUILDER extends Builder<GRAPH_DELEGATE, ?>> {
        BUILDER _self();

        Graph createGraph();
    }

    abstract class BaseBuilder<GRAPH_DELEGATE extends GraphDelegateInterface, BUILDER extends BaseBuilder<GRAPH_DELEGATE, ?>>
            implements Builder<GRAPH_DELEGATE, BUILDER> {
        protected Store store;
        protected String graphId;
        protected Schema schema;
        protected StoreProperties storeProperties;
        protected List<String> parentSchemaIds;
        protected String parentStorePropertiesId;


        public Graph build() {
            return _self().createGraph();
        }

        @Override
        public BUILDER _self() {
            return (BUILDER) this;
        }

        public BUILDER storeProperties(final StoreProperties storeProperties) {
            this.storeProperties = storeProperties;
            return _self();
        }

        public BUILDER store(final Store store) {
            this.store = store;
            return _self();
        }

        public BUILDER graphId(final String graphId) {
            this.graphId = graphId;
            return _self();
        }

        public BUILDER schema(final Schema schema) {
            this.schema = schema;
            return _self();
        }

        public BUILDER parentSchemaIds(final List<String> parentSchemaIds) {
            this.parentSchemaIds = parentSchemaIds;
            return _self();
        }

        public BUILDER parentStorePropertiesId(final String parentStorePropertiesId) {
            this.parentStorePropertiesId = parentStorePropertiesId;
            return _self();
        }
    }
}
