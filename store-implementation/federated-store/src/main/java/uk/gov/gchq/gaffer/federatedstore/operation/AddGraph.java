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

package uk.gov.gchq.gaffer.federatedstore.operation;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.Properties;

/**
 * An Operation used for adding graphs to a FederatedStore.
 * <p>Requires:
 * <ul>
 * <li>graphId
 * <li>properties
 * <li>schema
 * </ul>
 *
 * @see FederatedStore
 * @see Operation
 * @see Schema
 * @see Properties
 * @see Graph
 */
public class AddGraph implements Operation {

    @Required
    private String graphId;
    @Required
    private Properties properties;
    @Required
    private Schema schema;

    public String getGraphId() {
        return graphId;
    }

    public void setGraphId(final String graphId) {
        this.graphId = graphId;
    }

    public Properties getProperties() {
        return this.properties;
    }

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(final Schema schema) {
        this.schema = schema;
    }

    public static class Builder extends BaseBuilder<AddGraph, Builder> {

        public Builder() {
            super(new AddGraph());
        }

        public Builder setGraphId(final String graphId) {
            _getOp().setGraphId(graphId);
            return this;
        }

        public Builder setStoreProperties(final Properties properties) {
            _getOp().setProperties(properties);
            return this;
        }

        public Builder setStoreProperties(final StoreProperties storeProperties) {
            return setStoreProperties(storeProperties.getProperties());
        }

        public Builder setSchema(final Schema schema) {
            _getOp().setSchema(schema);
            return _self();
        }
    }
}
