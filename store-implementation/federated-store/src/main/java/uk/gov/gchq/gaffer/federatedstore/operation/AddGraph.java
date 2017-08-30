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

import org.apache.commons.lang3.exception.CloneFailedException;
import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.List;

/**
 * An Operation used for adding graphs to a FederatedStore.
 * <p>Requires:
 * <ul>
 * <li>graphId
 * <li>properties
 * <li>schema
 * </ul>
 *
 * @see uk.gov.gchq.gaffer.federatedstore.FederatedStore
 * @see uk.gov.gchq.gaffer.operation.Operation
 * @see uk.gov.gchq.gaffer.store.schema.Schema
 * @see uk.gov.gchq.gaffer.data.element.Properties
 * @see uk.gov.gchq.gaffer.graph.Graph
 */
public class AddGraph implements Operation {

    @Required
    private String graphId;
    private StoreProperties storeProperties;
    private String parentPropertiesId;
    private Schema schema;
    private List<String> parentSchemaIds;

    public String getGraphId() {
        return graphId;
    }

    public void setGraphId(final String graphId) {
        this.graphId = graphId;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(final Schema schema) {
        this.schema = schema;
    }

    @Override
    public AddGraph shallowClone() throws CloneFailedException {
        return new Builder()
                .setGraphId(graphId)
                .setSchema(schema)
                .setProperties(storeProperties)
                .setParentSchemaIds(parentSchemaIds)
                .setParentPropertiesId(parentPropertiesId)
                .build();
    }


    public List<String> getParentSchemaIds() {
        return parentSchemaIds;
    }

    public void setParentSchemaIds(final List<String> parentSchemaIds) {
        this.parentSchemaIds = parentSchemaIds;
    }

    public StoreProperties getProperties() {
        return storeProperties;
    }

    public void setProperties(final StoreProperties properties) {
        this.storeProperties = properties;
    }

    public String getParentPropertiesId() {
        return parentPropertiesId;
    }

    public void setParentPropertiesId(final String parentPropertiesId) {
        this.parentPropertiesId = parentPropertiesId;
    }

    public static class Builder extends BaseBuilder<AddGraph, Builder> {

        public Builder() {
            super(new AddGraph());
        }

        public Builder setGraphId(final String graphId) {
            _getOp().setGraphId(graphId);
            return this;
        }

        public Builder setProperties(final StoreProperties storeProperties) {
            _getOp().setProperties(storeProperties);
            return this;
        }

        public Builder setSchema(final Schema schema) {
            _getOp().setSchema(schema);
            return _self();
        }

        public Builder setParentPropertiesId(final String parentPropertiesId) {
            this._getOp().setParentPropertiesId(parentPropertiesId);
            return _self();
        }

        public Builder setParentSchemaIds(final List<String> parentSchemaIds) {
            _getOp().setParentSchemaIds(parentSchemaIds);
            return _self();
        }
    }
}
