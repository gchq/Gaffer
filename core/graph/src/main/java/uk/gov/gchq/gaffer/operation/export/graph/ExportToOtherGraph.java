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

package uk.gov.gchq.gaffer.operation.export.graph;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.export.ExportTo;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.Properties;

public class ExportToOtherGraph<T> implements
        Operation,
        ExportTo<T> {
    @Required
    private String graphId;

    private T input;

    private String parentSchemaId;
    private Schema schema;

    private String parentStorePropertiesId;
    private StoreProperties storeProperties;

    @Override
    public String getKey() {
        return null;
    }

    @Override
    public void setKey(final String key) {
        // key is not used
    }

    @Override
    public T getInput() {
        return input;
    }

    @Override
    public void setInput(final T input) {
        this.input = input;
    }

    public String getParentSchemaId() {
        return parentSchemaId;
    }

    public void setParentSchemaId(final String parentSchemaId) {
        this.parentSchemaId = parentSchemaId;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(final Schema schema) {
        this.schema = schema;
    }

    public String getGraphId() {
        return graphId;
    }

    public void setGraphId(final String graphId) {
        this.graphId = graphId;
    }

    public String getParentStorePropertiesId() {
        return parentStorePropertiesId;
    }

    public void setParentStorePropertiesId(final String parentStorePropertiesId) {
        this.parentStorePropertiesId = parentStorePropertiesId;
    }

    @JsonIgnore
    public StoreProperties getStoreProperties() {
        return storeProperties;
    }

    @JsonIgnore
    public void setStoreProperties(final StoreProperties storeProperties) {
        this.storeProperties = storeProperties;
    }

    @JsonGetter("storeProperties")
    public Properties getProperties() {
        return null != storeProperties ? storeProperties.getProperties() : null;
    }

    @JsonSetter("storeProperties")
    public void setProperties(final Properties properties) {
        if (null == properties) {
            this.storeProperties = null;
        } else {
            this.storeProperties = StoreProperties.loadStoreProperties(properties);
        }
    }

    @Override
    public TypeReference<T> getOutputTypeReference() {
        return (TypeReference) new TypeReferenceImpl.Object();
    }

    public static final class Builder<T> extends BaseBuilder<ExportToOtherGraph<T>, Builder<T>>
            implements ExportTo.Builder<ExportToOtherGraph<T>, T, Builder<T>> {
        public Builder() {
            super(new ExportToOtherGraph<>());
        }

        public Builder<T> graphId(final String graphId) {
            _getOp().setGraphId(graphId);
            return _self();
        }

        public Builder<T> parentStorePropertiesId(final String parentStorePropertiesId) {
            _getOp().setParentStorePropertiesId(parentStorePropertiesId);
            return _self();
        }

        public Builder<T> storeProperties(final StoreProperties storeProperties) {
            _getOp().setStoreProperties(storeProperties);
            return _self();
        }

        public Builder<T> parentSchemaId(final String parentSchemaId) {
            _getOp().setParentSchemaId(parentSchemaId);
            return _self();
        }

        public Builder<T> schema(final Schema schema) {
            _getOp().setSchema(schema);
            return _self();
        }

    }
}
