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

import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.graph.library.GraphLibrary;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.export.ExportTo;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.ValidationResult;

public class ExportToOtherGraph<T> implements
        Operation,
        ExportTo<T> {
    @Required
    private String graphId;

    private T input;
    private GraphLibrary graphLibrary;

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

    public StoreProperties getStoreProperties() {
        return storeProperties;
    }

    public void setStoreProperties(final StoreProperties storeProperties) {
        this.storeProperties = storeProperties;
    }

    public GraphLibrary getGraphLibrary() {
        return graphLibrary;
    }

    public void setGraphLibrary(final GraphLibrary graphLibrary) {
        this.graphLibrary = graphLibrary;
    }

    @Override
    public TypeReference<T> getOutputTypeReference() {
        return (TypeReference) new TypeReferenceImpl.Object();
    }

    @Override
    public ValidationResult validate() {
        final ValidationResult result = ExportTo.super.validate();

        if (null == graphLibrary) {
            // No graph library so we cannot look up the graphId/schemaId/storePropertiesId
            if (null != parentSchemaId) {
                result.addError("parentSchemaId cannot be used without a GraphLibrary");
            }
            if (null != parentStorePropertiesId) {
                result.addError("parentStorePropertiesId cannot be used without a GraphLibrary");
            }
        } else if (graphLibrary.exists(graphId)) {
            if (null != parentSchemaId) {
                throw new IllegalArgumentException("GraphId " + graphId + " already exists so you cannot use a different schema. Do not set the parentSchemaId field");
            }
            if (null != schema) {
                throw new IllegalArgumentException("GraphId " + graphId + "already exists so you cannot provide a different schema. Do not set the schema field.");
            }
            if (null != parentStorePropertiesId) {
                throw new IllegalArgumentException("GraphId " + graphId + " already exists so you cannot use different store properties. Do not set the parentStorePropertiesId field");
            }
            if (null != storeProperties) {
                throw new IllegalArgumentException("GraphId " + graphId + " already exists so you cannot provide different store properties. Do not set the storeProperties field.");
            }
        } else {
            if (null != parentSchemaId) {
                final Schema parentSchema = graphLibrary.getSchema(parentSchemaId);
                if (null == parentSchema) {
                    throw new IllegalArgumentException("Schema could not be found in the graphLibrary with id: " + parentSchemaId);
                }
            }
            if (null != parentStorePropertiesId) {
                final Schema parentStoreProperties = graphLibrary.getSchema(parentStorePropertiesId);
                if (null == parentStoreProperties) {
                    throw new IllegalArgumentException("Store properties could not be found in the graphLibrary with id: " + parentStorePropertiesId);
                }
            }
        }
        return result;
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

        public Builder<T> graphLibrary(final GraphLibrary graphLibrary) {
            _getOp().setGraphLibrary(graphLibrary);
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
