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
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.List;
import java.util.Map;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;

/**
 * <p>
 * An Operation used for adding graphs to a FederatedStore.
 * </p>
 * Requires:
 * <ul>
 * <li>graphId
 * <li>storeProperties and/or parentPropertiesId</li>
 * <li>schema and/or parentSchemaIds</li>
 * </ul>
 * <p>
 * parentId can be used solely, if known by the graphLibrary.
 * </p>
 * <p>
 * schema can be used solely.
 * </p>
 * <p>
 * storeProperties can be used, if authorised to by {@link uk.gov.gchq.gaffer.federatedstore.FederatedStore#isLimitedToLibraryProperties(uk.gov.gchq.gaffer.user.User)}
 * both non-parentId and parentId can be used, and will be merged together.
 * </p>
 *
 * @see uk.gov.gchq.gaffer.federatedstore.FederatedStore
 * @see Schema
 * @see uk.gov.gchq.gaffer.data.element.Properties
 * @see uk.gov.gchq.gaffer.graph.Graph
 */
public class AddSchema implements FederatedOperation {

    @Required
    private Schema schema;
    private List<String> parentSchemaIds;
    private Map<String, String> options;

    public AddSchema() {
        addOption(KEY_OPERATION_OPTIONS_GRAPH_IDS, "");
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(final Schema schema) {
        this.schema = schema;
    }


    @Override
    public AddSchema shallowClone() throws CloneFailedException {
        return new Builder()
                .options(options)
                .parentSchemaIds(parentSchemaIds)
                .schema(schema).build();
    }


    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public List<String> getParentSchemaIds() {
        return parentSchemaIds;
    }

    public void setParentSchemaIds(final List<String> parentSchemaIds) {
        this.parentSchemaIds = parentSchemaIds;
    }

    public static class Builder extends BaseBuilder<AddSchema, Builder> {
        public Builder() {
            super(new AddSchema());
        }

        public Builder schema(final Schema schema) {
            _getOp().setSchema(schema);
            return _self();
        }

        public Builder parentSchemaIds(final List<String> parentSchemaIds) {
            _getOp().setParentSchemaIds(parentSchemaIds);
            return _self();
        }

    }
}
