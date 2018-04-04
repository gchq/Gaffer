/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.add;

import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.Since;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * An Operation used for adding {@link Schema} to the {@link uk.gov.gchq.gaffer.store.library.GraphLibrary} of a store.
 *
 * @see Schema
 */
@Since("1.5.0")
public class AddSchemaToLibrary implements Operation {

    @Required
    private Schema schema;

    @Required
    private String id;
    /**
     * A list of schema Id's held within the Library to be retrieved
     * and merged to form a new schema, before be merged with the optional
     * {@link #schema} field.
     */
    private List<String> parentSchemaIds;
    private Map<String, String> options;

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(final Schema schema) {
        this.schema = schema;
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    @Override
    public AddSchemaToLibrary shallowClone() throws CloneFailedException {
        return new Builder()
                .options(options)
                .parentSchemaIds(parentSchemaIds)
                .schema(schema)
                .id(id)
                .build();
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

    public static class Builder extends BaseBuilder<AddSchemaToLibrary, Builder> {
        public Builder() {
            super(new AddSchemaToLibrary());
        }

        public Builder schema(final Schema schema) {
            _getOp().setSchema(schema);
            return _self();
        }

        public Builder parentSchemaIds(final List<String> parentSchemaIds) {
            if (null == _getOp().getParentSchemaIds()) {
                _getOp().setParentSchemaIds(parentSchemaIds);
            } else {
                _getOp().getParentSchemaIds().addAll(parentSchemaIds);
            }
            return _self();
        }

        public Builder parentSchemaIds(final String... parentSchemaIds) {
            parentSchemaIds(Arrays.asList(parentSchemaIds));
            return _self();
        }

        public Builder id(final String id) {
            _getOp().setId(id);
            return _self();
        }
    }
}
