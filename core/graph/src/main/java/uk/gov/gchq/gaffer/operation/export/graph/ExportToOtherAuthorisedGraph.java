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
import com.google.common.collect.Lists;
import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.export.ExportTo;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.util.Collections;
import java.util.List;

public class ExportToOtherAuthorisedGraph<T> implements Operation, ExportTo<T> {

    @Required
    private String graphId;

    private T input;

    private List<String> parentSchemaIds;

    private String parentStorePropertiesId;

    public String getGraphId() {
        return graphId;
    }

    public void setGraphId(final String graphId) {
        this.graphId = graphId;
    }

    @Override
    public T getInput() {
        return input;
    }

    @Override
    public void setInput(final T input) {
        this.input = input;
    }

    public List<String> getParentSchemaIds() {
        return parentSchemaIds;
    }

    public void setParentSchemaIds(final List<String> parentSchemaId) {
        this.parentSchemaIds = parentSchemaIds;
    }

    public String getParentStorePropertiesId() {
        return parentStorePropertiesId;
    }

    public void setParentStorePropertiesId(final String parentStorePropertiesId) {
        this.parentStorePropertiesId = parentStorePropertiesId;
    }

    @Override
    public String getKey() {
        return null;
    }

    @Override
    public void setKey(final String key) {
        // Key not used
    }

    @Override
    public TypeReference<T> getOutputTypeReference() {
        return (TypeReference) new TypeReferenceImpl.Object();
    }

    public static final class Builder<T> extends BaseBuilder<ExportToOtherAuthorisedGraph<T>, Builder<T>>
            implements ExportTo.Builder<ExportToOtherAuthorisedGraph<T>, T, Builder<T>> {
        public Builder() {
            super(new ExportToOtherAuthorisedGraph<>());
        }

        public Builder<T> graphId(final String graphId) {
            _getOp().setGraphId(graphId);
            return _self();
        }

        public Builder<T> parentStorePropertiesId(final String parentStorePropertiesId) {
            _getOp().setParentStorePropertiesId(parentStorePropertiesId);
            return _self();
        }

        public Builder<T> parentSchemaId(final String... parentSchemaIds) {
            if (null == _getOp().getParentSchemaIds()) {
                _getOp().setParentSchemaIds(Lists.newArrayList(parentSchemaIds));
            } else {
                Collections.addAll(_getOp().getParentSchemaIds(), parentSchemaIds);
            }
            return _self();
        }

    }
}
