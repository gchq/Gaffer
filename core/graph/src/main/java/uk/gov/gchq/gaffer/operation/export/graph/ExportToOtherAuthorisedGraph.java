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
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.export.ExportTo;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.util.Collections;
import java.util.List;

public class ExportToOtherAuthorisedGraph implements
        Operation,
        MultiInput<Element>,
        ExportTo<Iterable<? extends Element>> {

    @Required
    private String graphId;
    private Iterable<? extends Element> input;
    private List<String> parentSchemaIds;
    private String parentStorePropertiesId;

    public String getGraphId() {
        return graphId;
    }

    public void setGraphId(final String graphId) {
        this.graphId = graphId;
    }

    @Override
    public Iterable<? extends Element> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends Element> input) {
        this.input = input;
    }

    public List<String> getParentSchemaIds() {
        return parentSchemaIds;
    }

    public void setParentSchemaIds(final List<String> parentSchemaIds) {
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
        // Key not used
        return null;
    }

    @Override
    public void setKey(final String key) {
        // Key not used
    }

    @Override
    public TypeReference<Iterable<? extends Element>> getOutputTypeReference() {
        return (TypeReference) new TypeReferenceImpl.Object();
    }

    public static final class Builder extends BaseBuilder<ExportToOtherAuthorisedGraph, Builder>
            implements ExportTo.Builder<ExportToOtherAuthorisedGraph, Iterable<? extends Element>, Builder> {
        public Builder() {
            super(new ExportToOtherAuthorisedGraph());
        }

        public Builder graphId(final String graphId) {
            _getOp().setGraphId(graphId);
            return _self();
        }

        public Builder parentStorePropertiesId(final String parentStorePropertiesId) {
            _getOp().setParentStorePropertiesId(parentStorePropertiesId);
            return _self();
        }

        public Builder parentSchemaIds(final String... parentSchemaIds) {
            if (null == _getOp().getParentSchemaIds()) {
                _getOp().setParentSchemaIds(Lists.newArrayList(parentSchemaIds));
            } else {
                Collections.addAll(_getOp().getParentSchemaIds(), parentSchemaIds);
            }
            return _self();
        }

    }
}
