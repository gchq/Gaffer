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

package uk.gov.gchq.gaffer.operation.export.graph;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.export.ExportTo;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A {@code ExportToOtherAuthorisedGraph} operation is used to export the results
 * of carrying out a query on a Gaffer {@link uk.gov.gchq.gaffer.graph.Graph} to
 * a different graph.
 * The graphs that are available to be exported to are limited to predefined set.
 * This is a more restricted version of {@link ExportToOtherGraph}.
 */
@JsonPropertyOrder(value = {"class", "graphId", "input"}, alphabetic = true)

@Since("1.0.0")
public class ExportToOtherAuthorisedGraph implements
        MultiInput<Element>,
        ExportTo<Iterable<? extends Element>> {

    @Required
    private String graphId;
    private Iterable<? extends Element> input;
    private List<String> parentSchemaIds;
    private String parentStorePropertiesId;
    private Map<String, String> options;

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

    @Override
    public ExportToOtherAuthorisedGraph shallowClone() {
        return new ExportToOtherAuthorisedGraph.Builder()
                .graphId(graphId)
                .input(input)
                .parentSchemaIds(parentSchemaIds.toArray(new String[parentSchemaIds.size()]))
                .parentStorePropertiesId(parentStorePropertiesId)
                .options(options)
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

        public Builder parentSchemaIds(final List<String> parentSchemaIds) {
            if (null == _getOp().getParentSchemaIds()) {
                _getOp().setParentSchemaIds(parentSchemaIds);
            } else {
                _getOp().getParentSchemaIds().addAll(parentSchemaIds);
            }
            return _self();
        }
    }
}
