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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.export.ExportTo;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A {@code ExportToOtherGraph} operation is used to export the results of carrying
 * out a query on a Gaffer {@link uk.gov.gchq.gaffer.graph.Graph} to a different
 * graph.
 */
@JsonPropertyOrder(value = {"class", "input", "graphId"}, alphabetic = true)
@Since("1.0.0")
@Summary("Exports elements to another Graph")
public class ExportToOtherGraph implements
        MultiInput<Element>,
        ExportTo<Iterable<? extends Element>> {
    @Required
    private String graphId;

    private Iterable<? extends Element> input;

    private List<String> parentSchemaIds;
    private Schema schema;

    private String parentStorePropertiesId;
    private StoreProperties storeProperties;
    private Map<String, String> options;

    @Override
    public String getKey() {
        return null;
    }

    @Override
    public void setKey(final String key) {
        // key is not used
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
    public ExportToOtherGraph shallowClone() {
        return new ExportToOtherGraph.Builder()
                .graphId(graphId)
                .input(input)
                .parentSchemaIds(parentSchemaIds.toArray(new String[parentSchemaIds.size()]))
                .schema(schema)
                .parentStorePropertiesId(parentStorePropertiesId)
                .storeProperties(storeProperties)
                .options(options)
                .build();
    }

    @Override
    public TypeReference<Iterable<? extends Element>> getOutputTypeReference() {
        return new TypeReferenceImpl.IterableElement();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public static final class Builder extends BaseBuilder<ExportToOtherGraph, Builder>
            implements ExportTo.Builder<ExportToOtherGraph, Iterable<? extends Element>, Builder> {

        public Builder() {
            super(new ExportToOtherGraph());
        }

        public Builder graphId(final String graphId) {
            _getOp().setGraphId(graphId);
            return _self();
        }

        public Builder parentStorePropertiesId(final String parentStorePropertiesId) {
            _getOp().setParentStorePropertiesId(parentStorePropertiesId);
            return _self();
        }

        public Builder storeProperties(final StoreProperties storeProperties) {
            _getOp().setStoreProperties(storeProperties);
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

        public Builder schema(final Schema schema) {
            _getOp().setSchema(schema);
            return _self();
        }
    }
}
