/*
 * Copyright 2016 Crown Copyright
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

package gaffer.data.elementdefinition.view;

import com.fasterxml.jackson.annotation.JsonGetter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gaffer.commonutil.CommonConstants;
import gaffer.data.elementdefinition.ElementDefinitions;
import gaffer.data.elementdefinition.exception.SchemaException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * The <code>View</code> defines the {@link gaffer.data.element.Element}s to be returned for an operation.
 * A view should contain {@link gaffer.data.element.Edge} and {@link gaffer.data.element.Entity} types required and
 * for each group it can optionally contain an {@link gaffer.data.element.function.ElementFilter} and a
 * {@link gaffer.data.element.function.ElementTransformer}.
 * The {@link gaffer.function.FilterFunction}s within the ElementFilter describe the how the elements should be filtered.
 * The {@link gaffer.function.TransformFunction}s within ElementTransformer allow transient properties to be created
 * from other properties and identifiers.
 * It also contains any transient properties that are created in transform functions.
 *
 * @see gaffer.data.elementdefinition.view.View.Builder
 * @see gaffer.data.elementdefinition.view.ViewElementDefinition
 * @see gaffer.data.element.function.ElementFilter
 * @see gaffer.data.element.function.ElementTransformer
 */
public class View extends ElementDefinitions<ViewElementDefinition, ViewElementDefinition> {
    private List<String> groupByProperties;

    public View() {
        super();
    }

    public static View fromJson(final InputStream inputStream) throws SchemaException {
        return fromJson(View.class, inputStream);
    }

    public static View fromJson(final Path filePath) throws SchemaException {
        return fromJson(View.class, filePath);
    }

    public static View fromJson(final byte[] jsonBytes) throws SchemaException {
        return fromJson(View.class, jsonBytes);
    }

    public List<String> getGroupByProperties() {
        return groupByProperties;
    }

    public void setSummariseGroupByProperties(final List<String> groupByProperties) {
        this.groupByProperties = groupByProperties;
    }

    public void setSummarise(final boolean summarise) {
        if (summarise) {
            if (null == groupByProperties) {
                // An empty list of group by properties will mean all properties
                // will be aggregated together - none will be held constant.
                groupByProperties = new ArrayList<>(0);
            }
        } else {
            // Setting group by properties will mean grouping by properties is
            // disabled and query time aggregation (summarisation) will not be
            // carried out.
            groupByProperties = null;
        }
    }

    public boolean isSummarise() {
        return null != groupByProperties;
    }

    @JsonGetter("summariseGroupByProperties")
    List<String> getSummariseGroupByPropertiesJson() {
        if (null == groupByProperties || groupByProperties.isEmpty()) {
            return null; // the summarise flag in json will be used instead.
        }

        return groupByProperties;
    }

    @SuppressFBWarnings(value = "NP_BOOLEAN_RETURN_NULL", justification = "Required so the summarise boolean isn't included in json")
    @JsonGetter("summarise")
    Boolean isSummariseJson() {
        if (null == groupByProperties) {
            return false;
        }

        if (groupByProperties.isEmpty()) {
            return true;
        }

        return null; // If group by properties have been set then these should be included in the json instead of the summarise flag.
    }

    @Override
    public String toString() {
        try {
            return "View" + new String(toJson(true), CommonConstants.UTF_8);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ViewElementDefinition getElement(final String group) {
        return (ViewElementDefinition) super.getElement(group);
    }

    public static class Builder extends ElementDefinitions.Builder<ViewElementDefinition, ViewElementDefinition> {
        public Builder() {
            this(new View());
        }

        public Builder(final View view) {
            super(view);
        }

        @Override
        public Builder edge(final String group, final ViewElementDefinition elementDef) {
            return (Builder) super.edge(group, elementDef);
        }

        public Builder edge(final String group) {
            return edge(group, new ViewElementDefinition());
        }

        public Builder edges(final Collection<String> groups) {
            for (String group : groups) {
                edge(group);
            }

            return this;
        }

        @Override
        public Builder entity(final String group, final ViewElementDefinition elementDef) {
            return (Builder) super.entity(group, elementDef);
        }

        public Builder entity(final String group) {
            return entity(group, new ViewElementDefinition());
        }

        public Builder entities(final Collection<String> groups) {
            for (String group : groups) {
                entity(group);
            }

            return this;
        }

        public Builder groupByProperties(final String... groupByProperties) {
            if (null == getElementDefs().getGroupByProperties()) {
                getElementDefs().setSummariseGroupByProperties(new ArrayList<String>());
            }
            Collections.addAll(getElementDefs().getGroupByProperties(), groupByProperties);
            return this;
        }

        public Builder summarise(final boolean summarise) {
            getElementDefs().setSummarise(summarise);
            return this;
        }

        @Override
        public View build() {
            return (View) super.build();
        }

        @Override
        protected View getElementDefs() {
            return (View) super.getElementDefs();
        }
    }
}
