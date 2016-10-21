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

package uk.gov.gchq.gaffer.data.elementdefinition.view;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.data.elementdefinition.ElementDefinitions;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashSet;

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
 * @see uk.gov.gchq.gaffer.data.elementdefinition.view.View.Builder
 * @see uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition
 * @see uk.gov.gchq.gaffer.data.element.function.ElementFilter
 * @see uk.gov.gchq.gaffer.data.element.function.ElementTransformer
 */
public class View extends ElementDefinitions<ViewElementDefinition, ViewElementDefinition> {
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

    public LinkedHashSet<String> getElementGroupBy(final String group) {
        ViewElementDefinition viewElementDef = (ViewElementDefinition) super.getElement(group);
        if (null == viewElementDef) {
            return null;
        }

        return viewElementDef.getGroupBy();
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
