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

import gaffer.data.elementdefinition.ElementDefinitions;
import gaffer.data.elementdefinition.schema.exception.SchemaException;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.Collection;

/**
 * The <code>View</code> defines the {@link gaffer.data.element.Element}s to be returned for an operation.
 * A view should contain {@link gaffer.data.element.Edge} and {@link gaffer.data.element.Entity} types required and
 * for each group it can optionally contain an {@link gaffer.data.element.function.ElementFilter} and a
 * {@link gaffer.data.element.function.ElementTransformer}.
 * The {@link gaffer.function.FilterFunction}s within the ElementFilter describe the how the elements should be filtered.
 * The {@link gaffer.function.TransformFunction}s within ElementTransformer allow transient properties to be created
 * from other properties and identifiers.
 * Any identifiers, properties or transient properties used in filters and transforms must be listed within the element
 * definition along with its type.
 *
 * @see gaffer.data.elementdefinition.view.View.Builder
 * @see gaffer.data.elementdefinition.view.ViewElementDefinition
 * @see gaffer.data.element.function.ElementFilter
 * @see gaffer.data.element.function.ElementTransformer
 */
public class View extends ElementDefinitions<ViewEntityDefinition, ViewEdgeDefinition> {
    private static final long serialVersionUID = 3056841284342147461L;

    public View() {
        super();
    }

    public View(final Collection<String> entityGroups, final Collection<String> edgeGroups) {
        super();
        for (String group : entityGroups) {
            addEntity(group, new ViewEntityDefinition());
        }
        for (String group : edgeGroups) {
            addEdge(group, new ViewEdgeDefinition());
        }
    }

    public static View fromJson(final InputStream inputStream) throws SchemaException {
        return fromJson(inputStream, View.class);
    }

    public static View fromJson(final Path filePath) throws SchemaException {
        return fromJson(filePath, View.class);
    }

    public static View fromJson(final byte[] jsonBytes) throws SchemaException {
        return fromJson(jsonBytes, View.class);
    }

    @Override
    public ViewElementDefinition getElement(final String group) {
        return (ViewElementDefinition) super.getElement(group);
    }

    public static class Builder extends ElementDefinitions.Builder<ViewEntityDefinition, ViewEdgeDefinition> {
        public Builder() {
            this(new View());
        }

        public Builder(final View view) {
            super(view);
        }

        @Override
        public Builder edge(final String group, final ViewEdgeDefinition elementDef) {
            return (Builder) super.edge(group, elementDef);
        }

        public Builder edge(final String group) {
            return edge(group, new ViewEdgeDefinition());
        }

        @Override
        public Builder entity(final String group, final ViewEntityDefinition elementDef) {
            return (Builder) super.entity(group, elementDef);
        }

        public Builder entity(final String group) {
            return entity(group, new ViewEntityDefinition());
        }

        @Override
        public View build() {
            return (View) super.build();
        }
    }
}
