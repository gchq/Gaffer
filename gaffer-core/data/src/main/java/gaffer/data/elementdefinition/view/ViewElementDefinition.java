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

import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.element.function.ElementTransformer;
import gaffer.data.elementdefinition.TypedElementDefinition;

/**
 * A <code>ViewElementDefinition</code> extends {@link gaffer.data.elementdefinition.TypedElementDefinition} and adds
 * the ability to specify a {@link gaffer.data.element.function.ElementTransformer} and a
 * {@link gaffer.data.element.function.ElementFilter}.
 */
public abstract class ViewElementDefinition extends TypedElementDefinition {
    private ElementTransformer transformer;
    private ElementFilter filter;

    public ViewElementDefinition() {
        super(new ViewElementDefinitionValidator());
    }

    public ElementTransformer getTransformer() {
        return transformer;
    }

    public void setTransformer(final ElementTransformer transformer) {
        this.transformer = transformer;
    }

    public ElementFilter getFilter() {
        return filter;
    }

    public void setFilter(final ElementFilter filter) {
        this.filter = filter;
    }

    public abstract static class Builder extends TypedElementDefinition.Builder {
        public Builder(final ViewElementDefinition elDef) {
            super(elDef);
        }

        public Builder property(final String propertyName, final Class<?> clazz) {
            return (Builder) super.property(propertyName, clazz);
        }

        public Builder identifier(final IdentifierType identifierType, final Class<?> clazz) {
            return (Builder) super.identifier(identifierType, clazz);
        }

        public Builder filter(final ElementFilter filter) {
            getElementDef().setFilter(filter);
            return this;
        }

        public Builder transformer(final ElementTransformer transformer) {
            getElementDef().setTransformer(transformer);
            return this;
        }

        public ViewElementDefinition build() {
            return getElementDef();
        }

        @Override
        protected ViewElementDefinition getElementDef() {
            return (ViewElementDefinition) super.getElementDef();
        }
    }
}
