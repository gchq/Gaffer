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

public class ViewEntityDefinition extends ViewElementDefinition {
    private static final long serialVersionUID = -2620877248554434905L;

    public void setVertex(final String className) {
        getIdentifierMap().put(IdentifierType.VERTEX, className);
    }

    public String getVertex() {
        return getIdentifierClassName(IdentifierType.VERTEX);
    }

    public static class Builder extends ViewElementDefinition.Builder {
        public Builder() {
            this(new ViewEntityDefinition());
        }

        public Builder(final ViewEntityDefinition elDef) {
            super(elDef);
        }

        public Builder property(final String propertyName, final Class<?> clazz) {
            return (Builder) super.property(propertyName, clazz);
        }

        public Builder vertex(final Class<?> clazz) {
            identifier(IdentifierType.VERTEX, clazz);
            return this;
        }

        public Builder filter(final ElementFilter filter) {
            return (Builder) super.filter(filter);
        }

        public Builder transformer(final ElementTransformer transformer) {
            return (Builder) super.transformer(transformer);
        }

        public ViewEntityDefinition build() {
            return getElementDef();
        }

        @Override
        protected ViewEntityDefinition getElementDef() {
            return (ViewEntityDefinition) super.getElementDef();
        }
    }
}
