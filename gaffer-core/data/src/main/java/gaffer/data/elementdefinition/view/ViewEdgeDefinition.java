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

public class ViewEdgeDefinition extends ViewElementDefinition {
    private static final long serialVersionUID = -7418280514323572835L;

    public void setSource(final String className) {
        getIdentifierMap().put(IdentifierType.SOURCE, className);
    }

    public void setDestination(final String className) {
        getIdentifierMap().put(IdentifierType.DESTINATION, className);
    }

    public void setDirected(final String className) {
        getIdentifierMap().put(IdentifierType.DIRECTED, className);
    }

    public String getSource() {
        return getIdentifierClassName(IdentifierType.SOURCE);
    }

    public String getDestination() {
        return getIdentifierClassName(IdentifierType.DESTINATION);
    }

    public String getDirected() {
        return getIdentifierClassName(IdentifierType.DIRECTED);
    }

    public static class Builder extends ViewElementDefinition.Builder {
        public Builder() {
            this(new ViewEdgeDefinition());
        }

        public Builder(final ViewEdgeDefinition elDef) {
            super(elDef);
        }

        public Builder property(final String propertyName, final Class<?> clazz) {
            return (Builder) super.property(propertyName, clazz);
        }

        public Builder source(final Class<?> clazz) {
            identifier(IdentifierType.SOURCE, clazz);
            return this;
        }

        public Builder destination(final Class<?> clazz) {
            identifier(IdentifierType.DESTINATION, clazz);
            return this;
        }

        public Builder directed(final Class<?> clazz) {
            identifier(IdentifierType.DIRECTED, clazz);
            return this;
        }

        public Builder filter(final ElementFilter filter) {
            return (Builder) super.filter(filter);
        }

        public Builder transformer(final ElementTransformer transformer) {
            return (Builder) super.transformer(transformer);
        }

        public ViewEdgeDefinition build() {
            return getElementDef();
        }

        @Override
        protected ViewEdgeDefinition getElementDef() {
            return (ViewEdgeDefinition) super.getElementDef();
        }
    }
}
