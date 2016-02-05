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

package gaffer.data.elementdefinition;

import gaffer.data.Validator;
import gaffer.data.element.IdentifierType;

/**
 * Extends TypedElementDefinition and adds getters and setters for Edge identifiers
 *
 * @see gaffer.data.elementdefinition.TypedEdgeDefinition.Builder
 */
public abstract class TypedEdgeDefinition extends TypedElementDefinition {

    public TypedEdgeDefinition(final Validator<? extends TypedElementDefinition> elementDefValidator) {
        super(elementDefValidator);
    }

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

    public static class Builder extends TypedElementDefinition.Builder {
        public Builder(final TypedEdgeDefinition elDef) {
            super(elDef);
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

        public TypedEdgeDefinition build() {
            return (TypedEdgeDefinition) super.build();
        }

        protected TypedEdgeDefinition getElementDef() {
            return (TypedEdgeDefinition) super.getElementDef();
        }
    }
}
