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
 * Extends TypedElementDefinition and adds a getter and setter for Entity vertex.
 *
 * @see gaffer.data.elementdefinition.TypedEntityDefinition.Builder
 */
public abstract class TypedEntityDefinition extends TypedElementDefinition {

    public TypedEntityDefinition(final Validator<? extends TypedElementDefinition> elementDefValidator) {
        super(elementDefValidator);
    }

    public void setVertex(final String className) {
        getIdentifierMap().put(IdentifierType.VERTEX, className);
    }

    public String getVertex() {
        return getIdentifierClassName(IdentifierType.VERTEX);
    }

    public static class Builder extends TypedElementDefinition.Builder {
        public Builder(final TypedEntityDefinition elDef) {
            super(elDef);
        }


        public TypedEntityDefinition build() {
            return (TypedEntityDefinition) super.build();
        }

        protected TypedEntityDefinition getElementDef() {
            return (TypedEntityDefinition) super.getElementDef();
        }
    }
}
