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

package uk.gov.gchq.gaffer.store.schema;

import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;

public class SchemaEntityDefinition extends SchemaElementDefinition {
    public void setVertex(final String className) {
        getIdentifierMap().put(IdentifierType.VERTEX, className);
    }

    public String getVertex() {
        return getIdentifierTypeName(IdentifierType.VERTEX);
    }

    public static class Builder extends SchemaElementDefinition.Builder {
        public Builder() {
            this(new SchemaEntityDefinition());
        }

        public Builder(final SchemaEntityDefinition elDef) {
            super(elDef);
        }

        @Override
        public Builder property(final String propertyName, final Class<?> typeClass) {
            return (Builder) super.property(propertyName, typeClass);
        }

        @Override
        public Builder property(final String propertyName, final String exisitingTypeName) {
            return (Builder) super.property(propertyName, exisitingTypeName);
        }

        @Override
        public Builder property(final String propertyName, final String typeName, final TypeDefinition type) {
            return (Builder) super.property(propertyName, typeName, type);
        }

        @Override
        public Builder property(final String propertyName, final String typeName, final Class<?> typeClass) {
            return (Builder) super.property(propertyName, typeName, typeClass);
        }

        public Builder vertex(final String exisitingTypeName) {
            identifier(IdentifierType.VERTEX, exisitingTypeName);
            return this;
        }

        public Builder vertex(final Class<?> typeClass) {
            return vertex(typeClass.getName(), typeClass);
        }

        public Builder vertex(final String typeName, final TypeDefinition type) {
            type(typeName, type);
            return vertex(typeName);
        }

        public Builder vertex(final String typeName, final Class<?> typeClass) {
            return vertex(typeName, new TypeDefinition(typeClass));
        }

        @Override
        public Builder groupBy(final String... propertyName) {
            return (Builder) super.groupBy(propertyName);
        }

        @Override
        public Builder validator(final ElementFilter validator) {
            return (Builder) super.validator(validator);
        }

        @Override
        public SchemaEntityDefinition build() {
            return (SchemaEntityDefinition) super.build();
        }

        @Override
        protected SchemaEntityDefinition getElementDef() {
            return (SchemaEntityDefinition) super.getElementDef();
        }
    }
}
