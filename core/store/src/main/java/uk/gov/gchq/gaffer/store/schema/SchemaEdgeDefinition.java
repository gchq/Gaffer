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

public class SchemaEdgeDefinition extends SchemaElementDefinition {
    public void setSource(final String typeName) {
        getIdentifierMap().put(IdentifierType.SOURCE, typeName);
    }

    public void setDestination(final String typeName) {
        getIdentifierMap().put(IdentifierType.DESTINATION, typeName);
    }

    public void setDirected(final String typeName) {
        getIdentifierMap().put(IdentifierType.DIRECTED, typeName);
    }

    public String getSource() {
        return getIdentifierTypeName(IdentifierType.SOURCE);
    }

    public String getDestination() {
        return getIdentifierTypeName(IdentifierType.DESTINATION);
    }

    public String getDirected() {
        return getIdentifierTypeName(IdentifierType.DIRECTED);
    }

    public static class Builder extends SchemaElementDefinition.Builder {
        public Builder() {
            this(new SchemaEdgeDefinition());
        }

        public Builder(final SchemaEdgeDefinition elDef) {
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

        @Override
        public Builder groupBy(final String... propertyName) {
            return (Builder) super.groupBy(propertyName);
        }

        @Override
        public Builder validator(final ElementFilter validator) {
            return (Builder) super.validator(validator);
        }

        public Builder source(final Class<?> typeClass) {
            return source(typeClass.getName(), typeClass);
        }

        public Builder source(final String exisitingTypeName) {
            identifier(IdentifierType.SOURCE, exisitingTypeName);
            return this;
        }

        public Builder source(final String typeName, final TypeDefinition type) {
            type(typeName, type);
            return source(typeName);
        }

        public Builder source(final String typeName, final Class<?> typeClass) {
            return source(typeName, new TypeDefinition(typeClass));
        }

        public Builder destination(final Class<?> typeClass) {
            return destination(typeClass.getName(), typeClass);
        }

        public Builder destination(final String exisitingTypeName) {
            identifier(IdentifierType.DESTINATION, exisitingTypeName);
            return this;
        }

        public Builder destination(final String typeName, final TypeDefinition type) {
            type(typeName, type);
            return destination(typeName);
        }

        public Builder destination(final String typeName, final Class<?> typeClass) {
            return destination(typeName, new TypeDefinition(typeClass));
        }

        public Builder directed(final Class<?> typeClass) {
            return directed(typeClass.getName(), typeClass);
        }

        public Builder directed(final String exisitingTypeName) {
            identifier(IdentifierType.DIRECTED, exisitingTypeName);
            return this;
        }

        public Builder directed(final String typeName, final TypeDefinition type) {
            type(typeName, type);
            return directed(typeName);
        }

        public Builder directed(final String typeName, final Class<?> typeClass) {
            return directed(typeName, new TypeDefinition(typeClass));
        }

        @Override
        public SchemaEdgeDefinition build() {
            return (SchemaEdgeDefinition) super.build();
        }

        @Override
        protected SchemaEdgeDefinition getElementDef() {
            return (SchemaEdgeDefinition) super.getElementDef();
        }
    }
}
