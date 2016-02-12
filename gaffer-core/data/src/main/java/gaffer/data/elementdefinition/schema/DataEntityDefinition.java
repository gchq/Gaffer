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

package gaffer.data.elementdefinition.schema;

import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementAggregator;
import gaffer.data.element.function.ElementFilter;

public class DataEntityDefinition extends DataElementDefinition {
    private static final long serialVersionUID = 2307022506203200871L;

    public void setVertex(final String className) {
        getIdentifierMap().put(IdentifierType.VERTEX, className);
    }

    public String getVertex() {
        return getIdentifierClassName(IdentifierType.VERTEX);
    }

    public static class Builder extends DataElementDefinition.Builder {
        public Builder() {
            this(new DataEntityDefinition());
        }

        public Builder(final DataEntityDefinition elDef) {
            super(elDef);
        }

        public Builder property(final String propertyName, final Class<?> clazz) {
            return (Builder) super.property(propertyName, clazz);
        }

        public Builder vertex(final Class<?> clazz) {
            identifier(IdentifierType.VERTEX, clazz);
            return this;
        }

        public Builder validator(final ElementFilter validator) {
            return (Builder) super.validator(validator);
        }

        public Builder aggregator(final ElementAggregator aggregator) {
            return (Builder) super.aggregator(aggregator);
        }

        public DataEntityDefinition build() {
            return (DataEntityDefinition) super.build();
        }

        @Override
        protected DataEntityDefinition getElementDef() {
            return (DataEntityDefinition) super.getElementDef();
        }
    }
}
