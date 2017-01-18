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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import uk.gov.gchq.gaffer.data.element.IdentifierType;

@JsonDeserialize(builder = SchemaEdgeDefinition.Builder.class)
public class SchemaEdgeDefinition extends SchemaElementDefinition {
    public String getSource() {
        return getIdentifierTypeName(IdentifierType.SOURCE);
    }

    public String getDestination() {
        return getIdentifierTypeName(IdentifierType.DESTINATION);
    }

    public String getDirected() {
        return getIdentifierTypeName(IdentifierType.DIRECTED);
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>> extends SchemaElementDefinition.BaseBuilder<SchemaEdgeDefinition, CHILD_CLASS> {
        protected BaseBuilder() {
            super(new SchemaEdgeDefinition());
        }

        protected BaseBuilder(final SchemaEdgeDefinition schemaElementDef) {
            this();
            merge(schemaElementDef);
        }

        public CHILD_CLASS source(final String typeName) {
            getElementDef().identifiers.put(IdentifierType.SOURCE, typeName);
            return self();
        }

        public CHILD_CLASS destination(final String typeName) {
            getElementDef().identifiers.put(IdentifierType.DESTINATION, typeName);
            return self();
        }

        public CHILD_CLASS directed(final String typeName) {
            getElementDef().identifiers.put(IdentifierType.DIRECTED, typeName);
            return self();
        }
    }

    @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
    public static final class Builder extends BaseBuilder<Builder> {
        public Builder() {
        }

        public Builder(final SchemaEdgeDefinition schemaElementDef) {
            this();
            merge(schemaElementDef);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
