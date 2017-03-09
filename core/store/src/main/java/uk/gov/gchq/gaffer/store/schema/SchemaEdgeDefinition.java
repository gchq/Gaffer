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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

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

    @JsonIgnore
    @Override
    public SchemaEdgeDefinition getExpandedDefinition() {
        return getExpandedDefinition(this);
    }

    private SchemaEdgeDefinition getExpandedDefinition(final SchemaEdgeDefinition edgeDef) {
        if (null == edgeDef.parents || edgeDef.parents.isEmpty()) {
            return this;
        }

        final SchemaEdgeDefinition.Builder builder = new SchemaEdgeDefinition.Builder();
        final Set<String> parents = new LinkedHashSet<>(edgeDef.parents);
        for (final String parent : edgeDef.parents) {
            final SchemaEdgeDefinition parentDef = getExpandedDefinition(parent);
            if (null != parentDef) {
                builder.merge(parentDef);
                parents.remove(parent);
            }
        }
        edgeDef.parents = Collections.unmodifiableSet(parents);
        builder.merge(edgeDef);
        return builder.build();
    }

    private SchemaEdgeDefinition getExpandedDefinition(final String parent) {
        SchemaEdgeDefinition parentDefinition = getSchemaReference().getEdge(parent);
        if (null == parentDefinition) {
            return null;
        }

        if (null == parentDefinition.parents || parentDefinition.parents.isEmpty()) {
            return parentDefinition;
        }

        parentDefinition = getExpandedDefinition(parentDefinition);
        getSchemaReference().getEdges().put(parent, parentDefinition);
        return parentDefinition;
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
            merge(schemaElementDef);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
