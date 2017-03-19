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

@JsonDeserialize(builder = SchemaEntityDefinition.Builder.class)
public class SchemaEntityDefinition extends SchemaElementDefinition {
    public String getVertex() {
        return getIdentifierTypeName(IdentifierType.VERTEX);
    }

    @JsonIgnore
    @Override
    public SchemaEntityDefinition getExpandedDefinition() {
        return getExpandedDefinition(this);
    }

    private SchemaEntityDefinition getExpandedDefinition(final SchemaEntityDefinition entityDef) {
        if (null == entityDef.parents || entityDef.parents.isEmpty()) {
            return this;
        }

        final SchemaEntityDefinition.Builder builder = new SchemaEntityDefinition.Builder();
        final Set<String> parents = new LinkedHashSet<>(entityDef.parents);
        for (final String parent : entityDef.parents) {
            final SchemaEntityDefinition parentDef = getExpandedDefinition(parent);
            if (null != parentDef) {
                builder.merge(parentDef);
                parents.remove(parent);
            }
        }
        entityDef.parents = Collections.unmodifiableSet(parents);
        builder.merge(entityDef);
        return builder.build();
    }

    private SchemaEntityDefinition getExpandedDefinition(final String parent) {
        SchemaEntityDefinition parentDefinition = getSchemaReference().getEntity(parent);
        if (null == parentDefinition) {
            return null;
        }

        if (null == parentDefinition.parents || parentDefinition.parents.isEmpty()) {
            return parentDefinition;
        }

        parentDefinition = getExpandedDefinition(parentDefinition);
        getSchemaReference().getEntities().put(parent, parentDefinition);
        return parentDefinition;
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>> extends SchemaElementDefinition.BaseBuilder<SchemaEntityDefinition, CHILD_CLASS> {
        protected BaseBuilder() {
            super(new SchemaEntityDefinition());
        }


        public CHILD_CLASS vertex(final String typeName) {
            getElementDef().identifiers.put(IdentifierType.VERTEX, typeName);
            return self();
        }
    }

    @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
    public static final class Builder extends BaseBuilder<Builder> {
        public Builder() {
        }

        public Builder(final SchemaEntityDefinition schemaElementDef) {
            merge(schemaElementDef);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
