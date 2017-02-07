/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.data.elementdefinition.view;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

@JsonDeserialize(builder = GlobalViewElementDefinition.Builder.class)
public class GlobalViewElementDefinition extends ViewElementDefinition {
    protected Set<String> groups;

    @Override
    public void lock() {
        if (null != groups) {
            groups = Collections.unmodifiableSet(groups);
        }
    }

    public Set<String> getGroups() {
        return groups;
    }

    public int hashCode() {
        return new HashCodeBuilder(23, 5)
                .appendSuper(super.hashCode())
                .append(groups)
                .toHashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return null != obj
                && (obj instanceof GlobalViewElementDefinition)
                && equals((GlobalViewElementDefinition) obj);
    }

    public boolean equals(final GlobalViewElementDefinition entity) {
        return null != entity
                && new EqualsBuilder()
                .appendSuper(super.equals(entity))
                .append(groups, entity.getGroups())
                .isEquals();
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @SuppressFBWarnings(value = "CN_IDIOM_NO_SUPER_CALL", justification = "Only inherits from Object")
    @Override
    public GlobalViewElementDefinition clone() {
        return new GlobalViewElementDefinition.Builder().json(toJson(false)).build();
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>> extends ViewElementDefinition.BaseBuilder<CHILD_CLASS> {
        public BaseBuilder() {
            super(new GlobalViewElementDefinition());
        }

        protected BaseBuilder(final GlobalViewElementDefinition elementDef) {
            super(elementDef);
        }

        public CHILD_CLASS groups(final String... groups) {
            if (null == getElementDef().groups) {
                getElementDef().groups = new LinkedHashSet<>();
            }

            Collections.addAll(getElementDef().groups, groups);

            return self();
        }

        public GlobalViewElementDefinition getElementDef() {
            return (GlobalViewElementDefinition) super.getElementDef();
        }

        @Override
        @JsonIgnore
        public CHILD_CLASS json(final byte[] jsonBytes) throws SchemaException {
            return json(jsonBytes, GlobalViewElementDefinition.class);
        }

        @Override
        public GlobalViewElementDefinition build() {
            return (GlobalViewElementDefinition) super.build();
        }

        @Override
        protected abstract CHILD_CLASS self();
    }

    @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
    public static final class Builder extends BaseBuilder<Builder> {
        public Builder() {
        }

        public Builder(final GlobalViewElementDefinition viewElementDef) {
            super();
            merge(viewElementDef);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
