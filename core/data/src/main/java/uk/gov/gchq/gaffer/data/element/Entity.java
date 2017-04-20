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

package uk.gov.gchq.gaffer.data.element;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.data.element.Entity.Builder;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import java.util.Map;

/**
 * An <code>Entity</code> in an {@link uk.gov.gchq.gaffer.data.element.Element} containing a single vertex.
 * The vertex can be any type of {@link java.lang.Object}.
 * There is no requirement for this vertex to connect to an {@link uk.gov.gchq.gaffer.data.element.Edge}'s source or
 * destination vertex - for example you could have a 'graph' of just entities.
 * Entities are designed so that multiple entities can share the same vertex but are distinguished via their
 * group.
 *
 * @see uk.gov.gchq.gaffer.data.element.Entity.Builder
 */
@JsonDeserialize(builder = Builder.class)
public class Entity extends Element implements EntityId {
    private static final Logger LOGGER = LoggerFactory.getLogger(Entity.class);
    private static final long serialVersionUID = 2863628004463113755L;
    private Object vertex;

    public Entity(final String group, final Object vertex) {
        super(group);
        this.vertex = vertex;
    }

    public Entity(final Builder builder) {
        super(builder.group);
        this.vertex = builder.vertex;
        this.properties = builder.properties;
    }

    public Object getVertex() {
        return vertex;
    }

    @Override
    public Object getIdentifier(final IdentifierType identifierType) {
        switch (identifierType) {
            case VERTEX:
                return getVertex();
            default:
                return null;
        }
    }

    @Override
    public void putIdentifier(final IdentifierType identifierType, final Object propertyToBeSet) {
        switch (identifierType) {
            case VERTEX:
                this.vertex = propertyToBeSet;
                break;
            default:
                LOGGER.error("Unknown identifier type: " + identifierType + " detected.");
                break;
        }
    }

    public int hashCode() {
        return new HashCodeBuilder(23, 5)
                .appendSuper(super.hashCode())
                .append(vertex)
                .toHashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return null != obj
                && (obj instanceof Entity)
                && equals((Entity) obj);
    }

    public boolean equals(final Entity entity) {
        return null != entity
                && new EqualsBuilder()
                .appendSuper(super.equals(entity))
                .append(vertex, entity.getVertex())
                .isEquals();
    }

    @Override
    public Entity emptyClone() {
        return new Entity(this.getGroup(), this.getVertex());
    }

    @Override
    public String toString() {
        return "Entity{vertex=" + vertex + super.toString() + "} ";
    }

    @JsonPOJOBuilder(withPrefix = "")
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
    public static class Builder {
        private String group = "UNKNOWN";
        private Object vertex;
        private Properties properties = new Properties();

        public Builder group(final String group) {
            this.group = group;
            return this;
        }

        @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
        public Builder vertex(final Object vertex) {
            this.vertex = vertex;
            return this;
        }

        @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
        public Builder properties(final Map<String, Object> properties) {
            this.properties.putAll(properties);
            return this;
        }

        public Builder property(final String name, final Object value) {
            this.properties.put(name, value);
            return this;
        }

        @JsonProperty("class")
        public Builder className(final String className) {
            // ignore the className as it will be picked up by the JsonTypeInfo annotation.
            return this;
        }

        public Entity build() {
            return new Entity(this);
        }
    }
}
