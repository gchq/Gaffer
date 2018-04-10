/*
 * Copyright 2016-2018 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.id.EntityId;

/**
 * An {@code Entity} in an {@link uk.gov.gchq.gaffer.data.element.Element} containing a single vertex.
 * The vertex can be any type of {@link java.lang.Object}.
 * There is no requirement for this vertex to connect to an {@link uk.gov.gchq.gaffer.data.element.Entity}'s source or
 * destination vertex - for example you could have a 'graph' of just entities.
 * Entities are designed so that multiple entities can share the same vertex but are distinguished via their
 * group.
 *
 * @see uk.gov.gchq.gaffer.data.element.Entity.Builder
 */
@JsonPropertyOrder(value = {"class", "group", "vertex", "properties"}, alphabetic = true)
public class Entity extends Element implements EntityId {
    private static final Logger LOGGER = LoggerFactory.getLogger(Entity.class);
    private static final long serialVersionUID = 2863628004463113755L;
    private Object vertex;

    Entity() {
        super();
    }

    public Entity(final String group) {
        super(group);
    }

    /**
     * Constructs an instance of Entity.
     *
     * @param group  the Entity group
     * @param vertex the vertex
     */
    public Entity(final String group, final Object vertex) {
        this(group, vertex, null);
    }

    /**
     * Constructs an instance of Entity.
     *
     * @param group      the Entity group
     * @param vertex     the vertex
     * @param properties the entity properties
     */
    @JsonCreator
    public Entity(
            @JsonProperty("group") final String group,
            @JsonProperty("vertex") final Object vertex,
            @JsonProperty("properties") final Properties properties) {
        super(group, properties);
        this.vertex = vertex;
    }

    @Override
    public Object getVertex() {
        return vertex;
    }

    @Override
    public void setVertex(final Object vertex) {
        this.vertex = vertex;
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
    public void putIdentifier(final IdentifierType identifierType, final Object value) {
        switch (identifierType) {
            case VERTEX:
                vertex = value;
                break;
            default:
                LOGGER.error("Unknown identifier type: {} detected.", identifierType);
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
        return new ToStringBuilder(this)
                .append("vertex", vertex)
                .appendSuper(super.toString())
                .build();
    }

    public static class Builder {
        private String group = Element.DEFAULT_GROUP;
        private Object vertex;
        private Properties properties = new Properties();

        public Builder group(final String group) {
            this.group = group;
            return this;
        }

        public Builder vertex(final Object vertex) {
            this.vertex = vertex;
            return this;
        }

        public Builder property(final String name, final Object value) {
            properties.put(name, value);
            return this;
        }

        public Entity build() {
            return new Entity(group, vertex, properties);
        }
    }
}
