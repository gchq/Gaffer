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

package gaffer.data.element;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang.builder.EqualsBuilder;

/**
 * An <code>Entity</code> in an {@link gaffer.data.element.Element} containing a single vertex.
 * The vertex can be any type of {@link java.lang.Object}.
 * There is no requirement for this vertex to connect to an {@link gaffer.data.element.Edge}'s source or
 * destination vertex - for example you could have a 'graph' of just entities.
 * Entities are designed so that multiple entities can share the same vertex but are distinguished via their
 * group.
 *
 * @see gaffer.data.element.Entity.Builder
 */
public class Entity extends Element<EntityId> {
    private static final long serialVersionUID = 3564192309337144721L;

    Entity() {
        super(new EntityId());
    }

    public Entity(final String group) {
        super(group, new EntityId());
    }

    public Entity(final String group, final Object vertex) {
        super(group, new EntityId(vertex));
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "class")
    public Object getVertex() {
        return id().getVertex();
    }

    public void setVertex(final Object vertex) {
        id().setVertex(vertex);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
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
                .isEquals();
    }

    @Override
    public Entity emptyClone() {
        return new Entity(this.getGroup(), this.getVertex());
    }

    @Override
    public String toString() {
        return "Entity{"
                + id()
                + super.toString()
                + "} ";
    }

    public static class Builder {
        private final Entity entity = new Entity();

        public Builder group(final String group) {
            entity.setGroup(group);
            return this;
        }

        public Builder vertex(final Object vertex) {
            entity.setVertex(vertex);
            return this;
        }

        public Builder property(final String name, final Object value) {
            entity.putProperty(name, value);
            return this;
        }

        public Entity build() {
            return entity;
        }
    }
}
