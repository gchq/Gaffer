/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.store.element;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * An {@code ElementKey} wraps an element and overrides hashcode and equals
 * to only select the parts of the element that make up the key - i.e the Group,
 * vertex/source/destination/directed and the group by properties.
 * <p>
 * This can then be used in a {@link Map} to group elements together prior to
 * performing aggregation.
 * </p>
 * <p>
 * An ElementKey should be constructed using one of the create methods.
 * </p>
 */
public interface ElementKey {
    static ElementKey create(final Element element, final Schema schema) {
        if (null == schema) {
            throw new IllegalArgumentException("Schema is required");
        }
        return create(element, null != element ? schema.getElement(element.getGroup()) : null);
    }

    static ElementKey create(final Element element, final SchemaElementDefinition elementDef) {
        return create(element, null != elementDef ? elementDef.getGroupBy() : null);
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
    static ElementKey create(final Element element, final Set<String> groupBy) {
        if (null == element) {
            throw new IllegalArgumentException("Element is required");
        }

        final Map<String, Object> groupByProperties;
        if (null == groupBy || groupBy.isEmpty()) {
            groupByProperties = Collections.emptyMap();
        } else {
            groupByProperties = new HashMap<>(groupBy.size());
            for (final String prop : groupBy) {
                groupByProperties.put(prop, element.getProperty(prop));
            }
        }

        if (element instanceof Entity) {
            return new EntityKey((Entity) element, groupByProperties);
        }
        return new EdgeKey((Edge) element, groupByProperties);
    }

    class EntityKey implements ElementKey {
        private final Entity entity;
        private final Map<String, Object> groupByProperties;

        private EntityKey(final Entity entity, final Map<String, Object> groupByProperties) {
            if (null == entity) {
                throw new IllegalArgumentException("Entity required");
            }
            this.entity = entity;
            this.groupByProperties = groupByProperties;
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(23, 5)
                    .append(entity.getGroup())
                    .append(entity.getVertex())
                    .append(groupByProperties)
                    .toHashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }

            if (null == obj || getClass() != obj.getClass()) {
                return false;
            }

            final EntityKey entityKey = (EntityKey) obj;

            return new EqualsBuilder()
                    .append(entity.getGroup(), entityKey.entity.getGroup())
                    .append(entity.getVertex(), entityKey.entity.getVertex())
                    .append(groupByProperties, entityKey.groupByProperties)
                    .isEquals();
        }
    }

    class EdgeKey implements ElementKey {
        private final Edge edge;
        private final Map<String, Object> groupByProperties;

        private EdgeKey(final Edge edge, final Map<String, Object> groupByProperties) {
            if (null == edge) {
                throw new IllegalArgumentException("Edge required");
            }
            this.edge = edge;
            this.groupByProperties = groupByProperties;
        }

        @Override
        public int hashCode() {
            int hash;
            if (edge.isDirected()) {
                hash = new HashCodeBuilder(21, 3)
                        .append(edge.getGroup())
                        .append(edge.getSource())
                        .append(edge.getDestination())
                        .append(edge.isDirected())
                        .append(groupByProperties)
                        .toHashCode();
            } else {
                hash = new HashCodeBuilder(21, 3)
                        .append(edge.getGroup())
                        .append(edge.isDirected())
                        .append(groupByProperties)
                        .toHashCode();
                if (null != edge.getSource()) {
                    hash ^= edge.getSource().hashCode();
                }

                if (null != edge.getDestination()) {
                    hash ^= edge.getDestination().hashCode();
                }
            }
            return hash;
        }

        @Override
        public boolean equals(final Object obj) {
            return null != obj
                    && (obj instanceof EdgeKey)
                    && equals((EdgeKey) obj);
        }

        public boolean equals(final EdgeKey edgeKey) {
            return null != edgeKey
                    && (new EqualsBuilder()
                    .append(edge.isDirected(), edgeKey.edge.isDirected())
                    .append(edge.getGroup(), edgeKey.edge.getGroup())
                    .append(edge.getSource(), edgeKey.edge.getSource())
                    .append(edge.getDestination(), edgeKey.edge.getDestination())
                    .append(groupByProperties, edgeKey.groupByProperties)
                    .isEquals()
                    || new EqualsBuilder()
                    .append(edge.isDirected(), false)
                    .append(edge.getDirectedType(), edgeKey.edge.getDirectedType())
                    .append(edge.getGroup(), edgeKey.edge.getGroup())
                    .append(edge.getSource(), edgeKey.edge.getDestination())
                    .append(edge.getDestination(), edgeKey.edge.getSource())
                    .append(groupByProperties, edgeKey.groupByProperties)
                    .isEquals());
        }
    }
}

