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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityId extends ElementId implements Id<EntityId> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EntityId.class);
    private static final long serialVersionUID = 2965833460091737229L;
    private Object vertex;

    public EntityId() { }

    public EntityId(final Object vertex) {
        setVertex(vertex);
    }

    public Object getVertex() {
        return vertex;
    }

    public void setVertex(final Object vertex) {
        this.vertex = vertex;
    }

    @Override
    public Object getIdentifier(final IdentifierType identifierType) {
        switch (identifierType) {
            case VERTEX:
                return getVertex();
            default:
                LOGGER.error("Unknown identifier type: " + identifierType + " detected.");
                return null;
        }
    }

    @Override
    public void putIdentifier(final IdentifierType identifierType, final Object propertyToBeSet) {
        switch (identifierType) {
            case VERTEX:
                setVertex(propertyToBeSet);
                break;
            default:
                LOGGER.error("Unknown identifier type: " + identifierType + " detected.");
        }
    }

    /**
     * This {@link EntityId} is related to a
     * {@link ElementId} if either the ElementId is equal to this EntityId or it is
     * an EdgeId and it's source or destination matches this EntityId's vertex.
     *
     * @param that the {@link ElementId} to compare
     * @return An instance of {@link ElementId.Matches} to describe how the seeds are related.
     */
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Edge it must be an Edge")
    @Override
    public Matches isRelated(final ElementId that) {
        if (that instanceof EntityId) {
            if (equals(that)) {
                return Matches.VERTEX;
            }
            return Matches.NONE;
        }

        return isRelated((EdgeId) that);
    }

    /**
     * This {@link ElementId} is related to an
     * {@link EdgeId} if either EdgeSeed's source or destination matches this
     * EntitySeed's vertex.
     *
     * @param that the {@link EdgeId} to compare
     * @return An instance of {@link ElementId.Matches} to describe how the seeds are related.
     */
    public Matches isRelated(final EdgeId that) {
        boolean matchesSource = (vertex == null) ? that.getSource() == null : vertex.equals(that.getSource());
        boolean matchesDestination = (vertex == null) ? that.getDestination() == null : vertex.equals(that.getDestination());
        if (matchesSource) {
            if (matchesDestination) {
                return Matches.BOTH;
            }
            return Matches.SOURCE;
        }

        if (matchesDestination) {
            return Matches.DESTINATION;
        }

        return Matches.NONE;
    }

    public int hashCode() {
        return new HashCodeBuilder(23, 5)
                .append(vertex)
                .toHashCode();
    }

    public boolean equals(final Object obj) {
        return obj instanceof EntityId
                && equals((EntityId) obj);
    }

    public boolean equals(final EntityId entity) {
        return null != entity
                && new EqualsBuilder()
                .append(vertex, entity.getVertex())
                .isEquals();
    }

    @Override
    public String toString() {
        return "EntityId{"
                + "vertex=" + vertex
                + "} ";
    }

    @Override
    public EntityId getId() {
        return this;
    }
}
