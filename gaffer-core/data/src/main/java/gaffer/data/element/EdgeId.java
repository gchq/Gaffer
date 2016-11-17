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

public class EdgeId extends GraphId implements Id<EdgeId> {
    private static final long serialVersionUID = -2967503081774931299L;
    private static final Logger LOGGER = LoggerFactory.getLogger(EdgeId.class);
    private Object source;
    private Object destination;
    private boolean directed;

    public EdgeId() { }

    public EdgeId(final Object source, final Object destination, final boolean directed) {
        this.source = source;
        this.destination = destination;
        this.directed = directed;
    }

    public Object getSource() {
        return source;
    }

    public void setSource(final Object source) {
        this.source = source;
    }

    public Object getDestination() {
        return destination;
    }

    public void setDestination(final Object destination) {
        this.destination = destination;
    }

    public boolean isDirected() {
        return directed;
    }

    public void setDirected(final boolean directed) {
        this.directed = directed;
    }

    @Override
    public Object getIdentifier(final IdentifierType identifierType) {
        switch (identifierType) {
            case SOURCE:
                return getSource();
            case DESTINATION:
                return getDestination();
            case DIRECTED:
                return isDirected();
            default:
                LOGGER.error("Unknown identifier type: " + identifierType + " detected.");
                return null;
        }
    }

    @Override
    public void putIdentifier(final IdentifierType identifierType, final Object propertyToBeSet) {
        switch (identifierType) {
            case SOURCE:
                setSource(propertyToBeSet);
                break;
            case DESTINATION:
                setDestination(propertyToBeSet);
                break;
            case DIRECTED:
                setDirected((boolean) propertyToBeSet);
                break;
            default:
                LOGGER.error("Unknown identifier type: " + identifierType + " detected.");
        }
    }

    /**
     * This {@link EdgeId} is related to a
     * {@link GraphId} if either the GraphId is equal to this EdgeId or it is
     * an EntityId and it's identifier matches this EdgeId's source or destination.
     *
     * @param that the {@link GraphId} to compare
     * @return An instance of {@link GraphId.Matches} to describe how the seeds are related.
     */
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Edge it must be an Entity")
    @Override
    public Matches isRelated(final GraphId that) {
        if (that instanceof EdgeId) {
            if (equals(that)) {
                return Matches.BOTH;
            }

            return Matches.NONE;
        }

        return isRelated((EntityId) that);
    }

    /**
     * This {@link EdgeId} is related to an
     * {@link EntityId} if the EntityId's identifier matches this
     * EdgeId's source or destination.
     *
     * @param that the {@link GraphId} to compare
     * @return An instance of {@link GraphId.Matches} to describe how the seeds are related.
     */
    public Matches isRelated(final EntityId that) {
        boolean matchesSource = (source == null) ? that.getVertex() == null : source.equals(that.getVertex());
        boolean matchesDestination = (destination == null) ? that.getVertex() == null : destination.equals(that.getVertex());
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
        int hash;
        if (directed) {
            hash = new HashCodeBuilder(21, 3)
                    .append(source)
                    .append(destination)
                    .append(directed)
                    .toHashCode();
        } else {
            hash = source.hashCode();
            hash ^= destination.hashCode();
        }
        return hash;
    }

    public boolean equals(final Object obj) {
        return obj instanceof EdgeId
                && equals((EdgeId) obj);
    }

    public boolean equals(final EdgeId edge) {
        return null != edge
                && (new EqualsBuilder()
                .append(source, edge.getSource())
                .append(destination, edge.getDestination())
                .append(directed, edge.isDirected())
                .isEquals()
                || new EqualsBuilder()
                .append(source, edge.getDestination())
                .append(destination, edge.getSource())
                .append(directed, false)
                .isEquals()
        );
    }

    @Override
    public String toString() {
        return "EdgeId{"
                + "source=" + source
                + ", destination=" + destination
                + ", directed=" + directed
                + "} ";
    }

    @Override
    public EdgeId id() {
        return this;
    }
}
