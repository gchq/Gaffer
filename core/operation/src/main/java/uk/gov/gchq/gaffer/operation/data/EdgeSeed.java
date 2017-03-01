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

package uk.gov.gchq.gaffer.operation.data;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * An <code>EdgeSeed</code> contains source, destination and directed identifiers to identify an
 * {@link uk.gov.gchq.gaffer.data.element.Edge}.
 * It is used as a mainly used as a seed for queries.
 */
public class EdgeSeed extends ElementSeed {
    private Object source;
    private Object destination;
    private boolean directed;

    public EdgeSeed() {
    }

    public EdgeSeed(final Object source, final Object destination, final boolean directed) {
        this.source = source;
        this.destination = destination;
        this.directed = directed;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "class")
    public Object getSource() {
        return source;
    }

    public void setSource(final Object source) {
        this.source = source;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "class")
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

    /**
     * This {@link EdgeSeed} is related to an
     * {@link ElementSeed} if either the ElementSeed is equal to this EdgeSeed or it is
     * an EntitySeed and it's identifier matches this EdgeSeed's source or destination.
     *
     * @param that the {@link ElementSeed} to compare
     * @return An instance of {@link ElementSeed.Matches} to describe how the seeds are related.
     */
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Edge it must be an Entity")
    @Override
    public Matches isRelated(final ElementSeed that) {
        if (that instanceof EdgeSeed) {
            if (equals(that)) {
                return Matches.BOTH;
            }

            return Matches.NONE;
        }

        return isRelated((EntitySeed) that);
    }

    /**
     * This {@link EdgeSeed} is related to an
     * {@link EntitySeed} if the EntitySeed's identifier matches this
     * EdgeSeed's source or destination.
     *
     * @param that the {@link ElementSeed} to compare
     * @return An instance of {@link ElementSeed.Matches} to describe how the seeds are related.
     */
    public Matches isRelated(final EntitySeed that) {
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

    @Override
    public boolean equals(final Object obj) {
        return null != obj
                && (obj instanceof EdgeSeed)
                && equals((EdgeSeed) obj);
    }

    private boolean equals(final EdgeSeed edgeSeed) {
        return null != edgeSeed
                && (new EqualsBuilder()
                .append(source, edgeSeed.getSource())
                .append(destination, edgeSeed.getDestination())
                .append(directed, edgeSeed.isDirected())
                .isEquals()
                || new EqualsBuilder()
                .append(source, edgeSeed.getDestination())
                .append(destination, edgeSeed.getSource())
                .append(directed, false)
                .isEquals()
        );
    }

    @Override
    public int hashCode() {
        int hash;
        if (directed) {
            hash = new HashCodeBuilder(21, 3)
                    .append(source)
                    .append(destination)
                    .append(directed)
                    .toHashCode();
        } else {
            hash = new HashCodeBuilder(21, 3)
                    .append(directed)
                    .toHashCode();
            hash ^= source.hashCode();
            hash ^= destination.hashCode();
        }
        return hash;
    }

    @Override
    public String toString() {
        return "EdgeSeed{"
                + "source=" + source
                + ", destination=" + destination
                + ", directed=" + directed
                + '}';
    }
}
