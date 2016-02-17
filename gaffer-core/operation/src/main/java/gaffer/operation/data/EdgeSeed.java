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

package gaffer.operation.data;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * An <code>EdgeSeed</code> contains source, destination and directed identifiers to identify an
 * {@link gaffer.data.element.Edge}.
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
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof EdgeSeed)) {
            return false;
        }

        final EdgeSeed that = (EdgeSeed) o;

        return directed == that.directed
                && !(destination != null ? !destination.equals(that.destination) : that.destination != null)
                && !(source != null ? !source.equals(that.source) : that.source != null);
    }

    @Override
    public int hashCode() {
        int result = source != null ? source.hashCode() : 0;
        result = 31 * result + (destination != null ? destination.hashCode() : 0);
        result = 31 * result + (directed ? 1 : 0);
        return result;
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
