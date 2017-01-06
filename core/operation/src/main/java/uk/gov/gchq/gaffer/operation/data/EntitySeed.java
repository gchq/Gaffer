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

/**
 * An <code>EntitySeed</code> contains a single vertex for an {@link uk.gov.gchq.gaffer.data.element.Entity}.
 * It is used as a mainly used as a seed for queries.
 */
public class EntitySeed extends ElementSeed {
    private Object vertex;

    public EntitySeed() {
    }

    public EntitySeed(final Object vertex) {
        this.vertex = vertex;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "class")
    public Object getVertex() {
        return vertex;
    }

    public void setVertex(final Object vertex) {
        this.vertex = vertex;
    }

    /**
     * This {@link EntitySeed} is related to an
     * {@link ElementSeed} if either the ElementSeed is equal to this EntitySeed or it is
     * an EdgeSeed and it's source or destination matches this EntitySeed's vertex.
     *
     * @param that the {@link ElementSeed} to compare
     * @return An instance of {@link ElementSeed.Matches} to describe how the seeds are related.
     */
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Edge it must be an Edge")
    @Override
    public Matches isRelated(final ElementSeed that) {
        if (that instanceof EntitySeed) {
            if (equals(that)) {
                return Matches.VERTEX;
            }
            return Matches.NONE;
        }

        return isRelated((EdgeSeed) that);
    }

    /**
     * This {@link EntitySeed} is related to an
     * {@link EdgeSeed} if either EdgeSeed's source or destination matches this
     * EntitySeed's vertex.
     *
     * @param that the {@link EdgeSeed} to compare
     * @return An instance of {@link ElementSeed.Matches} to describe how the seeds are related.
     */
    public Matches isRelated(final EdgeSeed that) {
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

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EntitySeed)) {
            return false;
        }

        final EntitySeed that = (EntitySeed) o;
        return !(vertex != null ? !vertex.equals(that.vertex) : that.vertex != null);
    }

    @Override
    public int hashCode() {
        return vertex != null ? vertex.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "EntitySeed{"
                + "vertex=" + vertex
                + '}';
    }
}
