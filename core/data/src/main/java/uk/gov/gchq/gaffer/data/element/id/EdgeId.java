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

package uk.gov.gchq.gaffer.data.element.id;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.builder.EqualsBuilder;

/**
 * An {@code EdgeId} is an interface describing the core methods that are required
 * in order to identify an {@link uk.gov.gchq.gaffer.data.element.Edge}.
 */
@JsonFilter("filterFieldsByName")
public interface EdgeId extends ElementId {

    /**
     * Enumerated type to denote which vertex of an {@link uk.gov.gchq.gaffer.data.element.Edge}
     * matches an input parameter.
     */
    enum MatchedVertex {
        SOURCE,
        DESTINATION;

        public static boolean isEqual(final MatchedVertex matchedVertex1, final MatchedVertex matchedVertex2) {
            return matchedVertex1 == matchedVertex2
                    || (null == matchedVertex1 && SOURCE == matchedVertex2)
                    || (null == matchedVertex2 && SOURCE == matchedVertex1);
        }
    }

    /**
     * Get the source vertex.
     *
     * @return the object at the edge source
     */
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "class")
    Object getSource();

    /**
     * Get the destination vertex.
     *
     * @return the object at the edge destination
     */
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "class")
    Object getDestination();

    /**
     * Get the directionality of the current edge.
     *
     * @return the directed type
     */
    DirectedType getDirectedType();

    /**
     * @return true if directed is DIRECTED, EITHER or null. Otherwise false.
     */
    @JsonIgnore
    default boolean isDirected() {
        return DirectedType.UNDIRECTED != getDirectedType();
    }

    /**
     * @return true if directed is UNDIRECTED, EITHER or null. Otherwise false.
     */
    @JsonIgnore
    default boolean isUndirected() {
        return DirectedType.DIRECTED != getDirectedType();
    }

    default void setIdentifiers(final Object source, final Object destination, final DirectedType directed) {
        setIdentifiers(source, destination, directed, getMatchedVertex());
    }

    void setIdentifiers(final Object source, final Object destination, final DirectedType directedType, final MatchedVertex matchedVertex);

    /**
     * Get the vertex which is to be reported if a matched vertex is requested.
     *
     * @return the matched vertex
     */
    MatchedVertex getMatchedVertex();

    @Override
    default boolean isEqual(final ElementId that) {
        return that instanceof EdgeId && isEqual((EdgeId) that);
    }

    /**
     * Note this does not include the matchedVertex field.
     *
     * @param that the reference EdgeId with which to compare.
     * @return {@code true} if this object is the same as the edge
     * argument; {@code false} otherwise.
     */
    default boolean isEqual(final EdgeId that) {
        return null != that
                && (new EqualsBuilder()
                .append(getDirectedType(), that.getDirectedType())
                .append(getSource(), that.getSource())
                .append(getDestination(), that.getDestination())
                .isEquals());
    }

    /**
     * This is related to an {@link ElementId} if either the ElementId is equal
     * to this EdgeId or it is an EntityId and its identifier matches this
     * EdgeId's source or destination.
     *
     * @param that the {@link ElementId} to compare
     * @return An instance of {@link ElementId.Matches} to describe how the ids are related.
     */
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Edge it must be an Entity")
    @Override
    default Matches isRelated(final ElementId that) {
        if (that instanceof EdgeId) {
            if (isEqual(that)) {
                return Matches.BOTH;
            }

            return Matches.NONE;
        }

        return isRelated((EntityId) that);
    }

    /**
     * This is related to an {@link EntityId} if the EntityId's identifier
     * matches this EdgeId's source or destination.
     *
     * @param that the {@link ElementId} to compare
     * @return An instance of {@link ElementId.Matches} to describe how the ids are related.
     */
    default Matches isRelated(final EntityId that) {
        boolean matchesSource = (null == getSource()) ? null == that.getVertex() : getSource().equals(that.getVertex());
        boolean matchesDestination = (null == getDestination()) ? null == that.getVertex() : getDestination().equals(that.getVertex());
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

    @JsonIgnore
    default Object getMatchedVertexValue() {
        return MatchedVertex.DESTINATION == getMatchedVertex() ? getDestination() : getSource();
    }

    @JsonIgnore
    default Object getAdjacentMatchedVertexValue() {
        return MatchedVertex.DESTINATION == getMatchedVertex() ? getSource() : getDestination();
    }

}
