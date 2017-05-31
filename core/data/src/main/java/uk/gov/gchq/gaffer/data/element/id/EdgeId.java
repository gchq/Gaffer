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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.builder.EqualsBuilder;

public interface EdgeId extends ElementId {
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    Object getSource();

    void setSource(final Object source);

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    Object getDestination();

    void setDestination(final Object destination);

    DirectedType getDirectedType();

    void setDirectedType(final DirectedType directed);

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

    default void setDirected(final boolean directed) {
        if (directed) {
            setDirectedType(DirectedType.DIRECTED);
        } else {
            setDirectedType(DirectedType.UNDIRECTED);
        }
    }

    @Override
    default boolean isEqual(final ElementId that) {
        return that instanceof EdgeId && isEqual((EdgeId) that);
    }

    default boolean isEqual(final EdgeId that) {
        return null != that
                && (new EqualsBuilder()
                .append(getDirectedType(), that.getDirectedType())
                .append(getSource(), that.getSource())
                .append(getDestination(), that.getDestination())
                .isEquals()
                || new EqualsBuilder()
                .append(isUndirected(), true)
                .append(getDirectedType(), that.getDirectedType())
                .append(getSource(), that.getDestination())
                .append(getDestination(), that.getSource())
                .isEquals());
    }

    /**
     * This {@link EdgeId} is related to an
     * {@link ElementId} if either the ElementId is equal to this EdgeId or it is
     * an EntityId and it's identifier matches this EdgeId's source or destination.
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
     * This {@link EdgeId} is related to an
     * {@link EntityId} if the EntityId's identifier matches this
     * EdgeId's source or destination.
     *
     * @param that the {@link ElementId} to compare
     * @return An instance of {@link ElementId.Matches} to describe how the ids are related.
     */
    default Matches isRelated(final EntityId that) {
        boolean matchesSource = (getSource() == null) ? that.getVertex() == null : getSource().equals(that.getVertex());
        boolean matchesDestination = (getDestination() == null) ? that.getVertex() == null : getDestination().equals(that.getVertex());
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
}
