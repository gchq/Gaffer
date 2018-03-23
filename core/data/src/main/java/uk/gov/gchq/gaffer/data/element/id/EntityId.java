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

package uk.gov.gchq.gaffer.data.element.id;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Objects;

/**
 * An {@code EntityId} is an interface describing the core methods that are required
 * in order to identify an {@link uk.gov.gchq.gaffer.data.element.Entity}.
 */
public interface EntityId extends ElementId {

    /**
     * Get the vertex object.
     *
     * @return the vertex
     */
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "class")
    Object getVertex();

    /**
     * Set the vertex object.
     *
     * @param vertex the vertex object to set
     */
    void setVertex(final Object vertex);

    @Override
    default boolean isEqual(final ElementId that) {
        return that instanceof EntityId && isEqual((EntityId) that);
    }

    default boolean isEqual(final EntityId that) {
        return Objects.equals(getVertex(), that.getVertex());
    }

    /**
     * This is related to an {@link ElementId} if either the ElementId is equal
     * to this EntityId or it is an EdgeId and its source or destination matches
     * this EntityId's vertex.
     *
     * @param that the {@link ElementId} to compare
     * @return An instance of {@link ElementId.Matches} to describe how the seeds are related.
     */
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Edge it must be an Edge")
    @Override
    default Matches isRelated(final ElementId that) {
        if (that instanceof EntityId) {
            if (isEqual(that)) {
                return Matches.VERTEX;
            }
            return Matches.NONE;
        }

        return isRelated((EdgeId) that);
    }

    /**
     * This  is related to an {@link EdgeId} if either the EdgeId's source or
     * destination matches this EntityId's vertex.
     *
     * @param that the {@link EdgeId} to compare
     * @return An instance of {@link ElementId.Matches} to describe how the seeds are related.
     */
    default Matches isRelated(final EdgeId that) {
        boolean matchesSource = (null == getVertex()) ? null == that.getSource() : getVertex().equals(that.getSource());
        boolean matchesDestination = (null == getVertex()) ? null == that.getDestination() : getVertex().equals(that.getDestination());
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
