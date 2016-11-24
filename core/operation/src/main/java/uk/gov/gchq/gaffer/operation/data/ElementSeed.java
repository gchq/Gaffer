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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;

/**
 * An <code>ElementSeed</code> contains the identifiers for an {@link uk.gov.gchq.gaffer.data.element.Entity} or
 * {@link uk.gov.gchq.gaffer.data.element.Edge}.
 * It is used as a mainly used as a seed for queries.
 *
 * @see EntitySeed
 * @see EdgeSeed
 */
@JsonTypeInfo(use = Id.CLASS, include = As.EXISTING_PROPERTY, property = "class")
public abstract class ElementSeed {
    /**
     * @param that the {@link ElementSeed} to compare
     * @return An instance of {@link Matches} to describe how the seeds are related.
     * @see EntitySeed#isRelated(ElementSeed)
     * @see EdgeSeed#isRelated(ElementSeed)
     */
    public abstract Matches isRelated(ElementSeed that);

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
    public static ElementSeed createSeed(final Element element) {
        if (element instanceof Entity) {
            return createSeed((Entity) element);
        }

        return createSeed((Edge) element);
    }

    public static EntitySeed createSeed(final Entity entity) {
        return new EntitySeed(entity.getVertex());
    }

    public static EdgeSeed createSeed(final Edge edge) {
        return new EdgeSeed(edge.getSource(), edge.getDestination(), edge.isDirected());
    }

    @JsonGetter("class")
    String getClassName() {
        return getClass().getName();
    }

    @JsonSetter("class")
    void setClassName(final String className) {
        // ignore the className as it will be picked up by the JsonTypeInfo annotation.
    }

    public enum Matches {
        BOTH,
        VERTEX,
        SOURCE,
        DESTINATION,
        NONE;

        public boolean isIdentifier() {
            return this == VERTEX;
        }

        public boolean isSource() {
            return this == BOTH || this == SOURCE;
        }

        public boolean isDestination() {
            return this == BOTH || this == DESTINATION;
        }

        public boolean isMatch() {
            return this != NONE;
        }
    }
}
