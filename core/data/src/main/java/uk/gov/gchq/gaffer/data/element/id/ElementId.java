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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import uk.gov.gchq.koryphe.serialisation.json.JsonSimpleClassName;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameIdResolver;

import java.io.Serializable;

/**
 * An {@code ElementId} is an interface describing the core methods that are required
 * in order to identify an {@link uk.gov.gchq.gaffer.data.element.Element}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
@JsonSimpleClassName(includeSubtypes = true)
public interface ElementId extends Serializable {
    Matches isRelated(final ElementId that);

    boolean isEqual(final ElementId that);

    @JsonGetter("class")
    default String getClassName() {
        return SimpleClassNameIdResolver.getSimpleClassName(getClass());
    }

    @JsonSetter("class")
    default void setClassName(final String className) {
        // ignore the className as it will be picked up by the JsonTypeInfo annotation.
    }

    /**
     * Enumerated type to denote which component of an {@link uk.gov.gchq.gaffer.data.element.Element}
     * matches an input parameter.
     */
    enum Matches {
        /**
         * Matches both the source and destination of an {@link uk.gov.gchq.gaffer.data.element.Edge}.
         */
        BOTH,
        /**
         * Matches the vertex of an {@link uk.gov.gchq.gaffer.data.element.Entity}
         */
        VERTEX,
        /**
         * Matches the source of an {@link uk.gov.gchq.gaffer.data.element.Edge}
         */
        SOURCE,
        /**
         * Matches the destination of an {@link uk.gov.gchq.gaffer.data.element.Edge}
         */
        DESTINATION,
        /**
         * Matches nothing.
         */
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
