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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.Serializable;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
public interface ElementId extends Serializable {
    /**
     * @param that the {@link ElementId} to compare
     * @return An instance of {@link Matches} to describe how the seeds are related.
     */
    Matches isRelated(ElementId that);

    boolean isEqual(final ElementId that);

    @JsonGetter("class")
    default String getClassName() {
        return getClass().getName();
    }

    @JsonSetter("class")
    default void setClassName(final String className) {
        // ignore the className as it will be picked up by the JsonTypeInfo annotation.
    }

    enum Matches {
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
