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

import com.fasterxml.jackson.annotation.JsonIgnore;

public abstract class GraphId {
    @JsonIgnore
    public abstract Object getIdentifier(final IdentifierType identifierType);

    public abstract void putIdentifier(final IdentifierType identifierType, final Object propertyToBeSet);

    /**
     * @param that the {@link GraphId} to compare
     * @return An instance of {@link Matches} to describe how the seeds are related.
     * @see EntityId#isRelated(GraphId)
     * @see EdgeId#isRelated(GraphId)
     */
    public abstract Matches isRelated(GraphId that);

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
