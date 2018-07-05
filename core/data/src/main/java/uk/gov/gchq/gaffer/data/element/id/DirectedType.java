/*
 * Copyright 2017-2018 Crown Copyright
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

import uk.gov.gchq.koryphe.Summary;

/**
 * A {@code DirectedType} defines whether edges should be
 * directed, undirected or either/both.
 */
@Summary("Is the Edge directed?")
public enum DirectedType {
    /**
     * Edges are either DIRECTED or UNDIRECTED. This is the default value.
     */
    EITHER,

    DIRECTED,

    UNDIRECTED;

    public static boolean isEither(final DirectedType directedType) {
        return null == directedType || EITHER == directedType;
    }

    public static boolean isDirected(final DirectedType directedType) {
        return null == directedType || directedType.isDirected();
    }

    public static boolean isUndirected(final DirectedType directedType) {
        return null == directedType || directedType.isUndirected();
    }

    public static boolean areCompatible(final DirectedType dirType1, final DirectedType dirType2) {
        if (DIRECTED == dirType1) {
            return dirType2.isDirected();
        } else if (UNDIRECTED == dirType1) {
            return dirType2.isUndirected();
        }

        return true;
    }

    public static DirectedType and(final DirectedType dirType1, final DirectedType dirType2) {
        if (null == dirType1 || EITHER == dirType1) {
            if (null == dirType2) {
                return EITHER;
            }
            return dirType2;
        }

        if (null == dirType2 || EITHER == dirType2) {
            return dirType1;
        }

        if (DIRECTED == dirType1) {
            if (DIRECTED == dirType2) {
                return DIRECTED;
            } else {
                throw new IllegalArgumentException("Cannot merge incompatible directed types DIRECTED and UNDIRECTED");
            }
        } else if (UNDIRECTED == dirType2) {
            return UNDIRECTED;
        } else {
            throw new IllegalArgumentException("Cannot merge incompatible directed types UNDIRECTED and DIRECTED");
        }
    }

    public boolean isDirected() {
        return UNDIRECTED != this;
    }

    public boolean isUndirected() {
        return DIRECTED != this;
    }
}
