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

package uk.gov.gchq.gaffer.data.element.comparison;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

/**
 * A {@code ComparableOrToStringComparator} is a {@link Comparator} which compares
 * two objects using their native {@link Comparator#compare(Object, Object)} method
 * if it is available and falls back onto a comparison of their {@link Object#toString()}
 * values.
 */
public class ComparableOrToStringComparator implements Comparator<Object>, Serializable {
    @Override
    public int compare(final Object vertex1, final Object vertex2) {
        if (null == vertex1) {
            if (null == vertex2) {
                return 0;
            }
            return 1;
        }

        if (null == vertex2) {
            return -1;
        }

        if (vertex1 instanceof Comparable
                && vertex2.getClass().equals(vertex1.getClass())) {
            return ((Comparable) vertex1).compareTo(vertex2);
        }

        final String toString1;
        if (vertex1.getClass().isArray()) {
            toString1 = Arrays.toString((Object[]) vertex1);
        } else {
            toString1 = vertex1.toString();
        }

        final String toString2;
        if (vertex2.getClass().isArray()) {
            toString2 = Arrays.toString((Object[]) vertex2);
        } else {
            toString2 = vertex2.toString();
        }

        return toString1.compareTo(toString2);
    }
}
