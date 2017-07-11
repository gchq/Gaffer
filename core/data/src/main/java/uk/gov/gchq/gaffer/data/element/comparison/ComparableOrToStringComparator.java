/*
 * Copyright 2017 Crown Copyright
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

import java.util.Comparator;

public class ComparableOrToStringComparator implements Comparator<Object> {
    @Override
    public int compare(final Object vertex1, final Object vertex2) {
        if (null == vertex1) {
            if (null == vertex2) {
                return 0;
            }
            return -1;
        }

        if (vertex1 instanceof Comparable
                && vertex2.getClass().equals(vertex1.getClass())) {
            return ((Comparable) vertex1).compareTo(vertex2);
        }

        return vertex1.toString().compareTo(vertex2.toString());
    }
}
