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

import com.fasterxml.jackson.annotation.JsonIgnore;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;

import java.util.Collections;
import java.util.Comparator;
import java.util.Set;

/**
 * Base interface describing {@link uk.gov.gchq.gaffer.data.element.Element}
 * {@link java.util.Comparator} instances.
 * <p>
 *     Implementations should be JSON serialiseable
 * </p>
 */
public interface ElementComparator extends Comparator<Element> {
    /**
     * This should return a set all properties that the comparator
     * requires to be of type Comparable. As properties are associated with a Group
     * we need to return Group,Property pairs. This is used to validate the the
     * ElementComparator against the schema.
     *
     * @return set of group,property pairs, in which all properties are required to be comparable.
     */
    @JsonIgnore
    default Set<Pair<String, String>> getComparableGroupPropertyPairs() {
        return Collections.emptySet();
    }
}
