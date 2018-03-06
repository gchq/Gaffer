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

package uk.gov.gchq.gaffer.operation.impl.compare;

import com.fasterxml.jackson.annotation.JsonIgnore;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.comparison.ElementComparator;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * An {@code ElementComparison} operation is an operation which is used
 * to make comparisons between elements. It is required to have an array of
 * {@link Comparator}s of {@link Element}s
 */
public interface ElementComparison {

    /**
     * Get the list of {@link Element} comparators registered to the operation.
     *
     * @return an ordered {@link List} containing the comparators
     */
    List<Comparator<Element>> getComparators();

    /**
     * Combine all currently registered comparators into a single {@link Comparator}
     * object.
     *
     * @return the combined comparator
     */
    @JsonIgnore
    default Comparator<Element> getCombinedComparator() {
        final List<Comparator<Element>> comparators = getComparators();
        Comparator<Element> combinedComparator = null;
        if (null != comparators && !comparators.isEmpty()) {
            for (final Comparator<Element> comparator : comparators) {
                if (null == combinedComparator) {
                    combinedComparator = comparator;
                } else if (null != comparator) {
                    combinedComparator = combinedComparator.thenComparing(comparator);
                }
            }
        }

        return combinedComparator;
    }

    /**
     * Get all of the Group-Property pairs which implement the {@link Comparable}
     * interface.
     *
     * @return a {@link Set} containing the pairs
     */
    @JsonIgnore
    default Set<Pair<String, String>> getComparableGroupPropertyPairs() {
        final List<Comparator<Element>> comparators = getComparators();
        if (null != comparators && !comparators.isEmpty()) {
            final Set<Pair<String, String>> pairs = new HashSet<>();
            for (final Comparator<Element> comparator : comparators) {
                if (null != comparator && comparator instanceof ElementComparator) {
                    pairs.addAll(((ElementComparator) comparator).getComparableGroupPropertyPairs());
                }
            }
            return pairs;
        }

        return Collections.emptySet();
    }
}
