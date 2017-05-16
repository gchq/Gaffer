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

import com.fasterxml.jackson.annotation.JsonIgnore;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.function.Predicate;

/**
 * {@link uk.gov.gchq.gaffer.data.element.comparison.ElementComparator} implementation
 * to use when making comparisons based on a single element property (e.g. a count
 * field).
 */
@SuppressFBWarnings(value = "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
        justification = "This class should not be serialised")
public class ElementPropertyComparator implements ElementComparator {

    /**
     * Reference to a mutated comparator, required since modifying the original
     * comparator leads to a non-JSON serialisable object.
     */
    @JsonIgnore
    private Comparator mutatedComparator;

    private Comparator comparator;
    private boolean reversed;
    private boolean includeNulls;
    private String propertyName;
    private String groupName;

    public ElementPropertyComparator() {
        // Empty
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(final String propertyName) {
        this.propertyName = propertyName;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(final String groupName) {
        this.groupName = groupName;
    }

    /**
     * Retrieve a {@link java.util.function.Predicate} which can be used to
     * filter {@link uk.gov.gchq.gaffer.data.element.Element}s in a {@link java.util.stream.Stream}.
     *
     * @return a correctly composed {@link java.util.function.Predicate} instance
     */
    public Predicate<Element> asPredicate() {
        final Predicate<Element> hasCorrectGroup = e -> groupName == e.getGroup();
        final Predicate<Element> propertyIsNull = e -> null == e.getProperty(propertyName);
        final Predicate<Element> nullsExcluded = e -> !isIncludeNulls();

        return propertyIsNull.and(nullsExcluded).negate().and(hasCorrectGroup);
    }

    private void mutateComparator() {
        if (null != comparator) {
            mutatedComparator = comparator;

            if (reversed) {
                mutatedComparator = mutatedComparator.reversed();
            }
            if (includeNulls) {
                mutatedComparator = Comparator.nullsLast(mutatedComparator);
            }
        }
    }

    @Override
    public Set<Pair<String, String>> getComparableGroupPropertyPairs() {
        if (null == comparator) {
            return Collections.singleton(new Pair<>(groupName, propertyName));
        }

        return Collections.emptySet();
    }

    @Override
    public int compare(final Element obj1, final Element obj2) {
        final Object val1 = obj1.getProperty(propertyName);
        final Object val2 = obj2.getProperty(propertyName);

        return (null == mutatedComparator)
                ? ((Comparable) val1).compareTo(val2)
                : mutatedComparator.compare(val1, val2);
    }

    @Override
    public Comparator getComparator() {
        return comparator;
    }

    @Override
    public void setComparator(final Comparator comparator) {
        this.comparator = comparator;
        mutateComparator();
    }

    @Override
    public boolean isReversed() {
        return reversed;
    }

    @Override
    public void setReversed(final boolean reversed) {
        this.reversed = reversed;
        mutateComparator();
    }

    @Override
    public boolean isIncludeNulls() {
        return includeNulls;
    }

    @Override
    public void setIncludeNulls(final boolean includeNulls) {
        this.includeNulls = includeNulls;
        mutateComparator();
    }

    public static class Builder extends ElementComparator.Builder<ElementPropertyComparator, Builder> {

        public Builder() {
            super(new ElementPropertyComparator());
        }

        public Builder groupName(final String groupName) {
            _getComparator().setGroupName(groupName);
            return _self();
        }

        public Builder propertyName(final String propertyName) {
            _getComparator().setPropertyName(propertyName);
            return _self();
        }
    }
}
